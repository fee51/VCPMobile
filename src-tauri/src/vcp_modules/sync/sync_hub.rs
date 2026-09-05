use crate::vcp_modules::chat_manager::ChatMessage;
use crate::vcp_modules::db_write_queue::{DbWriteQueue, DbWriteTask};
use crate::vcp_modules::sync::sync_dto::{
    AgentSyncDTO, AgentTopicSyncDTO, GroupSyncDTO, GroupTopicSyncDTO,
};
use crate::vcp_modules::sync::sync_executor::pull_executor::{
    apply_pulled_topic_messages, canonicalize_message_attachments, BoundedWarnings,
};
use crate::vcp_modules::topic_types::TopicKey;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::{HashMap, HashSet};

const HUB_SNAPSHOT_PATH: &str = "/api/sync-hub/snapshot";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HubProbe {
    Hub,
    NotHub,
    Unauthorized,
    Unreachable(String),
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HubApplySummary {
    pub cursor: u64,
    pub agents: usize,
    pub groups: usize,
    pub topics: usize,
    pub messages: usize,
    pub legacy_attachment_warnings: usize,
    pub warning_samples: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HubSnapshot {
    #[serde(default)]
    pub latest_cursor: u64,
    #[serde(default)]
    pub entities: Vec<HubEntity>,
    #[serde(default)]
    pub messages: Vec<HubSnapshotMessage>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HubEntity {
    #[serde(default)]
    pub entity_type: String,
    #[serde(default)]
    pub entity_id: String,
    pub owner_type: Option<String>,
    pub owner_id: Option<String>,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HubSnapshotMessage {
    #[serde(default)]
    pub topic_id: String,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Default)]
pub struct GroupedHubSnapshot {
    pub agents: HashMap<String, Value>,
    pub groups: HashMap<String, Value>,
    pub topics_by_owner: HashMap<(String, String), Vec<Value>>,
    pub messages_by_topic: HashMap<(String, String, String), Vec<Value>>,
}

fn snapshot_url(http_url: &str) -> String {
    format!("{}{}", http_url.trim_end_matches('/'), HUB_SNAPSHOT_PATH)
}

fn json_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_string)
}

fn json_i64(value: &Value, key: &str, default: i64) -> i64 {
    value
        .get(key)
        .and_then(Value::as_i64)
        .or_else(|| value.get(key).and_then(Value::as_f64).map(|n| n as i64))
        .unwrap_or(default)
}

fn json_f64(value: &Value, key: &str, default: f64) -> f64 {
    value
        .get(key)
        .and_then(Value::as_f64)
        .or_else(|| value.get(key).and_then(Value::as_i64).map(|n| n as f64))
        .unwrap_or(default)
}

fn json_bool(value: &Value, key: &str, default: bool) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(default)
}

fn short_topic_id(raw: &str) -> String {
    raw.rsplit('/').next().unwrap_or(raw).to_string()
}

pub fn group_snapshot(snapshot: &HubSnapshot) -> GroupedHubSnapshot {
    let mut grouped = GroupedHubSnapshot::default();
    let mut topic_owners: HashMap<String, HashSet<(String, String)>> = HashMap::new();

    for entity in &snapshot.entities {
        match entity.entity_type.as_str() {
            "agent" => {
                grouped
                    .agents
                    .insert(entity.entity_id.clone(), entity.payload.clone());
            }
            "group" => {
                grouped
                    .groups
                    .insert(entity.entity_id.clone(), entity.payload.clone());
            }
            "topic" | "agent_topic" | "group_topic" => {
                let topic_id = short_topic_id(&entity.entity_id);
                if topic_id.is_empty() {
                    continue;
                }
                let owner_id = entity
                    .owner_id
                    .clone()
                    .or_else(|| json_string(&entity.payload, "ownerId"))
                    .unwrap_or_default();
                if owner_id.is_empty() {
                    continue;
                }
                let owner_type = entity
                    .owner_type
                    .clone()
                    .or_else(|| {
                        if entity.entity_type == "group_topic" {
                            Some("group".to_string())
                        } else if entity.entity_type == "agent_topic" {
                            Some("agent".to_string())
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| "agent".to_string());
                let owner_type = if owner_type == "group" {
                    "group"
                } else {
                    "agent"
                };
                topic_owners
                    .entry(topic_id.clone())
                    .or_default()
                    .insert((owner_type.to_string(), owner_id.clone()));
                grouped
                    .topics_by_owner
                    .entry((owner_type.to_string(), owner_id))
                    .or_default()
                    .push(json!({
                        "id": topic_id,
                        "name": json_string(&entity.payload, "name").unwrap_or_else(|| "未命名话题".to_string()),
                        "createdAt": json_i64(&entity.payload, "createdAt", 0),
                        "locked": json_bool(&entity.payload, "locked", true),
                        "unread": json_bool(&entity.payload, "unread", false),
                    }));
            }
            _ => {}
        }
    }

    for message in &snapshot.messages {
        if message.topic_id.is_empty() {
            continue;
        }
        let (owner_type, owner_id, topic_id) =
            if let Some((owner, topic)) = message.topic_id.split_once('/') {
                let owner_type = if grouped.groups.contains_key(owner) {
                    "group"
                } else {
                    "agent"
                };
                (owner_type.to_string(), owner.to_string(), topic.to_string())
            } else {
                let owners = topic_owners.get(&message.topic_id);
                let Some(owners) = owners else { continue };
                if owners.len() != 1 {
                    continue;
                }
                let (owner_type, owner_id) = owners.iter().next().expect("single owner").clone();
                (owner_type, owner_id, message.topic_id.clone())
            };
        if topic_id.is_empty() || owner_id.is_empty() {
            continue;
        }
        grouped
            .messages_by_topic
            .entry((owner_type, owner_id, topic_id))
            .or_default()
            .push(message.payload.clone());
    }

    grouped
}

pub fn agent_dto_from_payload(payload: &Value) -> AgentSyncDTO {
    AgentSyncDTO {
        name: json_string(payload, "name").unwrap_or_else(|| "Unnamed Agent".to_string()),
        system_prompt: json_string(payload, "systemPrompt").unwrap_or_default(),
        model: json_string(payload, "model").unwrap_or_default(),
        temperature: json_f64(payload, "temperature", 1.0),
        context_token_limit: json_i64(payload, "contextTokenLimit", 1_000_000) as i32,
        max_output_tokens: json_i64(payload, "maxOutputTokens", 64_000) as i32,
        stream_output: json_bool(payload, "streamOutput", true),
    }
}

pub fn group_dto_from_payload(payload: &Value) -> GroupSyncDTO {
    let members = payload
        .get("members")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    GroupSyncDTO {
        name: json_string(payload, "name").unwrap_or_else(|| "Unnamed Group".to_string()),
        members,
        mode: json_string(payload, "mode").unwrap_or_else(|| "sequential".to_string()),
        member_tags: None,
        group_prompt: json_string(payload, "groupPrompt"),
        invite_prompt: json_string(payload, "invitePrompt"),
        use_unified_model: json_bool(payload, "useUnifiedModel", false),
        unified_model: json_string(payload, "unifiedModel"),
        tag_match_mode: json_string(payload, "tagMatchMode"),
        created_at: json_i64(payload, "createdAt", 0),
    }
}

pub(crate) fn chat_message_from_payload(
    mut payload: Value,
    topic_id: &str,
    owner_type: &str,
    owner_id: &str,
    warnings: &mut BoundedWarnings,
) -> Result<ChatMessage, String> {
    if !payload.is_object() {
        return Err(format!(
            "SyncHub topic {topic_id} contains a non-object message"
        ));
    }
    if payload
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .is_empty()
    {
        if let Some(message_id) = json_string(&payload, "messageId") {
            payload["id"] = json!(message_id);
        }
    }
    if let Some(content) = payload.get("content").cloned() {
        if !content.is_string() {
            payload["content"] = json!(content.to_string());
        }
    }
    payload["topicId"] = json!(topic_id);
    if owner_type == "group" {
        payload["groupId"] = json!(owner_id);
        payload["isGroupMessage"] = json!(true);
    } else {
        payload["agentId"] = json!(owner_id);
    }
    let message_id = json_string(&payload, "id")
        .filter(|id| !id.is_empty())
        .ok_or_else(|| format!("SyncHub topic {topic_id} contains a message without id"))?;
    let object = payload
        .as_object_mut()
        .ok_or_else(|| format!("SyncHub message {message_id} must be an object"))?;
    canonicalize_message_attachments(object, &message_id, warnings)?;
    // Hash only the normalized message, not the desktop's legacy payload.
    object.remove("contentHash");
    object.remove("content_hash");
    let mut message: ChatMessage = serde_json::from_value(payload)
        .map_err(|error| format!("SyncHub message {message_id} is invalid: {error}"))?;
    if message.updated_at.is_none() {
        message.updated_at = Some(message.timestamp);
    }
    Ok(message)
}

async fn authorized_get(
    client: &reqwest::Client,
    url: &str,
    sync_token: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    client
        .get(url)
        .header("Authorization", format!("Bearer {sync_token}"))
        .header("x-sync-token", sync_token)
        .send()
        .await
}

pub async fn probe_sync_hub(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
) -> HubProbe {
    match fetch_snapshot_inner(client, http_url, sync_token).await {
        Ok(_) => HubProbe::Hub,
        Err(HubFetchError::Unauthorized) => HubProbe::Unauthorized,
        Err(HubFetchError::NotHub) => HubProbe::NotHub,
        Err(HubFetchError::Transport(message) | HubFetchError::Invalid(message)) => {
            HubProbe::Unreachable(message)
        }
    }
}

#[derive(Debug)]
enum HubFetchError {
    Unauthorized,
    NotHub,
    Transport(String),
    Invalid(String),
}

pub async fn fetch_snapshot(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
) -> Result<HubSnapshot, String> {
    fetch_snapshot_inner(client, http_url, sync_token)
        .await
        .map_err(|error| match error {
            HubFetchError::Unauthorized => "SyncHub unauthorized".to_string(),
            HubFetchError::NotHub => "server is not SyncHub".to_string(),
            HubFetchError::Transport(message) | HubFetchError::Invalid(message) => message,
        })
}

async fn fetch_snapshot_inner(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
) -> Result<HubSnapshot, HubFetchError> {
    let url = snapshot_url(http_url);
    let response = authorized_get(client, &url, sync_token)
        .await
        .map_err(|error| HubFetchError::Transport(error.to_string()))?;
    let status = response.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(HubFetchError::Unauthorized);
    }
    if status.as_u16() == 404 {
        return Err(HubFetchError::NotHub);
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(HubFetchError::Transport(format!(
            "GET {HUB_SNAPSHOT_PATH} failed: {status} {body}"
        )));
    }
    let snapshot = response
        .json::<HubSnapshot>()
        .await
        .map_err(|error| HubFetchError::Invalid(format!("invalid SyncHub snapshot: {error}")))?;
    Ok(snapshot)
}

pub async fn apply_snapshot(
    snapshot: &HubSnapshot,
    write_queue: &DbWriteQueue,
    prerender_enabled: bool,
    pool: &sqlx::SqlitePool,
) -> Result<HubApplySummary, String> {
    let grouped = group_snapshot(snapshot);
    let mut summary = HubApplySummary {
        cursor: snapshot.latest_cursor,
        agents: grouped.agents.len(),
        groups: grouped.groups.len(),
        topics: 0,
        messages: 0,
        legacy_attachment_warnings: 0,
        warning_samples: Vec::new(),
    };
    let mut warnings = BoundedWarnings::default();

    for (agent_id, payload) in &grouped.agents {
        write_queue
            .submit(DbWriteTask::Agent {
                id: agent_id.clone(),
                dto: agent_dto_from_payload(payload),
            })
            .await?;
    }
    for (group_id, payload) in &grouped.groups {
        write_queue
            .submit(DbWriteTask::Group {
                id: group_id.clone(),
                dto: group_dto_from_payload(payload),
            })
            .await?;
    }

    let mut agent_topics = Vec::new();
    let mut group_topics = Vec::new();
    let mut known_topics = HashSet::new();
    for ((owner_type, owner_id), topics) in &grouped.topics_by_owner {
        for topic in topics {
            let topic_id = json_string(topic, "id").unwrap_or_default();
            if topic_id.is_empty() {
                continue;
            }
            let key = TopicKey::new(owner_type, owner_id, &topic_id);
            known_topics.insert((owner_type.clone(), owner_id.clone(), topic_id.clone()));
            if owner_type == "group" {
                group_topics.push((
                    key,
                    GroupTopicSyncDTO {
                        id: topic_id,
                        name: json_string(topic, "name")
                            .unwrap_or_else(|| "未命名话题".to_string()),
                        created_at: json_i64(topic, "createdAt", 0),
                        owner_id: owner_id.clone(),
                    },
                ));
            } else {
                agent_topics.push((
                    key,
                    AgentTopicSyncDTO {
                        id: topic_id,
                        name: json_string(topic, "name")
                            .unwrap_or_else(|| "未命名话题".to_string()),
                        created_at: json_i64(topic, "createdAt", 0),
                        locked: json_bool(topic, "locked", true),
                        unread: json_bool(topic, "unread", false),
                        owner_id: owner_id.clone(),
                    },
                ));
            }
        }
    }

    for (owner_type, owner_id, topic_id) in grouped.messages_by_topic.keys() {
        if known_topics.contains(&(owner_type.clone(), owner_id.clone(), topic_id.clone())) {
            continue;
        }
        let key = TopicKey::new(owner_type, owner_id, topic_id);
        if owner_type == "group" {
            group_topics.push((
                key,
                GroupTopicSyncDTO {
                    id: topic_id.clone(),
                    name: "未命名话题".to_string(),
                    created_at: 0,
                    owner_id: owner_id.clone(),
                },
            ));
        } else {
            agent_topics.push((
                key,
                AgentTopicSyncDTO {
                    id: topic_id.clone(),
                    name: "未命名话题".to_string(),
                    created_at: 0,
                    locked: true,
                    unread: false,
                    owner_id: owner_id.clone(),
                },
            ));
        }
    }

    let modified_topics = agent_topics
        .iter()
        .map(|(key, _)| key.clone())
        .chain(group_topics.iter().map(|(key, _)| key.clone()))
        .collect::<HashSet<_>>();
    summary.topics = modified_topics.len();
    if !agent_topics.is_empty() {
        write_queue
            .submit(DbWriteTask::AgentTopicBatch {
                topics: agent_topics,
            })
            .await?;
    }
    if !group_topics.is_empty() {
        write_queue
            .submit(DbWriteTask::GroupTopicBatch {
                topics: group_topics,
            })
            .await?;
    }
    write_queue.flush().await?;

    for ((owner_type, owner_id, topic_id), payloads) in grouped.messages_by_topic {
        let key = TopicKey::new(&owner_type, &owner_id, &topic_id);
        let messages = payloads
            .into_iter()
            .map(|payload| {
                chat_message_from_payload(payload, &topic_id, &owner_type, &owner_id, &mut warnings)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if messages.is_empty() {
            continue;
        }
        summary.messages +=
            apply_pulled_topic_messages(&key, messages, write_queue, prerender_enabled).await?;
    }
    write_queue.flush().await?;
    // Snapshot imports bypass the ordinary Wire phase controller. Explicitly
    // finalize counts/hashes before notifying the UI, including empty topics.
    crate::vcp_modules::sync::sync_finalize::finalize_modified_topics(pool, &modified_topics)
        .await?;
    summary.legacy_attachment_warnings = warnings.count;
    summary.warning_samples = warnings.samples;
    Ok(summary)
}

pub async fn pull_and_apply(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
    write_queue: &DbWriteQueue,
    prerender_enabled: bool,
    pool: &sqlx::SqlitePool,
) -> Result<HubApplySummary, String> {
    let snapshot = fetch_snapshot(client, http_url, sync_token).await?;
    apply_snapshot(&snapshot, write_queue, prerender_enabled, pool).await
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HubPushSummary {
    pub entities: usize,
    pub topics: usize,
    pub messages: usize,
    pub deleted_messages: usize,
}

pub fn hub_topic_key(owner_id: &str, topic_id: &str) -> String {
    if topic_id == "default" {
        format!("{owner_id}/{topic_id}")
    } else {
        topic_id.to_string()
    }
}

fn should_upload_message(role: &str, content: &str) -> bool {
    role != "assistant" || !content.trim().is_empty()
}

async fn authorized_post_json(
    client: &reqwest::Client,
    url: &str,
    sync_token: &str,
    body: &Value,
) -> Result<reqwest::Response, String> {
    client
        .post(url)
        .header("Authorization", format!("Bearer {sync_token}"))
        .header("x-sync-token", sync_token)
        .json(body)
        .send()
        .await
        .map_err(|error| error.to_string())
}

async fn authorized_post_ndjson(
    client: &reqwest::Client,
    url: &str,
    sync_token: &str,
    body: Vec<u8>,
) -> Result<reqwest::Response, String> {
    client
        .post(url)
        .header("Authorization", format!("Bearer {sync_token}"))
        .header("x-sync-token", sync_token)
        .header("content-type", "application/x-ndjson")
        .body(body)
        .send()
        .await
        .map_err(|error| error.to_string())
}

async fn upload_entity(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
    entity_type: &str,
    id: &str,
    data: Value,
) -> Result<(), String> {
    let url = format!(
        "{}/api/mobile-sync/upload-entity",
        http_url.trim_end_matches('/')
    );
    let response = authorized_post_json(
        client,
        &url,
        sync_token,
        &json!({ "id": id, "type": entity_type, "data": data }),
    )
    .await?;
    if !response.status().is_success() {
        return Err(format!(
            "upload-entity {entity_type}/{id} failed: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ));
    }
    Ok(())
}

async fn upload_message_batch(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
    topic_id: &str,
    messages: &[Value],
) -> Result<(), String> {
    if messages.is_empty() {
        return Ok(());
    }
    let url = format!(
        "{}/api/mobile-sync/upload-messages-batch",
        http_url.trim_end_matches('/')
    );
    let mut body = Vec::new();
    let mut batch = Vec::new();
    let mut batch_bytes = 64usize;
    for message in messages {
        let encoded = serde_json::to_vec(message).map_err(|error| error.to_string())?;
        if !batch.is_empty() && batch_bytes + encoded.len() > 750 * 1024 {
            body.extend(
                serde_json::to_vec(&json!({ "topicId": topic_id, "messages": batch }))
                    .map_err(|error| error.to_string())?,
            );
            body.push(b'\n');
            batch.clear();
            batch_bytes = 64;
        }
        batch_bytes += encoded.len();
        batch.push(message.clone());
    }
    if !batch.is_empty() {
        body.extend(
            serde_json::to_vec(&json!({ "topicId": topic_id, "messages": batch }))
                .map_err(|error| error.to_string())?,
        );
        body.push(b'\n');
    }
    let response = authorized_post_ndjson(client, &url, sync_token, body).await?;
    if !response.status().is_success() {
        return Err(format!(
            "upload-messages-batch {topic_id} failed: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ));
    }
    Ok(())
}

async fn delete_remote_message(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
    message_id: &str,
) -> Result<(), String> {
    let url = format!(
        "{}/api/mobile-sync/delete-message",
        http_url.trim_end_matches('/')
    );
    let response = authorized_post_json(
        client,
        &url,
        sync_token,
        &json!({ "msgId": message_id, "deletedAt": chrono::Utc::now().timestamp_millis() }),
    )
    .await?;
    if !response.status().is_success() {
        return Err(format!(
            "delete-message {message_id} failed: {} {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ));
    }
    Ok(())
}

// The phone stores a normalized projection, not all desktop metadata. Overlay
// local edits onto the original payload so a later push cannot erase legacy
// attachments that were intentionally omitted from the mobile CAS index.
fn merge_remote_message_payload(remote: Option<&Value>, local: &Value) -> Value {
    let mut merged = remote
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(local) = local.as_object() {
        merged.extend(local.clone());
    }
    Value::Object(merged)
}

pub async fn push_local_changes(
    client: &reqwest::Client,
    http_url: &str,
    sync_token: &str,
    pool: &sqlx::SqlitePool,
    since_timestamp: i64,
) -> Result<HubPushSummary, String> {
    let mut summary = HubPushSummary::default();
    let topic_rows = sqlx::query(
        "SELECT owner_type, owner_id, topic_id, title, created_at, locked, unread
         FROM topics
         WHERE deleted_at IS NULL",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| format!("load local topics failed: {error}"))?;

    let mut topic_meta: HashMap<(String, String, String), (String, i64, bool, bool)> =
        HashMap::new();
    for row in topic_rows {
        let owner_type: String = row.get("owner_type");
        let owner_id: String = row.get("owner_id");
        let topic_id: String = row.get("topic_id");
        topic_meta.insert(
            (owner_type, owner_id, topic_id),
            (
                row.get::<String, _>("title"),
                row.get::<i64, _>("created_at"),
                row.get::<i64, _>("locked") != 0,
                row.get::<i64, _>("unread") != 0,
            ),
        );
    }

    let message_rows = sqlx::query(
        "SELECT owner_type, owner_id, topic_id, msg_id, role, name, agent_id, content, timestamp,
                is_group_message, group_id, finish_reason, updated_at
         FROM messages
         WHERE deleted_at IS NULL AND timestamp >= ?
         ORDER BY owner_type, owner_id, topic_id, timestamp, msg_id",
    )
    .bind(since_timestamp)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("load local messages failed: {error}"))?;

    let mut messages_by_topic: HashMap<(String, String, String), Vec<Value>> = HashMap::new();
    let mut owners: HashSet<(String, String)> = HashSet::new();
    for row in message_rows {
        let role: String = row.get("role");
        let content: String = row.get("content");
        if !should_upload_message(&role, &content) {
            continue;
        }
        let owner_type: String = row.get("owner_type");
        let owner_id: String = row.get("owner_id");
        let topic_id: String = row.get("topic_id");
        let msg_id: String = row.get("msg_id");
        let timestamp: i64 = row.get("timestamp");
        let updated_at: i64 = row.get("updated_at");
        owners.insert((owner_type.clone(), owner_id.clone()));
        messages_by_topic
            .entry((owner_type.clone(), owner_id.clone(), topic_id.clone()))
            .or_default()
            .push(json!({
                "id": msg_id,
                "role": role,
                "name": row.get::<Option<String>, _>("name"),
                "content": content,
                "timestamp": timestamp,
                "updatedAt": if updated_at > 0 { updated_at } else { timestamp },
                "agentId": row.get::<Option<String>, _>("agent_id"),
                "groupId": row.get::<Option<String>, _>("group_id"),
                "topicId": topic_id,
                "isGroupMessage": row.get::<i64, _>("is_group_message") != 0,
                "finishReason": row.get::<Option<String>, _>("finish_reason"),
            }));
    }

    // Do this before any upload. If the remote original cannot be read, fail
    // closed rather than replace it with a lossy mobile-only representation.
    let mut remote_payloads = HashMap::new();
    if !messages_by_topic.is_empty() {
        for message in fetch_snapshot(client, http_url, sync_token).await?.messages {
            let id = message
                .message_id
                .or_else(|| json_string(&message.payload, "id"))
                .or_else(|| json_string(&message.payload, "messageId"));
            if let Some(id) = id {
                remote_payloads.insert((message.topic_id, id), message.payload);
            }
        }
    }
    for ((_owner_type, owner_id, topic_id), messages) in &mut messages_by_topic {
        let wire_topic = hub_topic_key(owner_id, topic_id);
        let full_topic = format!("{owner_id}/{topic_id}");
        for message in messages {
            if let Some(id) = json_string(message, "id") {
                let remote = remote_payloads
                    .get(&(wire_topic.clone(), id.clone()))
                    .or_else(|| remote_payloads.get(&(full_topic.clone(), id)));
                *message = merge_remote_message_payload(remote, message);
            }
        }
    }

    for (owner_type, owner_id) in &owners {
        if owner_type == "group" {
            if let Some(row) = sqlx::query(
                "SELECT name, mode, group_prompt, invite_prompt, use_unified_model, unified_model, tag_match_mode, created_at
                 FROM groups WHERE owner_type = 'group' AND group_id = ? AND deleted_at IS NULL",
            )
            .bind(owner_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| format!("load group {owner_id} failed: {error}"))?
            {
                upload_entity(
                    client,
                    http_url,
                    sync_token,
                    "group",
                    owner_id,
                    json!({
                        "name": row.get::<String, _>("name"),
                        "mode": row.get::<String, _>("mode"),
                        "groupPrompt": row.get::<Option<String>, _>("group_prompt"),
                        "invitePrompt": row.get::<Option<String>, _>("invite_prompt"),
                        "useUnifiedModel": row.get::<i64, _>("use_unified_model") != 0,
                        "unifiedModel": row.get::<Option<String>, _>("unified_model"),
                        "tagMatchMode": row.get::<Option<String>, _>("tag_match_mode"),
                        "createdAt": row.get::<i64, _>("created_at"),
                        "ownerId": owner_id,
                    }),
                )
                .await?;
                summary.entities += 1;
            }
        } else if let Some(row) = sqlx::query(
            "SELECT name, system_prompt, model, temperature, context_token_limit, max_output_tokens, stream_output, updated_at
             FROM agents WHERE owner_type = 'agent' AND agent_id = ? AND deleted_at IS NULL",
        )
        .bind(owner_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| format!("load agent {owner_id} failed: {error}"))?
        {
            upload_entity(
                client,
                http_url,
                sync_token,
                "agent",
                owner_id,
                json!({
                    "name": row.get::<String, _>("name"),
                    "systemPrompt": row.get::<String, _>("system_prompt"),
                    "model": row.get::<String, _>("model"),
                    "temperature": row.get::<f64, _>("temperature"),
                    "contextTokenLimit": row.get::<i64, _>("context_token_limit"),
                    "maxOutputTokens": row.get::<i64, _>("max_output_tokens"),
                    "streamOutput": row.get::<i64, _>("stream_output") != 0,
                    "updatedAt": row.get::<i64, _>("updated_at"),
                    "ownerId": owner_id,
                }),
            )
            .await?;
            summary.entities += 1;
        }
    }

    for (owner_type, owner_id, topic_id) in messages_by_topic.keys() {
        let meta = topic_meta.get(&(owner_type.clone(), owner_id.clone(), topic_id.clone()));
        let (name, created_at, locked, unread) = meta
            .cloned()
            .unwrap_or_else(|| ("未命名话题".to_string(), 0, true, false));
        let entity_topic_id = hub_topic_key(owner_id, topic_id);
        let entity_type = if owner_type == "group" {
            "group_topic"
        } else {
            "agent_topic"
        };
        upload_entity(
            client,
            http_url,
            sync_token,
            entity_type,
            &entity_topic_id,
            json!({
                "id": entity_topic_id,
                "name": name,
                "createdAt": created_at,
                "locked": locked,
                "unread": unread,
                "ownerId": owner_id,
                "ownerType": owner_type,
            }),
        )
        .await?;
        summary.topics += 1;
    }

    for ((_owner_type, owner_id, topic_id), messages) in &messages_by_topic {
        let topic_key = hub_topic_key(owner_id, topic_id);
        upload_message_batch(client, http_url, sync_token, &topic_key, messages).await?;
        summary.messages += messages.len();
    }

    let deleted_rows =
        sqlx::query("SELECT msg_id FROM messages WHERE deleted_at IS NOT NULL AND deleted_at >= ?")
            .bind(since_timestamp)
            .fetch_all(pool)
            .await
            .map_err(|error| format!("load local deletions failed: {error}"))?;
    for row in deleted_rows {
        let msg_id: String = row.get("msg_id");
        delete_remote_message(client, http_url, sync_token, &msg_id).await?;
        summary.deleted_messages += 1;
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_fixture() -> HubSnapshot {
        HubSnapshot {
            latest_cursor: 9,
            entities: vec![
                HubEntity {
                    entity_type: "agent".to_string(),
                    entity_id: "agent-1".to_string(),
                    owner_type: None,
                    owner_id: None,
                    payload: json!({
                        "name": "拾一助手",
                        "systemPrompt": "hi",
                        "model": "gemini",
                        "temperature": 0.2,
                        "contextTokenLimit": 100,
                        "maxOutputTokens": 50,
                        "streamOutput": true,
                        "extraIgnored": true
                    }),
                },
                HubEntity {
                    entity_type: "topic".to_string(),
                    entity_id: "topic-1".to_string(),
                    owner_type: Some("agent".to_string()),
                    owner_id: Some("agent-1".to_string()),
                    payload: json!({
                        "name": "办公室",
                        "createdAt": 10,
                        "ownerId": "agent-1"
                    }),
                },
            ],
            messages: vec![
                HubSnapshotMessage {
                    topic_id: "topic-1".to_string(),
                    message_id: Some("msg-1".to_string()),
                    timestamp: Some(11),
                    payload: json!({
                        "id": "msg-1",
                        "role": "user",
                        "content": "hello from office",
                        "timestamp": 11
                    }),
                },
                HubSnapshotMessage {
                    topic_id: "agent-1/default".to_string(),
                    message_id: Some("msg-2".to_string()),
                    timestamp: Some(12),
                    payload: json!({
                        "messageId": "msg-2",
                        "role": "assistant",
                        "content": ["not", "a", "string"],
                        "timestamp": 12
                    }),
                },
            ],
        }
    }

    #[test]
    fn groups_short_and_compound_topic_ids() {
        let grouped = group_snapshot(&snapshot_fixture());
        assert_eq!(grouped.agents.len(), 1);
        assert_eq!(grouped.topics_by_owner.len(), 1);
        assert!(grouped.messages_by_topic.contains_key(&(
            "agent".to_string(),
            "agent-1".to_string(),
            "topic-1".to_string()
        )));
        assert!(grouped.messages_by_topic.contains_key(&(
            "agent".to_string(),
            "agent-1".to_string(),
            "default".to_string()
        )));
    }

    #[test]
    fn agent_dto_ignores_unknown_config_fields() {
        let dto = agent_dto_from_payload(&snapshot_fixture().entities[0].payload);
        assert_eq!(dto.name, "拾一助手");
        assert_eq!(dto.temperature, 0.2);
    }

    #[test]
    fn hub_topic_key_matches_desktop_client() {
        assert_eq!(hub_topic_key("agent-1", "default"), "agent-1/default");
        assert_eq!(hub_topic_key("agent-1", "topic-1"), "topic-1");
    }

    #[test]
    fn legacy_attachments_warn_without_losing_body_or_mutating_source() {
        let payload = json!({
            "id": "legacy", "role": "user", "content": "正文仍然保留", "timestamp": 1,
            "attachments": [{"name": "old.md", "src": "file:///desktop/old.md"}]
        });
        let original = payload.clone();
        let mut warnings = BoundedWarnings::default();
        let message =
            chat_message_from_payload(payload.clone(), "topic", "agent", "a", &mut warnings)
                .unwrap();
        assert_eq!(message.content, "正文仍然保留");
        assert!(message.attachments.is_none());
        assert_eq!(warnings.count, 1);
        assert_eq!(payload, original);
    }

    #[test]
    fn nested_hashes_are_promoted_and_desktop_paths_are_removed() {
        let hash = "ab".repeat(32);
        let mut warnings = BoundedWarnings::default();
        let message = chat_message_from_payload(
            json!({
                "id": "nested", "role": "user", "timestamp": 1,
                "attachments": [{"name": "image.png", "src": "file:///desktop/image.png",
                    "internalPath": "/desktop/image.png", "status": "ready",
                    "_fileManagerData": {"hash": hash.to_uppercase(), "extractedText": "text"}}]
            }),
            "topic",
            "agent",
            "a",
            &mut warnings,
        )
        .unwrap();
        let attachment = &message.attachments.unwrap()[0];
        assert_eq!(attachment.hash.as_deref(), Some(hash.as_str()));
        assert_eq!(attachment.extracted_text.as_deref(), Some("text"));
        assert!(attachment.src.is_empty());
        assert!(attachment.internal_path.is_empty());
        assert!(attachment.status.is_none());
        assert_eq!(warnings.count, 0);
    }

    #[test]
    fn conflicting_hashes_warn_and_samples_are_bounded() {
        let attachments = (0..12)
            .map(|_| {
                json!({
                    "hash": "a".repeat(64), "_fileManagerData": {"hash": "b".repeat(64)}
                })
            })
            .collect::<Vec<_>>();
        let mut warnings = BoundedWarnings::default();
        let message = chat_message_from_payload(
            json!({
                "id": "conflict", "attachments": attachments
            }),
            "topic",
            "agent",
            "a",
            &mut warnings,
        )
        .unwrap();
        assert!(message.attachments.is_none());
        assert_eq!(warnings.count, 12);
        assert_eq!(warnings.samples.len(), 8);
    }

    #[test]
    fn malformed_hub_messages_are_reported_not_silently_discarded() {
        for payload in [
            json!(null),
            json!({"role": "user"}),
            json!({"id": "bad", "attachments": 42}),
        ] {
            assert!(chat_message_from_payload(
                payload,
                "topic",
                "agent",
                "a",
                &mut BoundedWarnings::default()
            )
            .is_err());
        }
    }

    #[test]
    fn upload_preserves_legacy_attachment_metadata_after_mobile_normalization() {
        let remote = json!({"id": "m", "content": "before", "attachments": [
            {"name": "legacy.md", "_fileManagerData": {"internalPath": "/desktop/legacy.md"}}
        ], "desktopExtra": true});
        let local = json!({"id": "m", "content": "edited on phone"});
        let merged = merge_remote_message_payload(Some(&remote), &local);
        assert_eq!(merged["content"], local["content"]);
        assert_eq!(merged["attachments"], remote["attachments"]);
        assert_eq!(merged["desktopExtra"], true);
        assert_eq!(merge_remote_message_payload(None, &local), local);
    }

    #[tokio::test]
    async fn snapshot_with_legacy_attachments_commits_messages_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("hub-test.db");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(include_str!("../../../migrations/0100_baseline_v2.sql"))
                .unwrap();
        }
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(&db_path))
            .await
            .unwrap();
        let queue = DbWriteQueue::new(pool.clone(), db_path);
        let mut snapshot = snapshot_fixture();
        snapshot.messages[0].payload["attachments"] = json!([
            {"name": "legacy.md", "src": "file:///desktop/legacy.md"},
            {"name": "valid.png", "_fileManagerData": {"hash": "a".repeat(64)}}
        ]);
        for _ in 0..2 {
            let summary = apply_snapshot(&snapshot, &queue, false, &pool)
                .await
                .expect("legacy attachment must not roll back message writes");
            assert_eq!(summary.messages, 2);
            assert_eq!(summary.legacy_attachment_warnings, 1);
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(count, 2);
            let content: String =
                sqlx::query_scalar("SELECT content FROM messages WHERE msg_id = 'msg-1'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(content, "hello from office");
            let attachments: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM message_attachments")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(attachments, 1);
            let counts: Vec<i64> =
                sqlx::query_scalar("SELECT msg_count FROM topics ORDER BY topic_id")
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            assert_eq!(counts, vec![1, 1], "snapshot must update both topic badges");
        }
        assert!(snapshot.messages[0].payload["attachments"][0]
            .get("hash")
            .is_none());
        drop(queue);
        pool.close().await;
    }

    #[test]
    fn history_payload_becomes_chat_message() {
        let grouped = group_snapshot(&snapshot_fixture());
        let payloads = grouped
            .messages_by_topic
            .get(&(
                "agent".to_string(),
                "agent-1".to_string(),
                "default".to_string(),
            ))
            .cloned()
            .unwrap();
        let message = chat_message_from_payload(
            payloads[0].clone(),
            "default",
            "agent",
            "agent-1",
            &mut BoundedWarnings::default(),
        )
        .expect("message");
        assert_eq!(message.id, "msg-2");
        assert_eq!(message.topic_id.as_deref(), Some("default"));
        assert!(!message.content.is_empty());
    }
}
