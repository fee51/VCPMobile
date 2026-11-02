use super::*;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};

struct Fixture {
    url: String,
    calls: Arc<Mutex<Vec<(String, Value)>>>,
    fail_ack: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) { self.task.abort(); }
}
impl Fixture {
    async fn start() -> Self {
        use axum::{extract::State, http::Uri, routing::any, Router};
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fail_ack = Arc::new(AtomicBool::new(false));
        let state = (calls.clone(), fail_ack.clone());
        async fn handle(
            State((calls, fail)): State<(Arc<Mutex<Vec<(String, Value)>>>, Arc<AtomicBool>)>,
            uri: Uri,
            body: String,
        ) -> String {
            if uri.path().ends_with("/status") {
                return json!({"ok": true, "latestCursor": 0}).to_string();
            }
            if uri.path().ends_with("/snapshot") {
                return json!({"latestCursor": 0, "entities": [], "messages": []}).to_string();
            }
            if uri.path().ends_with("/upload-messages-batch") {
                let mut response = String::new();
                for line in body.lines().filter(|line| !line.is_empty()) {
                    let value: Value = serde_json::from_str(line).unwrap();
                    response.push_str(&json!({
                        "topicId": value["topicId"],
                        "success": !fail.load(Ordering::SeqCst),
                    }).to_string());
                    response.push('\n');
                    calls.lock().unwrap().push((uri.path().to_string(), value));
                }
                return response;
            }
            let value = serde_json::from_str(&body).unwrap_or(Value::Null);
            calls.lock().unwrap().push((uri.path().to_string(), value));
            json!({"success": true}).to_string()
        }
        let app = Router::new().fallback(any(handle)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        Self { url, calls, fail_ack, task }
    }
}

async fn database() -> sqlx::SqlitePool {
    let pool = sqlx::sqlite::SqlitePoolOptions::new().max_connections(1)
        .connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER);
         CREATE TABLE topics(owner_type TEXT,owner_id TEXT,topic_id TEXT,title TEXT,created_at INTEGER,locked INTEGER,unread INTEGER,deleted_at INTEGER);
         CREATE TABLE messages(owner_type TEXT,owner_id TEXT,topic_id TEXT,msg_id TEXT,role TEXT,name TEXT,agent_id TEXT,content TEXT,timestamp INTEGER,is_group_message INTEGER,group_id TEXT,finish_reason TEXT,updated_at INTEGER,deleted_at INTEGER);
         CREATE TABLE agents(owner_type TEXT,agent_id TEXT,name TEXT,system_prompt TEXT,model TEXT,temperature REAL,context_token_limit INTEGER,max_output_tokens INTEGER,stream_output INTEGER,updated_at INTEGER,deleted_at INTEGER);
         INSERT INTO topics VALUES('agent','a','t','Topic',1,1,0,NULL);"
    ).execute(&pool).await.unwrap();
    pool
}

async fn message(pool: &sqlx::SqlitePool, id: &str, updated: i64, deleted: Option<i64>) {
    sqlx::query("INSERT INTO messages VALUES('agent','a','t',?,'user',NULL,NULL,'edited',10,0,NULL,NULL,?,?)")
        .bind(id).bind(updated).bind(deleted).execute(pool).await.unwrap();
}

#[test]
fn acknowledgements_require_all_frames_and_matching_topic() {
    let good = "{\"topicId\":\"a/t\",\"success\":true}\n";
    assert!(validate_message_upload_ack(good, "a/t", 1).is_ok());
    assert!(validate_message_upload_ack(&(good.to_owned() + good), "a/t", 2).is_ok());
    assert!(validate_message_upload_ack("", "a/t", 1).is_err());
    assert!(validate_message_upload_ack(good, "a/t", 2).is_err());
    assert!(validate_message_upload_ack(good, "other/t", 1).is_err());
    assert!(validate_message_upload_ack("{\"topicId\":\"a/t\",\"success\":false}", "a/t", 1).is_err());
    assert!(validate_message_upload_ack("invalid-json", "a/t", 1).is_err());
}

#[test]
fn checkpoints_are_scoped_to_endpoint_credential_and_device() {
    let key = push_progress_key("https://hub/", "secret", "phone");
    assert_eq!(key, push_progress_key("https://hub", "secret", "phone"));
    assert_ne!(key, push_progress_key("https://other", "secret", "phone"));
    assert_ne!(key, push_progress_key("https://hub", "other", "phone"));
    assert_ne!(key, push_progress_key("https://hub", "secret", "other"));
    assert!(!key.contains("secret"));
}

#[tokio::test]
async fn editing_a_known_old_message_uses_updated_at() {
    let fixture = Fixture::start().await;
    let pool = database().await;
    message(&pool, "m", 2000, None).await;
    let mut known = HashSet::from(["m".to_string()]);
    let summary = push_local_changes(
        &reqwest::Client::new(), &fixture.url, "token", "phone",
        &pool, 1000, &mut known, None,
    ).await.unwrap();
    assert_eq!(summary.messages, 1);
    let calls = fixture.calls.lock().unwrap();
    let (_, frame) = calls.iter().find(|(path, _)| path.ends_with("upload-messages-batch")).unwrap();
    assert_eq!(frame["messages"][0]["updatedAt"], 2000);
}

#[tokio::test]
async fn failed_ack_keeps_progress_then_offline_delete_retries_and_restart_resumes() {
    let fixture = Fixture::start().await;
    let pool = database().await;
    message(&pool, "m", 2000, None).await;
    message(&pool, "deleted-offline", 10, Some(500)).await;
    let key = push_progress_key(&fixture.url, "token", "phone");
    sqlx::query("INSERT INTO settings VALUES(?, '100', 100)").bind(&key)
        .execute(&pool).await.unwrap();
    let client = reqwest::Client::new();
    let mut known = HashSet::new();
    let mut baseline = None;
    fixture.fail_ack.store(true, Ordering::SeqCst);
    assert!(push_with_checkpoint(
        &client, &fixture.url, "token", "phone", &pool, &mut known, &mut baseline,
    ).await.is_err());
    let stored: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=?")
        .bind(&key).fetch_one(&pool).await.unwrap();
    assert_eq!(stored, "100");

    fixture.fail_ack.store(false, Ordering::SeqCst);
    let summary = push_with_checkpoint(
        &client, &fixture.url, "token", "phone", &pool, &mut known, &mut baseline,
    ).await.unwrap();
    assert_eq!(summary.deleted_messages, 1);
    assert!(fixture.calls.lock().unwrap().iter().any(|(path, body)|
        path.ends_with("/delete-message") && body["msgId"] == "deleted-offline"));

    // Simulate a new session: in-memory state is gone, SQLite progress survives.
    let summary = push_with_checkpoint(
        &client, &fixture.url, "token", "phone", &pool,
        &mut HashSet::new(), &mut None,
    ).await.unwrap();
    assert_eq!(summary.deleted_messages, 0);
}

fn remote(version: i64, content: &str) -> HubSnapshot {
    serde_json::from_value(json!({
        "latestCursor": 1,
        "messages": [{
            "topicId": "a/t", "messageId": "m",
            "payload": {"id":"m","content":content,"timestamp":10,"updatedAt":version,
                "attachments":[{"legacy":"must survive"}]}
        }]
    })).unwrap()
}

#[tokio::test]
async fn remote_newer_is_not_overwritten_and_equal_version_conflict_is_reported() {
    let fixture = Fixture::start().await;
    let pool = database().await;
    message(&pool, "m", 2000, None).await;
    let client = reqwest::Client::new();
    let summary = push_local_changes(
        &client, &fixture.url, "token", "phone", &pool, 0,
        &mut HashSet::new(), Some(&remote(3000, "remote-newer")),
    ).await.unwrap();
    assert_eq!(summary.messages, 0);
    assert!(fixture.calls.lock().unwrap().is_empty());
    let error = push_local_changes(
        &client, &fixture.url, "token", "phone", &pool, 0,
        &mut HashSet::new(), Some(&remote(2000, "different")),
    ).await.err().expect("same-version conflict");
    assert!(error.contains("conflict"));
}

#[tokio::test]
async fn upload_preserves_remote_attachment_metadata() {
    let fixture = Fixture::start().await;
    let pool = database().await;
    message(&pool, "m", 2000, None).await;
    push_local_changes(
        &reqwest::Client::new(), &fixture.url, "token", "phone", &pool, 0,
        &mut HashSet::new(), Some(&remote(1000, "before-edit")),
    ).await.unwrap();
    let calls = fixture.calls.lock().unwrap();
    let (_, frame) = calls.iter().find(|(path, _)| path.ends_with("upload-messages-batch")).unwrap();
    assert_eq!(frame["messages"][0]["attachments"][0]["legacy"], "must survive");
    assert_eq!(frame["messages"][0]["content"], "edited");
}