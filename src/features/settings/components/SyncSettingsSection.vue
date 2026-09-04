<script setup lang="ts">
import { ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import type { AppSettings } from "../../../core/stores/settings";
import { useOverlayStore } from "../../../core/stores/overlay";
import SettingsTextField from "../../../components/settings/SettingsTextField.vue";
import SettingsActionButton from "../../../components/settings/SettingsActionButton.vue";
import SettingsActionWithStatus from "../../../components/settings/SettingsActionWithStatus.vue";
import SettingsRow from "../../../components/settings/SettingsRow.vue";

defineProps<{
  settings: AppSettings;
}>();

const overlayStore = useOverlayStore();

const emit = defineEmits<{
  (e: "save-request"): void;
}>();

const emoticonStatus = ref<{
  type: "success" | "error" | "loading" | null;
  message: string;
}>({ type: null, message: "" });

const startManualSync = () => {
  overlayStore.openSyncSession();
};

const rebuildEmoticonLibrary = async () => {
  emit("save-request");
  emoticonStatus.value = { type: "loading", message: "正在从远程服务器获取..." };
  try {
    const count = await invoke<number>("regenerate_emoticon_library");
    emoticonStatus.value = {
      type: "success",
      message: `同步成功：共计 ${count} 个表情`,
    };
    setTimeout(() => {
      emoticonStatus.value = { type: null, message: "" };
    }, 3000);
  } catch (e: any) {
    emoticonStatus.value = { type: "error", message: `同步失败: ${e}` };
  }
};
</script>

<template>
  <div class="space-y-5 px-1">
    <p class="text-xs leading-5 opacity-60 px-0.5">
      办公室电脑和手机不在同一网络时，填写云端 SyncHub 地址，令牌与电脑
      <span class="font-mono">AppData/cloud-sync.json</span>
      相同。同一 WiFi 下也可继续填电脑局域网 5974/5975。
    </p>
    <SettingsTextField
      v-model="settings.syncHttpUrl"
      label="HTTP 服务 URL"
      placeholder="https://hub-vcp.0012138.xyz/hub 或 http://192.168.x.x:5974"
      mono
    />
    <SettingsTextField
      v-model="settings.syncServerUrl"
      label="WebSocket 服务 URL"
      placeholder="wss://hub-vcp.0012138.xyz/hub-ws 或 ws://192.168.x.x:5975"
      mono
    />
    <SettingsTextField
      v-model="settings.syncToken"
      is-secure
      label="Mobile Sync Token"
      placeholder="与电脑 AppData/cloud-sync.json 的 syncToken 一致"
      mono
    />
    <SettingsTextField
      v-model="settings.syncDeviceId"
      label="同步设备 ID"
      placeholder="首次启动自动生成"
      mono
      readonly
    />

    <div class="border-t border-black/5 dark:border-white/5 pt-2 space-y-4">
      <SettingsTextField
        v-model="settings.fileKey"
        is-secure
        label="表情包图床密钥 (fileKey)"
        placeholder="用于构造表情包 URL 的密码"
        mono
      />

      <SettingsActionWithStatus
        title="表情包修复库"
        description="从 VCP 服务器同步表情包元数据"
        button-variant="secondary"
        button-size="sm"
        button-label="REFRESH"
        :button-loading="emoticonStatus.type === 'loading'"
        :status-type="emoticonStatus.type"
        :status-message="emoticonStatus.message"
        @action-click="rebuildEmoticonLibrary"
      />

      <SettingsRow
        title="全量神经同步"
        description="打开全量神经同步面板，查看历史日志或开始同步"
      >
        <template #action>
          <SettingsActionButton
            variant="secondary"
            size="sm"
            @click="startManualSync"
          >
            OPEN PANEL
          </SettingsActionButton>
        </template>
      </SettingsRow>
    </div>
  </div>
</template>
