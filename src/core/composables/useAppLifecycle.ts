import { onMounted, onUnmounted, watch } from 'vue';
import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { useAppLifecycleStore } from '../stores/appLifecycle';
import { useChatStreamStore } from '../stores/chatStreamStore';
import { useDataReload } from './useDataReload';

export function useAppLifecycle() {
  const lifecycleStore = useAppLifecycleStore();
  const streamStore = useChatStreamStore();
  const { performFullReload } = useDataReload();
  let unlisten: UnlistenFn | null = null;
  let unlistenSyncCompleted: UnlistenFn | null = null;
  let recoveryPromise: Promise<void> | null = null;
  let reloadPromise: Promise<void> | null = null;

  const reconcileInterruptedStreams = (source: string) => {
    if (lifecycleStore.state !== 'READY' || lifecycleStore.isBackground) return;
    if (recoveryPromise) return recoveryPromise;
    console.log(`[useAppLifecycle] ${source}. Triggering stream recovery.`);
    recoveryPromise = streamStore.checkAndRecoverInterruptedStreams()
      .catch(err => {
        console.error(`[useAppLifecycle] Failed to recover streams (${source}):`, err);
      })
      .finally(() => {
        recoveryPromise = null;
      });
    return recoveryPromise;
  };

  // 中断流恢复属于应用生命周期调和：冷启动、WebView 重载均由 READY 转换覆盖。
  watch(
    () => lifecycleStore.state,
    (state, previous) => {
      if (state === 'READY' && previous !== 'READY') {
        void reconcileInterruptedStreams('Core ready');
      }
    },
  );

  // 监听后台状态，控制全局动画挂起，替代 App.vue 中的 watch
  watch(() => lifecycleStore.isBackground, (newVal) => {
    if (newVal) {
      document.documentElement.classList.add("vcp-paused-animations");
      console.log("[useAppLifecycle] App moved to background, pausing animations.");
    } else {
      document.documentElement.classList.remove("vcp-paused-animations");
      console.log("[useAppLifecycle] App moved to foreground, resuming animations.");
      lifecycleStore.hydrateSystemStatus().catch((err) => {
        console.error("[useAppLifecycle] Failed to hydrate system status:", err);
      });
      void reconcileInterruptedStreams('App foreground');
    }
  }, { immediate: true });

  const handleVisibilityChange = () => {
    if (typeof document !== 'undefined') {
      const isHidden = document.hidden;
      lifecycleStore.isBackground = isHidden;
      console.log(`[useAppLifecycle] Visibility changed: hidden=${isHidden}`);
    }
  };

  const handleOnline = () => {
    void reconcileInterruptedStreams("Device online");
  };

  onMounted(async () => {
    if (typeof window !== 'undefined') {
      document.addEventListener("visibilitychange", handleVisibilityChange);
      window.addEventListener("online", handleOnline);
    }

    try {
      unlistenSyncCompleted = await listen<{ status?: string }>("vcp-sync-completed", (event) => {
        const status = event.payload?.status;
        if (status !== "completed" && status !== "completed_with_warnings") return;
        if (lifecycleStore.state !== "READY" || lifecycleStore.isBackground) return;
        if (reloadPromise) return;
        reloadPromise = performFullReload()
          .catch((err) => {
            console.error("[useAppLifecycle] Failed to reload after sync:", err);
          })
          .finally(() => {
            reloadPromise = null;
          });
      });

      unlisten = await listen<{ state: string }>("vcp-lifecycle-changed", (event) => {
        const state = event.payload.state;
        console.log(`[useAppLifecycle] Received vcp-lifecycle-changed: state=${state}`);
        
        if (state === 'pause' || state === 'stop') {
          lifecycleStore.isBackground = true;
        } else if (state === 'resume') {
          lifecycleStore.isBackground = false;
          invoke('start_manual_sync').catch(err => {
            console.error('[useAppLifecycle] Failed to resume automatic sync:', err);
          });
          // 原生 resume 可能先于 document.visibilityState 变化，显式调和一次。
          void reconcileInterruptedStreams("Native resume");
        }
      });
    } catch (err) {
      console.error("[useAppLifecycle] Failed to setup Tauri lifecycle listener:", err);
    }
  });

  onUnmounted(() => {
    if (typeof window !== 'undefined') {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      window.removeEventListener("online", handleOnline);
    }
    if (unlisten) {
      unlisten();
    }
    if (unlistenSyncCompleted) {
      unlistenSyncCompleted();
    }
  });
}
