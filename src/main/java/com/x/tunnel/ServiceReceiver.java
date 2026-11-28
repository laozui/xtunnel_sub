/*
 ============================================================================
 Name        : ServiceReceiver.java
 Author      : hev <r@hev.cc>
 Copyright   : Copyright (c) 2023 xyz
 Description : ServiceReceiver
 ============================================================================
 */

package com.x.tunnel;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.net.VpnService;
import android.os.Build;
import android.util.Log;

public class ServiceReceiver extends BroadcastReceiver {
	@Override
	public void onReceive(Context context, Intent intent) {
        if (intent == null || intent.getAction() == null) return;
        String action = intent.getAction();

        if (Intent.ACTION_BOOT_COMPLETED.equals(action)) {
            /* 开机不自连：尊重用户上次状态，避免一开机就拉起 VPN。
               （原逻辑：写死关闭；保留，避免改变既有行为。） */
            Preferences prefs = new Preferences(context);
            prefs.setEnable(false);
            return;
        }

        /* ★ v2.10.25 冷重启自拉起：闹钟到点后检查 enable，仍开着才拉全新进程。
           Android 8+ 后台启动服务限制 → 用 startForegroundService；
           服务侧 onStartCommand 开头已提前挂前台通知，满足 5 秒约束。 */
        if (TProxyService.ACTION_RESTART_WAKE.equals(action)) {
            try {
                Preferences prefs = new Preferences(context);
                if (!prefs.getEnable()) {
                    Log.i("pat", "冷重启自拉起取消：用户已关闭 VPN");
                    return;
                }
                Intent svc = new Intent(context, TProxyService.class);
                svc.setAction(TProxyService.ACTION_CONNECT);
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                    context.startForegroundService(svc);
                } else {
                    context.startService(svc);
                }
                Log.w("pat", "冷重启：闹钟到点，已拉起全新 PAT 服务进程");
            } catch (Throwable t) {
                Log.e("pat", "冷重启自拉起失败: " + t.getMessage());
            }
            return;
        }
	}
}
