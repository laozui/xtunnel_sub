package com.x.tunnel;

import android.content.Context;
import android.content.Intent;
import android.os.Handler;
import android.os.Looper;
import android.text.TextUtils;
import android.util.Log;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cloudflare 本地动态优选 IP 引擎 (小羊弹性质量评分算法移植版)
 *
 * 核心机制：
 * 1. 纯原生高精度 TCP 握手探针 (System.nanoTime 纳秒单调时钟)
 * 2. 两级漏斗机制：初筛并发淘汰不可达节点 -> 12强入围节点三轮抖动采样
 * 3. 小羊弹性质量综合打分模型：
 *    Score = p50 * (1.0 + 1.5 * CV) + 0.8 * (p95 - p50) + 15.0 * loss_pct
 * 4. 自动选取 Top 5 最优 IP，持久化写入配置
 * 5. 若 VPN 正在运行，通过 TProxyService ACTION_SWITCH 实现无感平滑热重载 (钥匙不闪断)
 */
public class CfOptimizer {
    private static final String TAG = "CfOptimizer";
    private static final int PROBE_PORT = 443;
    private static final int PROBE_TIMEOUT_MS = 1200;
    private static final int TOP_N_SELECT = 5;

    /* 内置优质 Cloudflare 跨运营商种子 IP 库 */
    private static final String[] BUILTIN_CF_IPS = {
        "104.16.0.0", "104.17.0.0", "104.18.0.0", "104.19.0.0", "104.20.0.0",
        "104.21.0.0", "104.22.0.0", "104.24.0.0", "104.25.0.0", "104.26.0.0",
        "104.27.0.0", "104.28.0.0", "172.64.0.0", "172.67.0.0", "108.162.192.0",
        "141.101.64.0", "162.158.0.0", "198.41.128.0", "197.234.240.0", "188.114.96.0",
        "108.162.192.42", "172.67.182.112", "104.21.58.12", "104.19.34.8", "104.16.132.22",
        "162.159.192.1", "162.159.193.1", "162.159.195.1", "104.17.209.9", "104.18.21.22",
        "172.64.150.1", "172.64.155.1", "104.26.12.1", "104.27.144.1", "104.22.8.1",
        "104.25.7.1"
    };

    public static final int STATUS_IDLE = 0;
    public static final int STATUS_RUNNING = 1;
    public static final int STATUS_SUCCESS = 2;
    public static final int STATUS_FAILED = 3;

    private static volatile int currentStatus = STATUS_IDLE;
    private static final AtomicBoolean isRunning = new AtomicBoolean(false);
    private static final ExecutorService workerPool = Executors.newFixedThreadPool(6);
    private static final Handler mainHandler = new Handler(Looper.getMainLooper());
    private static final Handler schedulerHandler = new Handler(Looper.getMainLooper());

    private static Runnable schedulerRunnable = null;
    private static OptimizerCallback activeCallback = null;

    public interface OptimizerCallback {
        void onStatusChanged(int status, String message);
        void onComplete(boolean success, String topIps, String summary);
    }

    public static void setCallback(OptimizerCallback callback) {
        activeCallback = callback;
    }

    public static int getStatus() {
        return currentStatus;
    }

    public static boolean isRunning() {
        return isRunning.get();
    }

    public static boolean isOptimizing() {
        return isRunning.get();
    }

    /**
     * 单次 TCP 握手探测 (单调纳秒时钟)
     */
    public static float probeTcpHandshake(String ip, int port, int timeoutMs) {
        long startNano = System.nanoTime();
        Socket socket = null;
        try {
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(ip, port), timeoutMs);
            long endNano = System.nanoTime();
            return (endNano - startNano) / 1000000.0f;
        } catch (Throwable t) {
            return -1.0f;
        } finally {
            if (socket != null) {
                try { socket.close(); } catch (Throwable ignored) {}
            }
        }
    }

    public static class CandidateItem {
        public String ip;
        public float firstLatency = 9999.0f;
        public boolean firstSuccess = false;

        public float[] samples = new float[3];
        public int successCount = 0;

        public float p50 = 0.0f;
        public float p95 = 0.0f;
        public float mean = 0.0f;
        public float stddev = 0.0f;
        public float cv = 0.0f;
        public float jitter = 0.0f;
        public float lossPct = 100.0f;
        public float elasticScore = 99999.0f;

        public CandidateItem(String ip) {
            this.ip = ip;
        }

        public void computeMetricsAndScore() {
            List<Float> validSamples = new ArrayList<>();
            for (float s : samples) {
                if (s > 0.0f) validSamples.add(s);
            }

            if (validSamples.isEmpty()) {
                this.lossPct = 100.0f;
                this.elasticScore = 99999.0f;
                return;
            }

            this.lossPct = ((3 - validSamples.size()) / 3.0f) * 100.0f;

            Collections.sort(validSamples);
            int n = validSamples.size();
            this.p50 = validSamples.get(n / 2);
            this.p95 = validSamples.get(n - 1);

            float sum = 0.0f;
            for (float v : validSamples) sum += v;
            this.mean = sum / n;

            float varSum = 0.0f;
            for (float v : validSamples) {
                float diff = v - this.mean;
                varSum += diff * diff;
            }
            this.stddev = (float) Math.sqrt(varSum / n);
            this.cv = (this.mean > 0.001f) ? (this.stddev / this.mean) : 0.0f;

            // RFC 3550 Jitter
            float curJitter = 0.0f;
            for (int i = 1; i < n; i++) {
                float diff = Math.abs(validSamples.get(i) - validSamples.get(i - 1));
                curJitter += (diff - curJitter) / 16.0f;
            }
            this.jitter = curJitter;

            // 小羊弹性质量评分公式
            this.elasticScore = this.p50 * (1.0f + 1.5f * this.cv)
                    + 0.8f * (this.p95 - this.p50)
                    + 15.0f * this.lossPct;
        }
    }

    /**
     * 启动本地动态优选
     */
    public static void startOptimize(final Context context, final boolean manual) {
        if (!isRunning.compareAndSet(false, true)) {
            Log.w(TAG, "CfOptimizer 任务已在运行中，跳过重复调用");
            return;
        }

        currentStatus = STATUS_RUNNING;
        notifyStatus(STATUS_RUNNING, manual ? "开始本地测速打分..." : "15分钟自适应巡检中...");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    doOptimizeWorkflow(context, manual);
                } catch (Throwable t) {
                    Log.e(TAG, "CfOptimizer 运行异常", t);
                    currentStatus = STATUS_FAILED;
                    notifyStatus(STATUS_FAILED, "优选测速发生异常: " + t.getMessage());
                } finally {
                    isRunning.set(false);
                }
            }
        }, "cf-optimizer-worker").start();
    }

    private static void doOptimizeWorkflow(final Context context, boolean manual) {
        final Preferences prefs = new Preferences(context);

        // 1. 准备种子候选池
        List<String> pool = new ArrayList<>();
        for (String ip : BUILTIN_CF_IPS) {
            if (!pool.contains(ip)) pool.add(ip);
        }

        Log.i(TAG, "候选池就绪: 共 " + pool.size() + " 个优质 IP，开始并发初筛...");

        // 2. 第一轮初筛：非阻塞并发快速探测
        final List<CandidateItem> items = new ArrayList<>();
        for (String ip : pool) items.add(new CandidateItem(ip));

        final CountDownLatch latch1 = new CountDownLatch(items.size());
        for (final CandidateItem item : items) {
            workerPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        float lat = probeTcpHandshake(item.ip, PROBE_PORT, PROBE_TIMEOUT_MS);
                        if (lat > 0.0f) {
                            item.firstLatency = lat;
                            item.firstSuccess = true;
                        } else {
                            item.firstLatency = 9999.0f;
                            item.firstSuccess = false;
                        }
                    } finally {
                        latch1.countDown();
                    }
                }
            });
        }

        try {
            latch1.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Log.e(TAG, "初筛等待超时", e);
        }

        // 按初筛延迟排序
        Collections.sort(items, new Comparator<CandidateItem>() {
            @Override
            public int compare(CandidateItem a, CandidateItem b) {
                return Float.compare(a.firstLatency, b.firstLatency);
            }
        });

        int validInitial = 0;
        for (CandidateItem it : items) {
            if (it.firstSuccess && it.firstLatency < 600.0f) validInitial++;
        }

        if (validInitial == 0) {
            Log.w(TAG, "当前本地网络下所有候选 IP 均未能握手成功，请检查网络！");
            currentStatus = STATUS_FAILED;
            notifyStatus(STATUS_FAILED, "未测得可用节点，请检查网络");
            notifyComplete(false, "", "所有节点握手超时");
            return;
        }

        // 3. 第二轮精测：对入围前 12 个优质节点展开 3 次重复采样与小羊抖动弹性打分
        int refineCount = Math.min(validInitial, 12);
        final List<CandidateItem> topCandidates = new ArrayList<>(items.subList(0, refineCount));
        Log.i(TAG, "初筛完成，入围 " + refineCount + " 个节点，开始采样评估...");

        for (CandidateItem item : topCandidates) {
            item.successCount = 0;
            for (int s = 0; s < 3; s++) {
                float lat = probeTcpHandshake(item.ip, PROBE_PORT, PROBE_TIMEOUT_MS);
                item.samples[s] = lat;
                if (lat > 0.0f) item.successCount++;
                try { Thread.sleep(15); } catch (Throwable ignored) {}
            }
            item.computeMetricsAndScore();
        }

        // 按小羊弹性得分升序排列 (分数越低越优)
        Collections.sort(topCandidates, new Comparator<CandidateItem>() {
            @Override
            public int compare(CandidateItem a, CandidateItem b) {
                return Float.compare(a.elasticScore, b.elasticScore);
            }
        });

        // 4. 选取 Top 5 最优 IP
        int pickCount = Math.min(topCandidates.size(), TOP_N_SELECT);
        StringBuilder sbTop = new StringBuilder();
        for (int i = 0; i < pickCount; i++) {
            if (i > 0) sbTop.append(",");
            sbTop.append(topCandidates.get(i).ip);
        }
        final String newTopIps = sbTop.toString();

        CandidateItem best = topCandidates.get(0);
        final String summary = String.format(Locale.getDefault(),
                "最低 %.1fms / 抖动 %.1fms (CV: %.2f)", best.p50, best.jitter, best.cv);

        String nowStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault()).format(new Date());

        // 5. 持久化存盘
        boolean ipChanged = !newTopIps.equals(prefs.getCfOptTopIps());
        prefs.setCfOptTopIps(newTopIps);
        prefs.setCfOptLastTime(nowStr);
        prefs.setCfOptSummary(summary);

        // 应用到节点
        boolean applyAll = prefs.getCfOptApplyAll();
        if (applyAll) {
            Set<String> allIds = prefs.getProfileIds();
            for (String pid : allIds) {
                prefs.setPrefIp(pid, newTopIps);
            }
            Log.i(TAG, "已将 Top 5 优选 IP 同步应用到全部 " + allIds.size() + " 个节点: " + newTopIps);
        } else {
            String currentId = prefs.getCurrentProfileId();
            if (!TextUtils.isEmpty(currentId)) {
                prefs.setPrefIp(currentId, newTopIps);
                Log.i(TAG, "已将 Top 5 优选 IP 应用到当前节点 [" + currentId + "]: " + newTopIps);
            }
        }

        // 6. 若 VPN 正在运行，执行毫秒级平滑热生效 (钥匙不闪断)
        if (prefs.getEnable()) {
            if (ipChanged || manual) {
                Log.i(TAG, "正在平滑热重启代理内核，使最优 IP 立即生效...");
                String currentId = prefs.getCurrentProfileId();
                Intent switchIntent = new Intent(context, TProxyService.class);
                switchIntent.setAction(TProxyService.ACTION_SWITCH);
                switchIntent.putExtra(TProxyService.EXTRA_PROFILE_ID, currentId);
                switchIntent.putExtra(TProxyService.EXTRA_PREF_IP, newTopIps);
                context.startService(switchIntent);
            } else {
                Log.i(TAG, "最优 IP 集合保持稳定，维持现有连接不闪断。");
            }
        }

        currentStatus = STATUS_SUCCESS;
        notifyStatus(STATUS_SUCCESS, "优选完成！");
        notifyComplete(true, newTopIps, summary);
    }

    private static void notifyStatus(final int status, final String message) {
        mainHandler.post(new Runnable() {
            @Override
            public void run() {
                if (activeCallback != null) {
                    activeCallback.onStatusChanged(status, message);
                }
            }
        });
    }

    private static void notifyComplete(final boolean success, final String topIps, final String summary) {
        mainHandler.post(new Runnable() {
            @Override
            public void run() {
                if (activeCallback != null) {
                    activeCallback.onComplete(success, topIps, summary);
                }
            }
        });
    }

    /**
     * 启动定时自动巡检调度器
     */
    public static void startScheduler(final Context context) {
        stopScheduler();

        Preferences prefs = new Preferences(context);
        if (!prefs.getCfOptEnabled()) return;

        int intervalMinutes = prefs.getCfOptInterval();
        if (intervalMinutes <= 0) intervalMinutes = 60;
        long intervalMs = intervalMinutes * 60L * 1000L;

        schedulerRunnable = new Runnable() {
            @Override
            public void run() {
                Preferences p = new Preferences(context);
                if (p.getCfOptEnabled()) {
                    startOptimize(context.getApplicationContext(), false);
                }
                int nextMinutes = p.getCfOptInterval();
                if (nextMinutes <= 0) nextMinutes = 60;
                schedulerHandler.postDelayed(this, nextMinutes * 60L * 1000L);
            }
        };

        // 启动时延迟 3 秒启动首次预热巡检，之后按周期运行
        schedulerHandler.postDelayed(schedulerRunnable, 3000);
        Log.i(TAG, "CfOptimizer 定时巡检已启动，周期: " + intervalMinutes + " 分钟");
    }

    /**
     * 停止定时调度器
     */
    public static void stopScheduler() {
        if (schedulerRunnable != null) {
            schedulerHandler.removeCallbacks(schedulerRunnable);
            schedulerRunnable = null;
        }
    }
}
