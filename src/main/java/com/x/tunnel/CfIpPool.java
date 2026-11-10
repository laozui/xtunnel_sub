package com.x.tunnel;

import android.content.Context;
import android.content.pm.PackageManager;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import android.telephony.SubscriptionInfo;
import android.telephony.SubscriptionManager;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地可用 IP 池（移动端三网络分区版）
 *
 * 设计要点（与桌面端两层架构对齐，但针对手机的多运营商异构网络做了扩展）：
 *   1. 手机可能处于三种截然不同的网络：WiFi（家里/公司可能还是不同运营商）、卡槽1 数据、卡槽2 数据。
 *      因此在同一运营商下测出的"黄金 IP"，换一条线路很可能完全不通。
 *      => 把 IP 池拆成 3 个独立分区：wifi / sim1 / sim2，每区上限 50 个。
 *   2. 更新池子时【只更新当前所在网络对应的那一个分区】，互不污染。
 *      => 在 WiFi 下更新只动 wifi 池；切到卡槽 2 更新只动 sim2 池。
 *   3. 演进合并规则（老豆定式，按 50 容量等比换算）：
 *      已有旧池 -> 保留旧池最前面的 10 个（历史稳定 IP）+ 本次新扫出的最优 40 个（去重后装满 50）；
 *      首次无池 -> 直接取本次新扫出的最优 50 个。
 *   4. 优选时（{@link CfOptimizer}）把三个分区合并去重后统一打分，从中挑 Top N（N = 节点并发数）。
 *      => 从 WiFi 走进电梯切 5G 的瞬间，优选引擎可以直接在合并池里秒选当前线路能通的最快 IP。
 */
public class CfIpPool {

    private static final String TAG = "CfIpPool";

    public static final String SLOT_WIFI = "wifi";
    public static final String SLOT_SIM1 = "sim1";
    public static final String SLOT_SIM2 = "sim2";

    /** 单分区容量上限 */
    public static final int POOL_CAPACITY = 200;
    /** 期望新扫出的 IP 数量 */
    private static final int SCAN_TARGET_NEW = 40;

    /* 深挖补足：三地扫不满 200 时，从最快的若干个 /24 段里再抽 IP 实测补齐。
       经验依据：排名靠前的 /24 段整体质量都不错（实测 172.64.53.x 全部落地 NRT），
       只是在"每 /24 只留 1 个"的规则下被浪费了。 */
    private static final int FILL_CIDR_COUNT = 5;
    private static final int FILL_PER_CIDR = 40;

    /* 扫描参数：手机上兼顾速度与耗电，单次随机采样 1200 个子网 + 64 路并发 */
    private static final int SCAN_MAX_SUBNETS = 6534;
    private static final int SCAN_CONCURRENCY = 64;
    private static final int SCAN_TIMEOUT_MS = 700;
    static final int PROBE_PORT = 443;

    private static final String ASSET_SUBNETS = "cf_subnets.txt";

    private static final AtomicBoolean scanning = new AtomicBoolean(false);
    private static final Handler mainHandler = new Handler(Looper.getMainLooper());

    /* ======================= 回调 ======================= */

    public interface PoolCallback {
        /** 进度：done/total 为探测进度，hint 为阶段说明（可选实现） */
        default void onProgress(int done, int total, String hint) { }

        /** 完成：success=false 时 message 说明失败原因 */
        void onFinish(boolean success, String slot, int poolSize, String message);
    }

    /* ======================= 网络分区识别 ======================= */

    /** 检测当前所处的网络分区键（wifi / sim1 / sim2）。 */
    public static String detectSlot(Context context) {
        try {
            ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) return SLOT_WIFI;
            Network active = cm.getActiveNetwork();
            NetworkCapabilities caps = active == null ? null : cm.getNetworkCapabilities(active);

            /* 注意：VPN 建立后 getActiveNetwork() 返回的是 VPN 网络，
               它只带 TRANSPORT_VPN，无法据此判断底层是 WiFi 还是哪张卡。
               此时代理进程正在跑，必须回溯到真实底层承载网络。 */
            if (caps == null || caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) {
                NetPick pick = findUnderlying(cm);
                caps = (pick == null) ? null : pick.caps;
            }
            if (caps == null) return SLOT_WIFI;

            if (caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)) {
                return detectCellularSlot(context);
            }
            // WiFi / 以太网 / 其他一律归入 wifi 分区（同一出口线路即同一分区）
            return SLOT_WIFI;
        } catch (Throwable t) {
            Log.w(TAG, "detectSlot 失败: " + t.getMessage());
            return SLOT_WIFI;
        }
    }

    /**
     * 取当前真实底层承载网络（非 VPN），供探测 socket 绑定使用；
     * VPN 在线时探测若走隧道，测到的就不是本地线路质量了。
     */
    public static Network getUnderlyingNetwork(Context context) {
        try {
            ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
            if (cm == null) return null;
            Network active = cm.getActiveNetwork();
            NetworkCapabilities caps = active == null ? null : cm.getNetworkCapabilities(active);
            if (caps != null && !caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) {
                return active;
            }
            NetPick pick = findUnderlying(cm);
            return (pick == null) ? null : pick.network;
        } catch (Throwable t) {
            return null;
        }
    }

    private static class NetPick {
        Network network;
        NetworkCapabilities caps;
    }

    /**
     * 在 VPN 在线时回溯真实底层承载：优先 WiFi（系统默认优先无线局域网），否则取蜂窝。
     */
    private static NetPick findUnderlying(ConnectivityManager cm) {
        NetPick wifi = null;
        NetPick cellular = null;
        NetPick other = null;
        try {
            Network[] all = cm.getAllNetworks();
            if (all == null) return null;
            for (Network n : all) {
                NetworkCapabilities c = cm.getNetworkCapabilities(n);
                if (c == null) continue;
                if (c.hasTransport(NetworkCapabilities.TRANSPORT_VPN)) continue;
                if (!c.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)) continue;
                NetPick pick = new NetPick();
                pick.network = n;
                pick.caps = c;
                if (c.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)) {
                    wifi = pick;
                } else if (c.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)) {
                    cellular = pick;
                } else if (other == null) {
                    other = pick;
                }
            }
        } catch (Throwable t) {
            Log.w(TAG, "回溯底层网络失败: " + t.getMessage());
        }
        if (wifi != null) return wifi;
        if (cellular != null) return cellular;
        return other;
    }

    private static String detectCellularSlot(Context context) {
        int slotIndex = -1;
        int subId = SubscriptionManager.INVALID_SUBSCRIPTION_ID;
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                subId = SubscriptionManager.getDefaultDataSubscriptionId();
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP_MR1 && subId != SubscriptionManager.INVALID_SUBSCRIPTION_ID) {
                SubscriptionManager sm = (SubscriptionManager) context.getSystemService(Context.TELEPHONY_SUBSCRIPTION_SERVICE);
                if (sm != null && hasPhoneStatePermission(context)) {
                    SubscriptionInfo info = sm.getActiveSubscriptionInfo(subId);
                    if (info != null) slotIndex = info.getSimSlotIndex();
                }
            }
        } catch (Throwable t) {
            Log.w(TAG, "读取卡槽信息失败: " + t.getMessage());
        }

        if (slotIndex >= 0) {
            return slotIndex >= 1 ? SLOT_SIM2 : SLOT_SIM1;
        }
        // 无权限兜底：用 subId 奇偶做稳定映射，两张卡的 subId 通常相邻，可稳定落到不同分区
        if (subId != SubscriptionManager.INVALID_SUBSCRIPTION_ID) {
            return (subId % 2 == 0) ? SLOT_SIM2 : SLOT_SIM1;
        }
        return SLOT_SIM1;
    }

    private static boolean hasPhoneStatePermission(Context context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return false;
        try {
            return context.checkSelfPermission(android.Manifest.permission.READ_PHONE_STATE)
                    == PackageManager.PERMISSION_GRANTED;
        } catch (Throwable t) {
            return false;
        }
    }

    /** 分区展示名称，例如「WiFi」「卡槽1 · 中国移动」 */
    public static String slotLabel(Context context, String slot) {
        if (SLOT_WIFI.equals(slot)) return "WiFi";
        String operator = detectOperatorName(context, slot);
        String base = SLOT_SIM2.equals(slot) ? "卡槽2" : "卡槽1";
        return TextUtils.isEmpty(operator) ? base : (base + " · " + operator);
    }

    private static String detectOperatorName(Context context, String slot) {
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP_MR1 && hasPhoneStatePermission(context)) {
                SubscriptionManager sm = (SubscriptionManager) context.getSystemService(Context.TELEPHONY_SUBSCRIPTION_SERVICE);
                if (sm != null) {
                    int wantSlot = SLOT_SIM2.equals(slot) ? 1 : 0;
                    List<SubscriptionInfo> list = sm.getActiveSubscriptionInfoList();
                    if (list != null) {
                        for (SubscriptionInfo info : list) {
                            if (info.getSimSlotIndex() == wantSlot) {
                                CharSequence name = info.getCarrierName();
                                if (name != null) return name.toString();
                            }
                        }
                    }
                }
            }
            if (SLOT_SIM1.equals(slot)) {
                TelephonyManager tm = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
                if (tm != null) return tm.getSimOperatorName();
            }
        } catch (Throwable ignored) {
        }
        return "";
    }

    /* ======================= 目标落地区域 =======================
       只收这三个区域：日本(NRT/KIX/FUK/OKA)、香港(HKG)、新加坡(SIN)。
       WiFi / 卡槽1 / 卡槽2 三个池子用同一套规则，不按运营商做差异化——
       少一层判断就少一类坑，实测效果也没必要细分。 */

    private static final String[] TARGET_AREAS = {"日本", "香港", "新加坡"};

    private static boolean isTargetArea(String area) {
        if (area == null) return false;
        for (String t : TARGET_AREAS) {
            if (t.equals(area)) return true;
        }
        return false;
    }

    public static String slotDisplayName(String slot) {
        if (SLOT_WIFI.equals(slot)) return "WiFi";
        if (SLOT_SIM2.equals(slot)) return "卡槽2";
        return "卡槽1";
    }

    /* ======================= 池读取 / 合并 ======================= */

    public static List<String> parseCsv(String csv) {
        List<String> out = new ArrayList<>();
        if (TextUtils.isEmpty(csv)) return out;
        for (String s : csv.split(",")) {
            String v = s.trim();
            if (v.length() > 0 && !out.contains(v)) out.add(v);
        }
        return out;
    }

    public static String joinCsv(List<String> ips) {
        if (ips == null || ips.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (String ip : ips) {
            if (sb.length() > 0) sb.append(',');
            sb.append(ip);
        }
        return sb.toString();
    }

    /** 某分区当前 IP 数量 */
    public static int poolSize(Preferences prefs, String slot) {
        return parseCsv(prefs.getCfPool(slot)).size();
    }

    /**
     * 合并三分区为优选候选池：当前所在分区优先，其余分区随后，去重。
     * 这样即便刚切网、当前分区还没更新过，也能立刻复用另外两条线路积累的 IP。
     */
    public static List<String> getMergedPool(Context context, Preferences prefs) {
        String current = detectSlot(context);
        LinkedHashSet<String> merged = new LinkedHashSet<>();
        merged.addAll(parseCsv(prefs.getCfPool(current)));
        for (String slot : new String[]{SLOT_WIFI, SLOT_SIM1, SLOT_SIM2}) {
            if (!slot.equals(current)) {
                merged.addAll(parseCsv(prefs.getCfPool(slot)));
            }
        }
        List<String> out = new ArrayList<>(merged);
        Log.i(TAG, "合并候选池: 当前分区=" + current + " 合计 " + out.size() + " 个");
        return out;
    }

    /** 三分区总量（去重后），用于界面统计 */
    public static int mergedSize(Context context, Preferences prefs) {
        return getMergedPool(context, prefs).size();
    }

    /* ======================= 池更新（扫描） ======================= */

    public interface ProgressCallback {
        void onProgress(int done, int total);
    }

    /**
     * 异步更新【当前网络分区】的 IP 池。
     *
     * @param context  上下文
     * @param manual   是否用户手动触发（影响日志与提示文案）
     * @param callback 进度与完成回调（主线程）
     */
    public static void updateCurrentPoolAsync(final Context context, final boolean manual, final PoolCallback callback) {
        if (!scanning.compareAndSet(false, true)) {
            Log.w(TAG, "IP 池扫描已在运行中，忽略重复请求");
            if (callback != null) {
                mainHandler.post(() -> callback.onFinish(false, detectSlot(context), 0, "扫描正在进行中"));
            }
            return;
        }

        final Context appCtx = context.getApplicationContext();
        new Thread(new Runnable() {
            @Override
            public void run() {
                boolean ok = false;
                String slot = SLOT_WIFI;
                int size = 0;
                String message = "";
                try {
                    slot = detectSlot(appCtx);
                    Preferences prefs = new Preferences(appCtx);
                    String slotName = slotDisplayName(slot);

                    List<String> oldPool = parseCsv(prefs.getCfPool(slot));
                    Log.i(TAG, "开始更新 " + slotName + " IP 池（旧池 " + oldPool.size() + " 个）");

                    // 1. 候选只用子网库随机采样，不掺旧池：区域策略已收敛到 日/港/新，
                    //    旧池里可能混着已排除的区域，带进来只会污染池子。
                    List<String> candidates = new ArrayList<>();
                    List<String> sampled = sampleSubnetIps(appCtx, SCAN_MAX_SUBNETS);
                    for (String ip : sampled) {
                        if (!candidates.contains(ip)) candidates.add(ip);
                    }
                    final int total = candidates.size();
                    Log.i(TAG, "候选 " + total + " 个（目标区域：日本/香港/新加坡）");

                    // 2. 并发探测（绑定底层真实网络，避免 VPN 隧道干扰测量）
                    Network bindNet = getUnderlyingNetwork(appCtx);
                    List<IpLatency> alive = probeAll(candidates, total, bindNet, callback);

                    // 3. 区域归类：只留三地，第一轮每个 /24 只取 1 个（保证前缀多样性）
                    List<IpLatency> regional = new ArrayList<>();
                    if (!alive.isEmpty()) {
                        regional = classifyByColo(alive, false, POOL_CAPACITY, callback);
                    }
                    if (regional.isEmpty() && !alive.isEmpty()) {
                        Log.w(TAG, "落地归类无结果，回退按延迟排序的原始候选");
                        regional = new ArrayList<>(alive.subList(0, Math.min(POOL_CAPACITY, alive.size())));
                    }

                    // 4. 深挖补足：三地扫不满 200 时，从最快的几个 /24 段再抽 IP 实测补齐
                    if (!regional.isEmpty() && regional.size() < POOL_CAPACITY) {
                        regional = fillUpByCidr(regional, bindNet, callback);
                    }

                    if (regional.isEmpty()) {
                        message = "未探测到可用 IP，请检查当前网络连通性";
                        Log.w(TAG, message);
                    } else {
                        // 5. 整池换新，不保留任何旧 IP
                        List<String> merged = new ArrayList<>();
                        java.util.Set<String> uniq = new java.util.HashSet<>();
                        for (IpLatency it : regional) {
                            if (merged.size() >= POOL_CAPACITY) break;
                            if (uniq.add(it.ip)) merged.add(it.ip);
                        }
                        String nowStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault()).format(new Date());
                        String dateStr = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault()).format(new Date());

                        prefs.setCfPool(slot, joinCsv(merged));
                        prefs.setCfPoolTime(slot, nowStr);
                        prefs.setCfPoolDate(slot, dateStr);

                        // 6. 顺便重建段库：这一轮问出过 colo 的 /24 段全部纳入。
                        //    之后每次优选只复测这些段的代表 IP，2~5 秒就能出新榜单。
                        int libN = CfCidrLib.rebuildFromCache(appCtx, slot);

                        size = merged.size();
                        ok = true;
                        message = "已更新 " + slotName + "：IP " + size + " 个 · 段库 " + libN + " 段";
                        Log.i(TAG, message + "（整池换新，旧池 " + oldPool.size() + " 个已废弃）");
                    }

                } catch (Throwable t) {
                    message = "IP 池更新异常: " + t.getMessage();
                    Log.e(TAG, message, t);
                } finally {
                    scanning.set(false);
                }

                final boolean fOk = ok;
                final String fSlot = slot;
                final int fSize = size;
                final String fMsg = message;
                if (callback != null) {
                    mainHandler.post(() -> callback.onFinish(fOk, fSlot, fSize, fMsg));
                }
            }
        }, "cf-ip-pool-scanner").start();
    }

    /**
     * 段池自动生成**只发生在首次**：当前分区的段库不存在才跑一次全量扫建库。
     *
     * <p>老豆定的规矩（2026-11-10）：IP 段池第一次生成之后，正常情况下不再自动更新，
     * 只手动更新。理由：段库对优选结果的影响是「有没有」，不是「新不新」——
     * 每轮优选都会重新实测段代表 IP，库本身不需要跟着变。按天自动重建只会带来
     * 无谓的波动（榜单大换血）和一次满速扫描的电量开销。
     *
     * @return true 表示触发了建库扫描
     */
    public static boolean maybeInitialUpdate(final Context context) {
        return maybeInitialUpdate(context, null);
    }

    public static boolean maybeInitialUpdate(final Context context, final PoolCallback callback) {
        try {
            final Context appCtx = context.getApplicationContext();
            final String slot = detectSlot(appCtx);
            if (CfCidrLib.hasLib(appCtx, slot)) {
                Log.i(TAG, slotDisplayName(slot) + " 段池已存在，跳过自动建库（之后只手动更新）");
                return false;
            }
            Log.i(TAG, slotDisplayName(slot) + " 未检测到段池（首次运行），后台自动生成一次...");
            updateCurrentPoolAsync(appCtx, false, callback);
            return true;
        } catch (Throwable t) {
            Log.w(TAG, "首次建库检查失败: " + t.getMessage());
            return false;
        }
    }

    public static boolean isScanning() {
        return scanning.get();
    }

    /**
     * 深挖补足：池子不足 POOL_CAPACITY 时，从延迟最优的若干个 /24 段里
     * 再随机抽 IP 实测，落地在三地的才并入池子。
     *
     * 依据：排名靠前的 /24 段整体质量都不错（实测 172.64.53.x 全部落地 NRT），
     * 只是在"每 /24 只留 1 个"的规则下被浪费了。
     * 注意不是盲抽 —— 每个都实测落地 POP，同段的 IP 也可能落到别的区域。
     */
    private static List<IpLatency> fillUpByCidr(List<IpLatency> pool, Network bindNet,
                                               final PoolCallback callback) {
        long t0 = System.currentTimeMillis();
        List<String> cidrs = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (IpLatency it : pool) {
            if (seen.add(cidr24(it.ip))) {
                cidrs.add(cidr24(it.ip));
                if (cidrs.size() >= FILL_CIDR_COUNT) break;
            }
        }
        if (cidrs.isEmpty()) return pool;

        java.util.Set<String> existed = new java.util.HashSet<>();
        for (IpLatency it : pool) existed.add(it.ip);

        Random rnd = new Random();
        List<String> deep = new ArrayList<>();
        for (String c : cidrs) {
            for (int k = 0; k < FILL_PER_CIDR; k++) {
                String ip = c + "." + (rnd.nextInt(254) + 1);
                if (!existed.add(ip)) continue;
                deep.add(ip);
            }
        }
        if (deep.isEmpty()) return pool;

        Log.i(TAG, "深挖补足：池子 " + pool.size() + " 个不足 " + POOL_CAPACITY
                + "，从最快 " + cidrs.size() + " 个 /24 段再探 " + deep.size() + " 个 IP");

        List<IpLatency> probed = probeAll(deep, deep.size(), bindNet, callback);
        if (probed.isEmpty()) return pool;

        // 归类（允许同段多个，否则补不进来）
        List<IpLatency> extra = classifyByColo(probed, true, POOL_CAPACITY, callback);
        if (extra.isEmpty()) {
            Log.w(TAG, "深挖补足：没有新的合格 IP");
            return pool;
        }

        List<IpLatency> all = new ArrayList<>(pool);
        all.addAll(extra);
        Collections.sort(all, new Comparator<IpLatency>() {
            @Override public int compare(IpLatency x, IpLatency y) {
                return Float.compare(x.latency, y.latency);
            }
        });

        List<IpLatency> out = new ArrayList<>();
        java.util.Set<String> used = new java.util.HashSet<>();
        for (IpLatency it : all) {
            if (out.size() >= POOL_CAPACITY) break;
            if (used.add(it.ip)) out.add(it);
        }
        Log.i(TAG, "深挖补足完成：" + pool.size() + " -> " + out.size()
                + " 个（新增 " + extra.size() + " 候选），耗时 "
                + (System.currentTimeMillis() - t0) + "ms");
        return out;
    }

    /* ======================= 落地 POP 归类（区域感知优选） =======================
     * 用「TLS + /cdn-cgi/trace」问出候选 IP 从本机网络真实落地的 Cloudflare POP，
     * 只保留 港/日/新/台/韩/美西（其余区域对国内就是绕路），每 /24 只留最快的一个，
     * 最后按区域轮转交错 —— "取前 N 个"就天然带上区域多样性。
     * 注：SNI 由 createSocket(socket, host, ...) 提供，兼容 minSdk 21。 */
    static final String PROBE_HOST = "jp2-xt.meetmeag.com";
    static final int COLO_TIMEOUT_MS = 2000;
    private static final int COLO_MAX_CHECK = 320;

    static final java.util.Map<String, String> COLO_AREA = new java.util.HashMap<String, String>();
    static {
        COLO_AREA.put("HKG", "香港");
        String[] jp = {"NRT", "KIX", "FUK", "OKA"};
        for (String c : jp) COLO_AREA.put(c, "日本");
        COLO_AREA.put("SIN", "新加坡");
        COLO_AREA.put("TPE", "中国台湾");
        String[] kr = {"ICN", "PUS"};
        for (String c : kr) COLO_AREA.put(c, "韩国");
        String[] us = {"LAX", "SJC", "SEA", "PDX", "SFO"};
        for (String c : us) COLO_AREA.put(c, "美西");
    }

    /**
     * 落地 POP 内存缓存：IP -> colo（NRT / SIN / HKG ...）。
     * ★ 仅供界面展示查表用。任何情况下都不得把 colo 拼进连接参数——
     *   Windows 端曾把 "(NRT)" 拼进节点 IP，内核当域名解析，导致 25 条通道全挂。
     */
    static final java.util.Map<String, String> COLO_CACHE =
            new java.util.concurrent.ConcurrentHashMap<String, String>();

    /** 查落地 POP：先查缓存，未命中现探一次并缓存（失败返回 null） */
    static String coloOf(String ip) {
        if (ip == null || ip.isEmpty()) return null;
        String c = COLO_CACHE.get(ip);
        if (c != null) return c;
        c = probeColo(ip);
        if (c != null) COLO_CACHE.put(ip, c);
        return c;
    }

    /**
     * 批量查落地 POP（并发，4 路），返回与入参等长的数组，探不到的位置为 null。
     * N 通常只有 3~5 个，最坏耗时约一个超时周期。
     */
    static String[] colosOf(java.util.List<String> ips) {
        if (ips == null || ips.isEmpty()) return new String[0];
        final String[] out = new String[ips.size()];
        final CountDownLatch latch = new CountDownLatch(ips.size());
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(4, ips.size()));
        for (int i = 0; i < ips.size(); i++) {
            final int idx = i;
            final String ip = ips.get(i);
            pool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        out[idx] = coloOf(ip);
                    } catch (Throwable ignored) {
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await(6, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        pool.shutdownNow();
        return out;
    }

    /** 探测单个 IP 的落地 POP（失败返回 null） */
    /**
     * 段池优选专用：一次 TLS + /cdn-cgi/trace 同时拿到【落地区域】和【握手耗时】，
     * 并绑定底层网络 —— VPN 在线时不绑就会测到隧道质量，整批结果全废。
     */
    static final class ColoProbe {
        final String colo;
        final float latencyMs;
        ColoProbe(String colo, float latencyMs) {
            this.colo = colo;
            this.latencyMs = latencyMs;
        }
    }

    static ColoProbe probeColoEx(String ip, Network bindNet) {
        java.net.Socket raw = null;
        javax.net.ssl.SSLSocket s = null;
        try {
            raw = (bindNet != null) ? bindNet.getSocketFactory().createSocket() : new java.net.Socket();
            long t0 = System.nanoTime();
            raw.connect(new java.net.InetSocketAddress(ip, PROBE_PORT), COLO_TIMEOUT_MS);
            javax.net.ssl.SSLSocketFactory f =
                    (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory.getDefault();
            s = (javax.net.ssl.SSLSocket) f.createSocket(raw, PROBE_HOST, 443, true);
            s.setSoTimeout(COLO_TIMEOUT_MS);
            s.startHandshake();
            float lat = (System.nanoTime() - t0) / 1000000f;
            java.io.OutputStream os = s.getOutputStream();
            String req = "GET /cdn-cgi/trace HTTP/1.1\r\nHost: " + PROBE_HOST
                    + "\r\nUser-Agent: cfscan\r\nAccept: */*\r\nConnection: close\r\n\r\n";
            os.write(req.getBytes("US-ASCII"));
            os.flush();
            s.setSoTimeout(1200);
            java.io.InputStream is = s.getInputStream();
            byte[] buf = new byte[512];
            StringBuilder sb = new StringBuilder();
            int n;
            while (sb.length() < 4096 && (n = is.read(buf)) > 0) {
                sb.append(new String(buf, 0, n, "US-ASCII"));
            }
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("(?m)^colo=(\\S+)").matcher(sb);
            if (!m.find()) return null;
            String colo = m.group(1);
            COLO_CACHE.put(ip, colo);
            return new ColoProbe(colo, lat);
        } catch (Throwable t) {
            return null;
        } finally {
            try { if (s != null) s.close(); } catch (Throwable ignored) { }
            try { if (raw != null) raw.close(); } catch (Throwable ignored) { }
        }
    }

    static String probeColo(String ip) {
        javax.net.ssl.SSLSocket s = null;
        java.net.Socket raw = null;
        try {
            raw = new java.net.Socket();
            raw.connect(new java.net.InetSocketAddress(ip, 443), COLO_TIMEOUT_MS);
            javax.net.ssl.SSLSocketFactory f =
                    (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory.getDefault();
            s = (javax.net.ssl.SSLSocket) f.createSocket(raw, PROBE_HOST, 443, true);
            s.setSoTimeout(COLO_TIMEOUT_MS);
            s.startHandshake();
            java.io.OutputStream os = s.getOutputStream();
            String req = "GET /cdn-cgi/trace HTTP/1.1\r\nHost: " + PROBE_HOST
                    + "\r\nUser-Agent: cfscan\r\nAccept: */*\r\nConnection: close\r\n\r\n";
            os.write(req.getBytes("US-ASCII"));
            os.flush();
            s.setSoTimeout(1200);
            java.io.InputStream is = s.getInputStream();
            byte[] buf = new byte[512];
            StringBuilder sb = new StringBuilder();
            int n;
            while (sb.length() < 4096 && (n = is.read(buf)) > 0) {
                sb.append(new String(buf, 0, n, "US-ASCII"));
            }
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("(?m)^colo=(\\S+)").matcher(sb);
            return m.find() ? m.group(1) : null;
        } catch (Throwable t) {
            return null;
        } finally {
            try { if (s != null) s.close(); } catch (Throwable ignored) { }
            try { if (raw != null) raw.close(); } catch (Throwable ignored) { }
        }
    }

    private static String cidr24(String ip) {
        int i = ip.lastIndexOf('.');
        return i > 0 ? ip.substring(0, i) : ip;
    }

    /**
     * 对延迟最优的一批候选做落地归类。
     * 只收 日本/香港/新加坡 三地；全部合并后按延迟升序返回，不追求区域均衡——
     * 本机线路优出来的本就集中在一两个区域，强行轮转只会把慢的塞进池子。
     *
     * @param multiPerCidr 是否允许同一个 /24 段收多个 IP。
     *                     第一轮 false（保证前缀多样性），深挖补足轮 true（否则补不进来）。
     * @param limit        返回数量上限
     */
    static List<IpLatency> classifyByColo(List<IpLatency> alive, final boolean multiPerCidr,
                                          final int limit, final PoolCallback callback) {
        int check = Math.min(COLO_MAX_CHECK, alive.size());
        if (check <= 0) return new ArrayList<IpLatency>();
        final List<IpLatency> head = new ArrayList<IpLatency>(alive.subList(0, check));
        final java.util.Map<String, List<IpLatency>> byArea =
                new java.util.concurrent.ConcurrentHashMap<String, List<IpLatency>>();
        final java.util.Set<String> seenCidr =
                java.util.Collections.synchronizedSet(new java.util.HashSet<String>());
        ExecutorService pool = Executors.newFixedThreadPool(24);
        final CountDownLatch latch = new CountDownLatch(head.size());
        final java.util.concurrent.atomic.AtomicInteger done =
                new java.util.concurrent.atomic.AtomicInteger(0);
        long t0 = System.currentTimeMillis();
        for (final IpLatency it : head) {
            pool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        String colo = probeColo(it.ip);
                        if (colo != null) COLO_CACHE.put(it.ip, colo);   /* 顺手缓存，界面展示复用 */
                        String area = (colo == null) ? null : COLO_AREA.get(colo);
                        if (area == null || !isTargetArea(area)) return;
                        if (!multiPerCidr && !seenCidr.add(cidr24(it.ip))) return;
                        List<IpLatency> l = byArea.get(area);
                        if (l == null) {
                            l = java.util.Collections.synchronizedList(new ArrayList<IpLatency>());
                            byArea.put(area, l);
                        }
                        l.add(it);
                    } catch (Throwable ignored) {
                    } finally {
                        int d = done.incrementAndGet();
                        if (callback != null && (d % 20 == 0 || d == head.size())) {
                            final int fd = d;
                            mainHandler.post(new Runnable() {
                                @Override public void run() {
                                    callback.onProgress(fd, head.size(), "正在按落地区域归类...");
                                }
                            });
                        }
                        latch.countDown();
                    }
                }
            });
        }
        try { latch.await(120, TimeUnit.SECONDS); } catch (Throwable ignored) { }
        pool.shutdownNow();

        // 合并三地结果，按延迟升序取前 limit 个
        List<IpLatency> all = new ArrayList<IpLatency>();
        for (java.util.Map.Entry<String, List<IpLatency>> e : byArea.entrySet()) {
            List<IpLatency> l = e.getValue();
            synchronized (l) { all.addAll(l); }
        }
        Collections.sort(all, new Comparator<IpLatency>() {
            @Override public int compare(IpLatency x, IpLatency y) {
                return Float.compare(x.latency, y.latency);
            }
        });
        if (all.size() > limit) all = new ArrayList<IpLatency>(all.subList(0, limit));

        StringBuilder sb = new StringBuilder();
        for (String a : TARGET_AREAS) {
            List<IpLatency> l = byArea.get(a);
            int n = 0;
            if (l != null) { synchronized (l) { n = l.size(); } }
            sb.append(a).append(':').append(n).append(' ');
        }
        Log.i(TAG, "落地归类 " + check + " 个候选 -> 保留 " + all.size() + " 个 ["
                + sb.toString().trim() + "] 耗时 " + (System.currentTimeMillis() - t0) + "ms");
        return all;
    }

    /* ======================= 探测实现 ======================= */

    static class IpLatency {
        final String ip;
        final float latency;

        IpLatency(String ip, float latency) {
            this.ip = ip;
            this.latency = latency;
        }
    }

    private static List<IpLatency> probeAll(List<String> candidates, final int total,
                                            final Network bindNet, final PoolCallback callback) {
        final List<IpLatency> alive = Collections.synchronizedList(new ArrayList<IpLatency>());
        final int threads = Math.min(SCAN_CONCURRENCY, Math.max(1, candidates.size()));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch latch = new CountDownLatch(candidates.size());
        final java.util.concurrent.atomic.AtomicInteger done = new java.util.concurrent.atomic.AtomicInteger(0);
        final long[] lastNotify = new long[]{0L};

        try {
            for (final String ip : candidates) {
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            float lat = CfOptimizer.probeTcpHandshake(ip, PROBE_PORT, SCAN_TIMEOUT_MS, bindNet);
                            if (lat > 0.0f) {
                                alive.add(new IpLatency(ip, lat));
                            }
                        } finally {
                            int d = done.incrementAndGet();
                            long now = System.currentTimeMillis();
                            if (callback != null && (now - lastNotify[0] > 300 || d == total)) {
                                lastNotify[0] = now;
                                final int fd = d;
                                mainHandler.post(() -> callback.onProgress(fd, total, "正在探测可用 IP..."));
                            }
                            latch.countDown();
                        }
                    }
                });
            }
            // 全量探测上限：最坏情况每轮 RTT 超时 × 轮数
            long waitMs = (long) Math.ceil((double) candidates.size() / threads) * (SCAN_TIMEOUT_MS + 120L) + 3000L;
            latch.await(Math.min(waitMs, 180000L), TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            Log.w(TAG, "探测中断: " + t.getMessage());
        } finally {
            pool.shutdownNow();
        }

        Collections.sort(alive, new Comparator<IpLatency>() {
            @Override
            public int compare(IpLatency a, IpLatency b) {
                return Float.compare(a.latency, b.latency);
            }
        });
        Log.i(TAG, "探测结束：候选 " + candidates.size() + " 个，可用 " + alive.size() + " 个");
        return alive;
    }

    /**
     * 从内置 Cloudflare 全网段清单中随机采样，每个 /24 段生成 1 个随机主机 IP。
     * 采样而非全扫，是为了让单次扫描在手机上 10~20 秒内完成；配合"每天更新 + 旧池保留"，
     * 覆盖面会随天数自然扩散到全量网段。
     */
    private static List<String> sampleSubnetIps(Context context, int maxCount) {
        List<String> subnets = loadSubnets(context);
        if (subnets.isEmpty()) {
            Log.w(TAG, "子网清单为空，退回内置种子 IP");
            return new ArrayList<>();
        }
        Collections.shuffle(subnets, new Random(System.currentTimeMillis()));
        int take = Math.min(maxCount, subnets.size());
        Random rnd = new Random(System.currentTimeMillis() ^ 0x5DEECE66DL);
        List<String> ips = new ArrayList<>(take);
        for (int i = 0; i < take; i++) {
            String ip = randomIpInSubnet(subnets.get(i), rnd);
            if (ip != null) ips.add(ip);
        }
        return ips;
    }

    private static String randomIpInSubnet(String cidr, Random rnd) {
        try {
            int slash = cidr.indexOf('/');
            String base = slash > 0 ? cidr.substring(0, slash) : cidr;
            String[] parts = base.split("\\.");
            if (parts.length != 4) return null;
            int a = Integer.parseInt(parts[0]);
            int b = Integer.parseInt(parts[1]);
            int c = Integer.parseInt(parts[2]);
            // /24 段：末位取 1~254 随机（避开网络号与广播地址）
            int d = 1 + rnd.nextInt(254);
            return a + "." + b + "." + c + "." + d;
        } catch (Throwable t) {
            return null;
        }
    }

    private static List<String> loadSubnets(Context context) {
        List<String> out = new ArrayList<>();
        InputStream is = null;
        BufferedReader br = null;
        try {
            is = context.getAssets().open(ASSET_SUBNETS);
            br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                String v = line.trim();
                if (v.isEmpty() || v.startsWith("#")) continue;
                if (v.contains("/")) out.add(v);
            }
        } catch (Throwable t) {
            Log.w(TAG, "读取子网清单失败: " + t.getMessage());
        } finally {
            try { if (br != null) br.close(); } catch (Throwable ignored) {}
            try { if (is != null) is.close(); } catch (Throwable ignored) {}
        }
        Log.i(TAG, "加载 Cloudflare 子网清单: " + out.size() + " 条");
        return out;
    }
}
