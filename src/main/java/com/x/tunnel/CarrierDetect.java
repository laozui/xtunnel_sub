package com.x.tunnel;

import android.content.Context;
import android.net.Network;
import android.os.Build;
import android.telephony.SubscriptionInfo;
import android.telephony.SubscriptionManager;
import android.telephony.TelephonyManager;
import android.text.TextUtils;
import android.util.Log;

import org.json.JSONObject;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 线路运营商识别 —— 决定段池去哪个区域取 IP。
 *
 * 策略（老豆 2026-11-10 定）：
 *   电信 → 日本 NRT
 *   移动 → 香港 HKG
 *   联通 → 日本 NRT + 香港 HKG
 *   其他 / 未知 → 三地混合（兜底，绝不因为认不出就把功能关掉）
 *
 * 为什么按运营商分区：
 *   Cloudflare 的 anycast IP 没有区域属性，落到哪个 POP 完全由你的 BGP 路由决定。
 *   电信走 163/CN2 到日本最好，移动走 CMI 到香港最好 —— 这是网络工程常识，
 *   不是玄学。先认线路、再定区域，比三地混选精准。
 *
 * 判据分两路：
 *   · 蜂窝（卡槽1/2）→ TelephonyManager / SubscriptionManager 的 MCC+MNC，不用联网，最准
 *   · WiFi → 查出口 IP 的 AS 号（AS4134 电信 / AS4837 联通 / AS9808 移动）
 *     WiFi 场景必须绑定底层网络，否则查到的可能是隧道/代理出口的归属。
 */
public class CarrierDetect {
    private static final String TAG = "CarrierDetect";

    public static final String TELECOM = "中国电信";
    public static final String UNICOM = "中国联通";
    public static final String MOBILE = "中国移动";
    public static final String WASU = "中国华数";
    public static final String OTHER = "其他";

    private static String sCacheKey = "";
    private static String sCachedCarrier = "";
    private static long sCachedAt = 0L;

    /** 识别当前分区对应的线路运营商 */
    public static String detect(Context ctx, String slot) {
        if (ctx == null) return OTHER;
        /* 缓存 key 用 slot 本身：换网/换卡时 detectSlot 会变，缓存自然失效 */
        String key = slot;
        long now = System.currentTimeMillis();
        if (key.equals(sCacheKey) && !TextUtils.isEmpty(sCachedCarrier)
                && now - sCachedAt < 30 * 60 * 1000L) {
            return sCachedCarrier;
        }

        String carrier;
        if (CfIpPool.SLOT_WIFI.equals(slot)) {
            carrier = byEgress(ctx);
            if (TextUtils.isEmpty(carrier)) carrier = OTHER;
        } else {
            carrier = byTelephony(ctx, slot);
            if (TextUtils.isEmpty(carrier)) carrier = OTHER;
        }

        sCacheKey = key;
        sCachedCarrier = carrier;
        sCachedAt = now;
        Log.i(TAG, "线路运营商: " + carrier + "（" + slot + "）");
        return carrier;
    }

    /* ---------------- 蜂窝：MCC + MNC ---------------- */

    private static String byTelephony(Context ctx, String slot) {
        try {
            SubscriptionManager sm = (SubscriptionManager)
                    ctx.getSystemService(Context.TELEPHONY_SUBSCRIPTION_SERVICE);
            int wantSlot = CfIpPool.SLOT_SIM2.equals(slot) ? 1 : 0;
            if (sm != null) {
                List<SubscriptionInfo> list = sm.getActiveSubscriptionInfoList();
                if (list != null) {
                    for (SubscriptionInfo info : list) {
                        if (info.getSimSlotIndex() != wantSlot) continue;
                        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP_MR1) {
                            int mcc = info.getMcc();
                            int mnc = info.getMnc();
                            String c = byMccMnc(mcc, mnc);
                            if (!OTHER.equals(c)) return c;
                        }
                    }
                }
            }
        } catch (Throwable ignored) {
        }
        // 兜底：当前驻留网络的 operator（多卡时可能不准，但总比没有好）
        try {
            TelephonyManager tm = (TelephonyManager) ctx.getSystemService(Context.TELEPHONY_SERVICE);
            if (tm != null) {
                String op = tm.getNetworkOperator();   // 例 "46003"
                if (!TextUtils.isEmpty(op) && op.length() >= 5) {
                    int mcc = Integer.parseInt(op.substring(0, 3));
                    int mnc = Integer.parseInt(op.substring(3));
                    String c = byMccMnc(mcc, mnc);
                    if (!OTHER.equals(c)) return c;
                }
            }
        } catch (Throwable ignored) {
        }
        return OTHER;
    }

    /** 中国 MCC 固定 460。MNC 分配见工信部号段。 */
    private static String byMccMnc(int mcc, int mnc) {
        if (mcc != 460) return OTHER;
        switch (mnc) {
            case 0: case 2: case 4: case 7: case 8: return MOBILE;
            case 1: case 6: case 9: case 10:        return UNICOM;
            case 3: case 5: case 11:                return TELECOM;
            case 15:                                return "中国广电";
            case 20:                                return MOBILE;
            default:                                return OTHER;
        }
    }

    /* ---------------- WiFi：查出口 IP 的 AS 号 ---------------- */

    private static String byEgress(Context ctx) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return "";
        Network net = CfIpPool.getUnderlyingNetwork(ctx);
        HttpURLConnection conn = null;
        try {
            URL u = new URL("http://ip-api.com/json/?lang=zh-CN&fields=status,query,isp,as");
            if (net != null) {
                conn = (HttpURLConnection) net.openConnection(u);
            } else {
                conn = (HttpURLConnection) u.openConnection();
            }
            conn.setConnectTimeout(6000);
            conn.setReadTimeout(6000);
            conn.setRequestProperty("User-Agent", "curl/8.5.0");
            if (conn.getResponseCode() != 200) return "";
            InputStream in = conn.getInputStream();
            byte[] buf = new byte[2048];
            int n = in.read(buf);
            in.close();
            if (n <= 0) return "";
            JSONObject o = new JSONObject(new String(buf, 0, n, "UTF-8"));
            if (!"success".equals(o.optString("status"))) return "";
            return byAsn(o.optString("as", "") + " " + o.optString("isp", ""));
        } catch (Throwable t) {
            Log.w(TAG, "出口 AS 查询失败: " + t.getMessage());
            return "";
        } finally {
            try { if (conn != null) conn.disconnect(); } catch (Throwable ignored) { }
        }
    }

    /** AS 号是纯 ASCII，跨编码绝对安全 —— 中文名只作兜底 */
    public static String byAsn(String as) {
        String u = as == null ? "" : as.toUpperCase();
        if (u.contains("CHINANET") || u.contains("CHINATELECOM")
                || u.contains("AS4134") || u.contains("AS4809") || u.contains("AS4812")) {
            return TELECOM;
        }
        if (u.contains("UNICOM") || u.contains("CHINA169")
                || u.contains("AS4837") || u.contains("AS9929") || u.contains("AS17816")) {
            return UNICOM;
        }
        if (u.contains("CMNET") || u.contains("CHINAMOBILE") || u.contains("CMI")
                || u.contains("AS9808") || u.contains("AS56040") || u.contains("AS24400")) {
            return MOBILE;
        }
        if (u.contains("AS58461") || u.contains("HZEN") || u.contains("华数")) {
            return WASU;
        }
        return OTHER;
    }

    /* ---------------- 运营商 → 区域 ---------------- */

    /** 主区域（POP 白名单）。联通按老豆 2026-11-11 定稿的三区规则，与 Windows 端对齐。 */
    public static String regionsFor(String carrier) {
        if (TELECOM.equals(carrier)) return "NRT";
        if (MOBILE.equals(carrier)) return "HKG";
        if (UNICOM.equals(carrier)) return "NRT,HKG,SIN";
        return "NRT,HKG,SIN";
    }

    /** 主区域产能不足时的降级区域。联通主区已含 SIN，无需再降级。 */
    public static String fallbackFor(String carrier) {
        if (TELECOM.equals(carrier)) return "HKG,SIN";
        if (MOBILE.equals(carrier)) return "NRT,SIN";
        if (UNICOM.equals(carrier)) return "";
        return "";
    }

    /** 区域串 → 中文区域名集合（段库里 area 存的是中文） */
    public static Set<String> areaSet(String regionsCsv) {
        Set<String> out = new HashSet<>();
        if (TextUtils.isEmpty(regionsCsv)) return out;
        for (String r : regionsCsv.split(",")) {
            String pop = r.trim().toUpperCase();
            if (pop.length() == 0) continue;
            String area = CfIpPool.COLO_AREA.get(pop);
            if (area != null) out.add(area);
        }
        return out;
    }

    /** 亚太区域（段库分级用：上次落这些地方的段值得复测） */
    public static boolean isAsiaPac(String area) {
        if (area == null) return false;
        return "日本".equals(area) || "香港".equals(area) || "新加坡".equals(area)
                || "中国台湾".equals(area) || "韩国".equals(area);
    }

    /** 界面展示用：运营商 + 区域的一句话描述 */
    public static String summary(String carrier) {
        if (TextUtils.isEmpty(carrier)) return "未识别";
        String r = regionsFor(carrier);
        if ("NRT".equals(r)) return carrier + " → 日本 NRT";
        if ("HKG".equals(r)) return carrier + " → 香港 HKG";
        return carrier + " → 三地混合";
    }
}
