package com.x.tunnel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import android.util.Base64;
import android.util.Log;

public class SubscriptionManager {
    private static final String TAG = "SubscriptionManager";

    public static class SubNode {
        public String name;
        public String server; // WssAddr
        public String token;  // Token
        public String ip;     // PrefIp
        public boolean fallback = true; // fallback=1 对应 disableEch (停用ECH走标准TLS)
        public boolean insecure = false; // insecure=1 对应跳过证书验证
        public int connections = 3;
        public String block = "443";
    }

    public static class ApplyResult {
        public final int count;
        public final boolean currentProfileChanged;
        public final String previousProfileId;
        public final String currentProfileId;
        public final String currentProfileName;

        public ApplyResult(int count, boolean currentProfileChanged, String previousProfileId, String currentProfileId, String currentProfileName) {
            this.count = count;
            this.currentProfileChanged = currentProfileChanged;
            this.previousProfileId = previousProfileId;
            this.currentProfileId = currentProfileId;
            this.currentProfileName = currentProfileName;
        }
    }

    public interface Callback {
        void onSuccess(ApplyResult result);
        void onError(String message);
    }

    public static void fetchAndUpdate(Preferences prefs, String subUrl, Callback callback) {
        new Thread(() -> {
            try {
                if (subUrl == null || subUrl.trim().isEmpty()) {
                    if (callback != null) callback.onError("订阅地址为空");
                    return;
                }

                String content = downloadUrl(subUrl.trim());
                if (content == null || content.trim().isEmpty()) {
                    if (callback != null) callback.onError("获取订阅内容为空");
                    return;
                }

                // 尝试检测是否为纯 Base64 编码，是则先解码
                content = tryBase64Decode(content.trim());

                List<SubNode> nodes = parseIniNodes(content);
                if (nodes.isEmpty()) {
                    if (callback != null) callback.onError("未解析到有效节点配置");
                    return;
                }

                ApplyResult result = applySubNodes(prefs, nodes);
                prefs.setSubLastSyncTime(System.currentTimeMillis());

                if (callback != null) {
                    callback.onSuccess(result);
                }
            } catch (Exception e) {
                Log.e(TAG, "Sync subscription failed", e);
                if (callback != null) {
                    callback.onError("同步失败: " + e.getMessage());
                }
            }
        }).start();
    }

    private static String downloadUrl(String urlStr) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(15000);
        conn.setRequestProperty("User-Agent", "X-Tunnel-Sub/1.0");

        int respCode = conn.getResponseCode();
        if (respCode >= 300 && respCode < 400) {
            String redirectUrl = conn.getHeaderField("Location");
            if (redirectUrl != null) {
                return downloadUrl(redirectUrl);
            }
        }

        if (respCode != 200) {
            throw new Exception("HTTP " + respCode);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }

    private static String tryBase64Decode(String text) {
        if (text.startsWith("[") || text.contains("[Server") || text.contains("server=")) {
            return text;
        }
        try {
            byte[] decoded = Base64.decode(text, Base64.DEFAULT);
            String str = new String(decoded, StandardCharsets.UTF_8);
            if (str.contains("[") || str.contains("server=")) {
                return str;
            }
        } catch (Throwable ignored) {}
        return text;
    }

    public static List<SubNode> parseIniNodes(String content) {
        List<SubNode> list = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new StringReader(content))) {
            String line;
            String currentSection = null;
            Map<String, Map<String, String>> sections = new LinkedHashMap<>();

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
                    continue;
                }
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSection = line.substring(1, line.length() - 1).trim();
                    if (!sections.containsKey(currentSection)) {
                        sections.put(currentSection, new LinkedHashMap<>());
                    }
                    continue;
                }

                if (currentSection != null) {
                    int eq = line.indexOf('=');
                    if (eq > 0) {
                        String key = line.substring(0, eq).trim().toLowerCase();
                        String val = line.substring(eq + 1).trim();
                        sections.get(currentSection).put(key, val);
                    }
                }
            }

            // 过滤提取 ServerX 节
            int index = 1;
            for (Map.Entry<String, Map<String, String>> entry : sections.entrySet()) {
                String secName = entry.getKey();
                if (secName.equalsIgnoreCase("Settings")) {
                    continue;
                }
                Map<String, String> kv = entry.getValue();
                String server = kv.get("server");
                if (server != null && !server.isEmpty()) {
                    SubNode node = new SubNode();
                    node.server = server;
                    String tokenVal = kv.get("token");
                    node.token = tokenVal != null ? tokenVal : "";
                    String ipVal = kv.get("ip");
                    node.ip = ipVal != null ? ipVal : "";

                    String fbStr = kv.get("fallback");
                    if (fbStr != null) {
                        node.fallback = "1".equals(fbStr.trim()) || "true".equalsIgnoreCase(fbStr.trim());
                    }
                    String insStr = kv.get("insecure");
                    if (insStr != null) {
                        node.insecure = "1".equals(insStr.trim()) || "true".equalsIgnoreCase(insStr.trim());
                    }
                    String connStr = kv.get("connections");
                    if (connStr != null) {
                        try {
                            node.connections = Integer.parseInt(connStr.trim());
                        } catch (Exception ignored) {}
                    }
                    String blockStr = kv.get("block");
                    if (blockStr != null && !blockStr.trim().isEmpty()) {
                        node.block = blockStr.trim();
                    }

                    String name = kv.get("name");
                    if (name == null || name.isEmpty()) {
                        name = "节点 " + index;
                    }
                    node.name = name;
                    list.add(node);
                    index++;
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "parse INI error", e);
        }
        return list;
    }

    private static synchronized ApplyResult applySubNodes(Preferences prefs, List<SubNode> newNodes) {
        // 1. 获取所有旧的 profile
        Set<String> oldIds = prefs.getProfileIds();
        String currentIdBefore = prefs.getCurrentProfileId();

        // 建立旧订阅节点的映射：server -> oldId
        Map<String, String> oldSubServerToId = new LinkedHashMap<>();
        for (String id : oldIds) {
            if (prefs.isSubProfile(id)) {
                String server = prefs.getWssAddr(id).trim();
                if (!server.isEmpty() && !oldSubServerToId.containsKey(server)) {
                    oldSubServerToId.put(server, id);
                }
            }
        }

        // 2. 依次映射或生成新节点的 ID，保持已有节点的 ID 稳定
        Set<String> retainedOrNewSubIds = new java.util.LinkedHashSet<>();
        List<String> newlyAddedSubIds = new ArrayList<>();
        String firstSubId = null;

        for (SubNode node : newNodes) {
            String server = node.server.trim();
            String id = oldSubServerToId.get(server);
            if (id == null) {
                id = "sub_" + UUID.randomUUID().toString().substring(0, 8);
                newlyAddedSubIds.add(id);
            }
            retainedOrNewSubIds.add(id);
            if (firstSubId == null) {
                firstSubId = id;
            }

            // 更新或添加该 profile 配置
            prefs.addProfile(id, node.name);
            prefs.setProfileIsSub(id, true);
            prefs.setWssAddr(id, node.server);
            prefs.setToken(id, node.token);
            prefs.setPrefIp(id, node.ip);

            // 属性根据订阅配置自动注入
            prefs.setWsConn(id, prefs.clampWsConn(node.connections));
            prefs.setUdpBlockPorts(id, node.block);
            prefs.setEchDns(id, "https://doh.pub/dns-query");
            prefs.setEchDomain(id, "cloudflare-ech.com");
            // fallback=1 对应停用 ECH (走标准 TLS)，避免 Cloudflare Tunnel 节点因 ECH 握手失败
            prefs.setDisableEch(id, node.fallback);
            prefs.setInsecure(id, node.insecure);
        }

        // 3. 清理本次订阅中已不存在的旧订阅节点
        for (String id : oldIds) {
            if (prefs.isSubProfile(id) && !retainedOrNewSubIds.contains(id)) {
                prefs.removeProfile(id);
            }
        }

        // 4. 维护持久化的自定义排序列表 (ProfileOrder)
        // 规则：保留现有排序中仍存在的节点；新加入的节点追加到排序尾部；已删除的节点自动剔除
        List<String> oldOrder = prefs.getProfileOrder();
        List<String> newOrder = new ArrayList<>();
        Set<String> allValidIds = prefs.getProfileIds();

        // 4.1 遍历原有的排序：只要还在 allValidIds 中，就保留用户自定义的位置
        for (String id : oldOrder) {
            if (allValidIds.contains(id) && !newOrder.contains(id)) {
                newOrder.add(id);
            }
        }
        // 4.2 对于没有在 newOrder 中的节点（新订阅添加的节点），按订阅原序追加到末尾
        for (String id : retainedOrNewSubIds) {
            if (!newOrder.contains(id)) {
                newOrder.add(id);
            }
        }
        // 4.3 兜底添加任何可能遗漏的节点
        for (String id : allValidIds) {
            if (!newOrder.contains(id)) {
                newOrder.add(id);
            }
        }
        prefs.setProfileOrder(newOrder);

        // 5. 处理当前选中的节点 ID
        // 需求：若当前的这个节点还在，默认更新之后继续使用当前节点。若当前节点更新后不存在了，用订阅的第一个节点。
        String newCurrentId = currentIdBefore;
        boolean changed = false;

        boolean currentStillExists = allValidIds.contains(currentIdBefore) && !prefs.getWssAddr(currentIdBefore).trim().isEmpty();
        if (currentStillExists) {
            // 当前节点依然存在且有效，继续使用当前节点
            newCurrentId = currentIdBefore;
            changed = false;
        } else {
            // 当前节点更新后不存在了，自动切换到订阅的第一个节点
            if (firstSubId != null) {
                newCurrentId = firstSubId;
            } else if (!newOrder.isEmpty()) {
                newCurrentId = newOrder.get(0);
            } else if (!allValidIds.isEmpty()) {
                newCurrentId = allValidIds.iterator().next();
            }
            changed = true;
            prefs.setCurrentProfileId(newCurrentId);
        }

        String currentProfileName = prefs.getProfileName(newCurrentId);
        return new ApplyResult(newNodes.size(), changed, currentIdBefore, newCurrentId, currentProfileName);
    }
}
