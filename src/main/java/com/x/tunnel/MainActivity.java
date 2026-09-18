package com.x.tunnel;

import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.Intent;
import android.net.VpnService;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.MenuItem;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.AutoCompleteTextView;
import android.widget.EditText;
import android.widget.PopupMenu;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.google.android.material.button.MaterialButton;
import com.google.android.material.materialswitch.MaterialSwitch;
import com.google.android.material.tabs.TabLayout;
import com.google.android.material.textfield.TextInputEditText;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

public class MainActivity extends AppCompatActivity {
    private Preferences prefs;
    private ProfileAdapter adapter;

    // TabLayout 与页面容器
    private TabLayout tabLayout;
    private View layoutTabNodes;
    private View layoutTabSubscription;
    private View layoutBottomBar;

    // 节点页控件
    private TextView textStatusValue;
    private TextView textActiveProfile;
    private TextView textModeValue;
    private TextView textProfileCount;
    private MaterialSwitch switchGlobal;
    private MaterialButton buttonManageApps;
    private MaterialButton buttonNewProfile;
    private MaterialButton buttonEditCurrent;
    private MaterialButton buttonControl;
    private TextInputEditText inputSearchProfile;
    private String currentSearchQuery = "";

    // 订阅页控件
    private TextInputEditText inputSubUrl;
    private MaterialButton buttonPasteSubUrl;
    private AutoCompleteTextView dropdownSyncInterval;
    private TextView textLastSyncTime;
    private TextView textSubTotalNodes;
    private MaterialButton buttonSyncSub;
    private MaterialButton buttonClearSubNodes;
    private boolean isSyncing = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);

        prefs = new Preferences(this);

        // 绑定 TabLayout 与主要容器
        tabLayout = findViewById(R.id.tab_layout);
        layoutTabNodes = findViewById(R.id.layout_tab_nodes);
        layoutTabSubscription = findViewById(R.id.layout_tab_subscription);
        layoutBottomBar = findViewById(R.id.layout_bottom_bar);

        // 绑定节点页控件
        textStatusValue = findViewById(R.id.text_status_value);
        textActiveProfile = findViewById(R.id.text_active_profile);
        textModeValue = findViewById(R.id.text_mode_value);
        textProfileCount = findViewById(R.id.text_profile_count);
        switchGlobal = findViewById(R.id.switch_global);
        buttonManageApps = findViewById(R.id.button_manage_apps);
        buttonNewProfile = findViewById(R.id.button_new_profile);
        buttonEditCurrent = findViewById(R.id.button_edit_current);
        buttonControl = findViewById(R.id.button_control);
        inputSearchProfile = findViewById(R.id.input_search_profile);

        // 绑定订阅页控件
        inputSubUrl = findViewById(R.id.input_sub_url);
        buttonPasteSubUrl = findViewById(R.id.button_paste_sub_url);
        dropdownSyncInterval = findViewById(R.id.dropdown_sync_interval);
        textLastSyncTime = findViewById(R.id.text_last_sync_time);
        textSubTotalNodes = findViewById(R.id.text_sub_total_nodes);
        buttonSyncSub = findViewById(R.id.button_sync_sub);
        buttonClearSubNodes = findViewById(R.id.button_clear_sub_nodes);

        setupTabLayout();
        setupSubscriptionUi();
        setupSearchUi();

        // 独立原生 RecyclerView 列表配置
        RecyclerView recyclerProfiles = findViewById(R.id.recycler_profiles);
        recyclerProfiles.setLayoutManager(new LinearLayoutManager(this));
        adapter = new ProfileAdapter();
        recyclerProfiles.setAdapter(adapter);

        switchGlobal.setOnCheckedChangeListener((buttonView, isChecked) -> {
            if (!buttonView.isPressed()) {
                return;
            }
            if (prefs.getEnable()) {
                syncGlobalSwitch();
                Toast.makeText(this, R.string.toast_profile_running_locked, Toast.LENGTH_SHORT).show();
                return;
            }
            prefs.setGlobal(isChecked);
            updateModeUi();
        });

        buttonManageApps.setOnClickListener(v -> startActivity(new Intent(this, AppListActivity.class)));
        buttonNewProfile.setOnClickListener(v -> openProfileEditor(null));
        buttonEditCurrent.setOnClickListener(v -> openProfileEditor(prefs.getCurrentProfileId()));
        buttonControl.setOnClickListener(v -> toggleConnection());
        buttonControl.setOnLongClickListener(v -> {
            try {
                // 读取 Go 侧写入的诊断文件
                java.io.File f = new java.io.File(getFilesDir(), "xtunnel_status.log");
                String status = "(暂无诊断数据，请先开启连接等待数秒)";
                if (f.exists()) {
                    java.io.BufferedReader r = new java.io.BufferedReader(new java.io.FileReader(f));
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = r.readLine()) != null) { sb.append(line).append("\n"); }
                    r.close();
                    if (sb.length() > 0) status = sb.toString();
                }
                new AlertDialog.Builder(MainActivity.this)
                        .setTitle("隧道诊断")
                        .setMessage(status)
                        .setPositiveButton(android.R.string.ok, null)
                        .show();
            } catch (Throwable t) {
                Toast.makeText(MainActivity.this, "诊断暂不可用: " + t.getMessage(), Toast.LENGTH_SHORT).show();
            }
            return true;
        });

        updateUi();

        Intent intent = VpnService.prepare(this);
        if (intent != null) {
            startActivityForResult(intent, 0);
        } else {
            onActivityResult(0, RESULT_OK, null);
        }
    }

    private void setupTabLayout() {
        tabLayout.addOnTabSelectedListener(new TabLayout.OnTabSelectedListener() {
            @Override
            public void onTabSelected(TabLayout.Tab tab) {
                int position = tab.getPosition();
                if (position == 0) {
                    layoutTabNodes.setVisibility(View.VISIBLE);
                    layoutTabSubscription.setVisibility(View.GONE);
                    layoutBottomBar.setVisibility(View.VISIBLE);
                } else {
                    layoutTabNodes.setVisibility(View.GONE);
                    layoutTabSubscription.setVisibility(View.VISIBLE);
                    layoutBottomBar.setVisibility(View.GONE);
                    updateSubscriptionUi();
                }
            }

            @Override
            public void onTabUnselected(TabLayout.Tab tab) {}

            @Override
            public void onTabReselected(TabLayout.Tab tab) {}
        });
    }

    private void setupSearchUi() {
        inputSearchProfile.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {}

            @Override
            public void afterTextChanged(Editable s) {
                currentSearchQuery = s != null ? s.toString().trim().toLowerCase() : "";
                updateProfileList();
            }
        });
    }

    @Override
    protected void onResume() {
        super.onResume();
        updateUi();
    }

    @Override
    public void onBackPressed() {
        // 如果在订阅 Tab，先切换回节点 Tab
        if (tabLayout != null && tabLayout.getSelectedTabPosition() != 0) {
            TabLayout.Tab tab = tabLayout.getTabAt(0);
            if (tab != null) tab.select();
            return;
        }
        // 在主节点页按返回键：退到后台保持常驻，防误杀 VPN 隧道
        moveTaskToBack(true);
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (resultCode == RESULT_OK && prefs.getEnable()) {
            startService(new Intent(this, TProxyService.class).setAction(TProxyService.ACTION_CONNECT));
        }
    }

    private void toggleConnection() {
        if (prefs.getEnable()) {
            prefs.setEnable(false);
            updateUi();
            startService(new Intent(this, TProxyService.class).setAction(TProxyService.ACTION_DISCONNECT));
            return;
        }

        if (prefs.getWssAddr().trim().isEmpty()) {
            Toast.makeText(this, R.string.toast_server_required, Toast.LENGTH_SHORT).show();
            return;
        }

        prefs.setEnable(true);
        updateUi();
        startService(new Intent(this, TProxyService.class).setAction(TProxyService.ACTION_CONNECT));
    }

    private void updateUi() {
        syncGlobalSwitch();
        updateStatusUi();
        updateModeUi();
        updateProfileList();
        updateSubscriptionUi();

        boolean editable = !prefs.getEnable();
        buttonNewProfile.setEnabled(editable);
        buttonEditCurrent.setEnabled(editable);
        buttonSyncSub.setEnabled(editable && !isSyncing);
        buttonClearSubNodes.setEnabled(editable);
    }

    private void setupSubscriptionUi() {
        // 绑定输入框初始值与监听
        inputSubUrl.setText(prefs.getSubUrl());
        inputSubUrl.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}
            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {}
            @Override
            public void afterTextChanged(Editable s) {
                prefs.setSubUrl(s != null ? s.toString().trim() : "");
            }
        });

        // 剪贴板一键粘贴
        buttonPasteSubUrl.setOnClickListener(v -> {
            ClipboardManager cm = (ClipboardManager) getSystemService(Context.CLIPBOARD_SERVICE);
            if (cm != null && cm.hasPrimaryClip() && cm.getPrimaryClip().getItemCount() > 0) {
                ClipData.Item item = cm.getPrimaryClip().getItemAt(0);
                CharSequence text = item.getText();
                if (text != null && text.length() > 0) {
                    inputSubUrl.setText(text.toString().trim());
                    Toast.makeText(this, "已粘贴订阅地址", Toast.LENGTH_SHORT).show();
                    return;
                }
            }
            Toast.makeText(this, R.string.toast_clipboard_empty, Toast.LENGTH_SHORT).show();
        });

        // 绑定同步间隔下拉
        String[] intervals = getResources().getStringArray(R.array.sub_sync_interval_entries);
        ArrayAdapter<String> intervalAdapter = new ArrayAdapter<>(this, android.R.layout.simple_list_item_1, intervals);
        dropdownSyncInterval.setAdapter(intervalAdapter);
        int currentInterval = prefs.getSubSyncInterval();
        if (currentInterval >= 0 && currentInterval < intervals.length) {
            dropdownSyncInterval.setText(intervals[currentInterval], false);
        }
        dropdownSyncInterval.setOnItemClickListener((parent, view, position, id) -> {
            prefs.setSubSyncInterval(position);
        });

        buttonSyncSub.setOnClickListener(v -> syncSubscription(false));

        buttonClearSubNodes.setOnClickListener(v -> {
            int subCount = prefs.getSubProfileCount();
            if (subCount == 0) {
                Toast.makeText(this, "当前没有订阅节点", Toast.LENGTH_SHORT).show();
                return;
            }
            if (prefs.getEnable()) {
                Toast.makeText(this, R.string.toast_profile_running_locked, Toast.LENGTH_SHORT).show();
                return;
            }
            new AlertDialog.Builder(this)
                    .setTitle(R.string.sub_clear_confirm_title)
                    .setMessage(R.string.sub_clear_confirm_msg)
                    .setPositiveButton(R.string.ok, (dialog, which) -> {
                        int cleared = prefs.clearAllSubProfiles();
                        updateUi();
                        Toast.makeText(this, "已清空 " + cleared + " 个订阅节点", Toast.LENGTH_SHORT).show();
                    })
                    .setNegativeButton(R.string.cancel, null)
                    .show();
        });

        checkAutoSync();
    }

    private void updateSubscriptionUi() {
        long lastTime = prefs.getSubLastSyncTime();
        if (lastTime <= 0) {
            textLastSyncTime.setText(R.string.sub_never_synced);
        } else {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.getDefault());
            textLastSyncTime.setText(getString(R.string.sub_last_sync, sdf.format(new Date(lastTime))));
        }

        int subCount = prefs.getSubProfileCount();
        textSubTotalNodes.setText(getString(R.string.sub_total_nodes, subCount));
    }

    private void checkAutoSync() {
        String url = prefs.getSubUrl();
        if (url == null || url.isEmpty()) {
            return;
        }
        int interval = prefs.getSubSyncInterval();
        long lastTime = prefs.getSubLastSyncTime();
        long now = System.currentTimeMillis();

        boolean needSync = false;
        if (interval == SubConfig.INTERVAL_ON_LAUNCH) {
            needSync = true;
        } else if (interval == SubConfig.INTERVAL_6_HOURS) {
            if (now - lastTime > 6 * 3600 * 1000L) {
                needSync = true;
            }
        } else if (interval == SubConfig.INTERVAL_DAILY) {
            if (now - lastTime > 24 * 3600 * 1000L) {
                needSync = true;
            }
        }

        if (needSync && !prefs.getEnable()) {
            syncSubscription(true);
        }
    }

    private void syncSubscription(boolean silent) {
        String url = prefs.getSubUrl();
        if (url == null || url.trim().isEmpty()) {
            if (!silent) {
                Toast.makeText(this, "请先输入订阅地址", Toast.LENGTH_SHORT).show();
            }
            return;
        }

        if (prefs.getEnable()) {
            Toast.makeText(this, R.string.toast_profile_running_locked, Toast.LENGTH_SHORT).show();
            return;
        }

        isSyncing = true;
        buttonSyncSub.setEnabled(false);
        buttonSyncSub.setText(R.string.sub_syncing);

        SubscriptionManager.fetchAndUpdate(prefs, url, new SubscriptionManager.Callback() {
            @Override
            public void onSuccess(int count) {
                runOnUiThread(() -> {
                    isSyncing = false;
                    buttonSyncSub.setEnabled(!prefs.getEnable());
                    buttonSyncSub.setText(R.string.sub_sync_now);
                    updateUi();
                    Toast.makeText(MainActivity.this, "订阅同步成功，已更新 " + count + " 个节点", Toast.LENGTH_SHORT).show();
                });
            }

            @Override
            public void onError(String message) {
                runOnUiThread(() -> {
                    isSyncing = false;
                    buttonSyncSub.setEnabled(!prefs.getEnable());
                    buttonSyncSub.setText(R.string.sub_sync_now);
                    if (!silent) {
                        Toast.makeText(MainActivity.this, message, Toast.LENGTH_LONG).show();
                    }
                });
            }
        });
    }

    private void syncGlobalSwitch() {
        switchGlobal.setChecked(prefs.getGlobal());
    }

    private void updateStatusUi() {
        boolean enabled = prefs.getEnable();
        textStatusValue.setText(enabled ? R.string.status_connected : R.string.status_disconnected);
        textStatusValue.setTextColor(getColor(enabled ? R.color.xt_success : R.color.xt_text_primary));
        textActiveProfile.setText(getString(R.string.profile_label) + " " + prefs.getProfileName(prefs.getCurrentProfileId()));
        buttonControl.setText(enabled ? R.string.control_disable : R.string.control_enable);
    }

    private void updateModeUi() {
        boolean global = prefs.getGlobal();
        textModeValue.setText(global ? R.string.mode_global_summary : R.string.mode_app_summary);
        boolean editable = !prefs.getEnable();
        switchGlobal.setEnabled(editable);
        buttonManageApps.setEnabled(editable && !global);
    }

    private void updateProfileList() {
        List<ProfileItem> allItems = getSortedProfileItems();
        List<ProfileItem> displayItems = new ArrayList<>();

        if (currentSearchQuery.isEmpty()) {
            displayItems.addAll(allItems);
            textProfileCount.setText(getString(R.string.profile_count, allItems.size()));
        } else {
            for (ProfileItem it : allItems) {
                if (it.name.toLowerCase().contains(currentSearchQuery) ||
                        it.summary.toLowerCase().contains(currentSearchQuery)) {
                    displayItems.add(it);
                }
            }
            textProfileCount.setText(displayItems.size() + " / " + allItems.size());
        }

        adapter.setItems(displayItems, prefs.getCurrentProfileId(), prefs.getEnable());
    }

    private List<ProfileItem> getSortedProfileItems() {
        Set<String> ids = prefs.getProfileIds();
        List<ProfileItem> items = new ArrayList<>();
        for (String id : ids) {
            items.add(new ProfileItem(id, prefs.getProfileName(id), buildProfileSummary(id), prefs.isSubProfile(id)));
        }
        Collections.sort(items, Comparator.comparing(item -> item.name.toLowerCase()));
        return items;
    }

    private String buildProfileSummary(String profileId) {
        String server = prefs.getWssAddr(profileId).trim();
        if (server.isEmpty()) {
            server = getString(R.string.profile_empty_server);
        }
        String secure = prefs.getInsecure(profileId) ? getString(R.string.profile_summary_insecure) : getString(R.string.profile_summary_secure);
        String wsConn = getString(R.string.profile_summary_ws_conn, prefs.clampWsConn(prefs.getWsConn(profileId)));
        return getString(R.string.profile_summary_template, server, wsConn, secure);
    }

    private void openProfileEditor(String profileId) {
        if (prefs.getEnable()) {
            Toast.makeText(this, R.string.toast_profile_running_locked, Toast.LENGTH_SHORT).show();
            return;
        }
        Intent intent = new Intent(this, ProfileEditActivity.class);
        if (profileId != null) {
            intent.putExtra(ProfileEditActivity.EXTRA_PROFILE_ID, profileId);
        }
        startActivity(intent);
    }

    /**
     * 选择/切换配置
     * - 稳健且顺滑原则（Hot Switch）：
     *   1) 停用状态下：即时切换当前节点并刷新界面高亮；
     *   2) 运行状态下：点击其他节点即时触发无缝热切换（Hot Switch），
     *      无需手动停用再启用，系统 VPN 隧道保持连通，后台平滑重连至新节点内核！
     */
    private void selectProfile(ProfileItem item) {
        if (item.id.equals(prefs.getCurrentProfileId())) {
            if (prefs.getEnable()) {
                Toast.makeText(this, R.string.toast_already_running, Toast.LENGTH_SHORT).show();
            }
            return;
        }

        if (prefs.getWssAddr(item.id).trim().isEmpty()) {
            Toast.makeText(this, R.string.toast_server_required, Toast.LENGTH_SHORT).show();
            return;
        }

        // 立即更新当前选中节点并刷新界面高亮
        prefs.setCurrentProfileId(item.id);
        updateUi();

        if (prefs.getEnable()) {
            // 运行状态下：发送 ACTION_SWITCH 执行无缝热切换
            Toast.makeText(this, getString(R.string.toast_switching_profile, item.name), Toast.LENGTH_SHORT).show();
            Intent intent = new Intent(this, TProxyService.class);
            intent.setAction(TProxyService.ACTION_SWITCH);
            intent.putExtra(TProxyService.EXTRA_PROFILE_ID, item.id);
            intent.putExtra(TProxyService.EXTRA_PROFILE_NAME, item.name);
            intent.putExtra(TProxyService.EXTRA_WSS_ADDR, prefs.getWssAddr(item.id));
            intent.putExtra(TProxyService.EXTRA_WS_CONN, prefs.clampWsConn(prefs.getWsConn(item.id)));
            intent.putExtra(TProxyService.EXTRA_UDP_BLOCK_PORTS, prefs.getUdpBlockPorts(item.id));
            intent.putExtra(TProxyService.EXTRA_ECH_DNS, prefs.getEchDns(item.id));
            intent.putExtra(TProxyService.EXTRA_ECH_DOMAIN, prefs.getEchDomain(item.id));
            intent.putExtra(TProxyService.EXTRA_PREF_IP, prefs.getPrefIp(item.id));
            intent.putExtra(TProxyService.EXTRA_TOKEN, prefs.getToken(item.id));
            intent.putExtra(TProxyService.EXTRA_DISABLE_ECH, prefs.getDisableEch(item.id));
            intent.putExtra(TProxyService.EXTRA_IPS_PREF, prefs.getIpsPref(item.id));
            intent.putExtra(TProxyService.EXTRA_INSECURE, prefs.getInsecure(item.id));
            startService(intent);
        } else {
            // 停用状态下：普通切换
            Toast.makeText(this, getString(R.string.toast_select_profile_saved) + ": " + item.name, Toast.LENGTH_SHORT).show();
        }
    }

    private void showProfileMenu(View anchor, ProfileItem item) {
        if (prefs.getEnable()) {
            Toast.makeText(this, R.string.toast_profile_running_locked, Toast.LENGTH_SHORT).show();
            return;
        }

        PopupMenu menu = new PopupMenu(this, anchor);
        menu.getMenu().add(0, 1, 0, R.string.menu_edit);
        menu.getMenu().add(0, 2, 1, R.string.menu_copy);
        menu.getMenu().add(0, 3, 2, R.string.menu_rename);
        menu.getMenu().add(0, 4, 3, R.string.menu_delete);
        menu.setOnMenuItemClickListener(menuItem -> handleProfileMenu(item, menuItem));
        menu.show();
    }

    private boolean handleProfileMenu(ProfileItem item, MenuItem menuItem) {
        switch (menuItem.getItemId()) {
            case 1:
                openProfileEditor(item.id);
                return true;
            case 2:
                copyProfile(item.id);
                return true;
            case 3:
                renameProfile(item.id);
                return true;
            case 4:
                deleteProfile(item.id);
                return true;
            default:
                return false;
        }
    }

    private void copyProfile(String sourceId) {
        EditText input = new EditText(this);
        input.setSingleLine(true);
        input.setText(buildCopyName(prefs.getProfileName(sourceId) + "_副本"));

        new AlertDialog.Builder(this)
                .setTitle(R.string.menu_copy)
                .setView(input)
                .setPositiveButton(R.string.ok, (dialog, which) -> {
                    String name = input.getText().toString().trim();
                    if (name.isEmpty()) {
                        Toast.makeText(this, R.string.toast_name_empty, Toast.LENGTH_SHORT).show();
                        return;
                    }
                    if (prefs.hasProfileName(name, null)) {
                        Toast.makeText(this, R.string.toast_profile_exists, Toast.LENGTH_SHORT).show();
                        return;
                    }
                    String newId = UUID.randomUUID().toString();
                    prefs.addProfile(newId, name);
                    prefs.copyProfile(sourceId, newId);
                    prefs.setCurrentProfileId(newId);
                    updateUi();
                })
                .setNegativeButton(R.string.cancel, null)
                .show();
    }

    private void renameProfile(String profileId) {
        EditText input = new EditText(this);
        input.setSingleLine(true);
        input.setText(prefs.getProfileName(profileId));
        input.setSelection(input.getText().length());

        new AlertDialog.Builder(this)
                .setTitle(R.string.menu_rename)
                .setView(input)
                .setPositiveButton(R.string.ok, (dialog, which) -> {
                    String name = input.getText().toString().trim();
                    if (name.isEmpty()) {
                        Toast.makeText(this, R.string.toast_name_empty, Toast.LENGTH_SHORT).show();
                        return;
                    }
                    if (prefs.hasProfileName(name, profileId)) {
                        Toast.makeText(this, R.string.toast_profile_exists, Toast.LENGTH_SHORT).show();
                        return;
                    }
                    prefs.setProfileName(profileId, name);
                    updateUi();
                })
                .setNegativeButton(R.string.cancel, null)
                .show();
    }

    private void deleteProfile(String profileId) {
        if (prefs.getProfileIds().size() <= 1) {
            Toast.makeText(this, R.string.toast_cannot_delete_last, Toast.LENGTH_SHORT).show();
            return;
        }

        new AlertDialog.Builder(this)
                .setTitle(R.string.dialog_title_delete)
                .setPositiveButton(R.string.ok, (dialog, which) -> {
                    prefs.removeProfile(profileId);
                    if (profileId.equals(prefs.getCurrentProfileId())) {
                        List<ProfileItem> items = getSortedProfileItems();
                        if (!items.isEmpty()) {
                            prefs.setCurrentProfileId(items.get(0).id);
                        }
                    }
                    updateUi();
                    Toast.makeText(this, R.string.toast_profile_deleted, Toast.LENGTH_SHORT).show();
                })
                .setNegativeButton(R.string.cancel, null)
                .show();
    }

    private String buildCopyName(String baseName) {
        String candidate = baseName;
        int idx = 2;
        while (prefs.hasProfileName(candidate, null)) {
            candidate = baseName + idx;
            idx++;
        }
        return candidate;
    }

    private static final class ProfileItem {
        final String id;
        final String name;
        final String summary;
        final boolean isSub;

        ProfileItem(String id, String name, String summary, boolean isSub) {
            this.id = id;
            this.name = name;
            this.summary = summary;
            this.isSub = isSub;
        }
    }

    private final class ProfileAdapter extends RecyclerView.Adapter<ProfileViewHolder> {
        private final List<ProfileItem> items = new ArrayList<>();
        private String currentProfileId;
        private boolean isRunning;

        void setItems(List<ProfileItem> newItems, String selectedId, boolean running) {
            items.clear();
            items.addAll(newItems);
            currentProfileId = selectedId;
            isRunning = running;
            notifyDataSetChanged();
        }

        @NonNull
        @Override
        public ProfileViewHolder onCreateViewHolder(@NonNull android.view.ViewGroup parent, int viewType) {
            View view = getLayoutInflater().inflate(R.layout.item_profile, parent, false);
            return new ProfileViewHolder(view);
        }

        @Override
        public void onBindViewHolder(@NonNull ProfileViewHolder holder, int position) {
            ProfileItem item = items.get(position);
            boolean selected = item.id.equals(currentProfileId);
            boolean running = selected && isRunning;

            holder.name.setText(item.name);
            holder.summary.setText(item.summary);
            holder.subBadge.setVisibility(item.isSub ? View.VISIBLE : View.GONE);

            // 强视觉高亮与徽章
            if (running) {
                // 运行中：翡翠绿卡片边框、浅绿雅致微底色、● 运行中绿色徽章
                holder.card.setCardBackgroundColor(getColor(R.color.xt_running_card_bg));
                holder.card.setStrokeColor(getColor(R.color.xt_success));
                holder.card.setStrokeWidth(dpToPx(2));
                holder.badge.setText(R.string.status_running_badge);
                holder.badge.setBackgroundResource(R.drawable.profile_running_badge_background);
                holder.badge.setTextColor(getColor(R.color.xt_running_text));
                holder.badge.setVisibility(View.VISIBLE);
            } else if (selected) {
                // 已选中未运行：科技蓝边框、淡蓝微底色、已选择蓝色徽章
                holder.card.setCardBackgroundColor(getColor(R.color.xt_selected_card_bg));
                holder.card.setStrokeColor(getColor(R.color.xt_accent));
                holder.card.setStrokeWidth(dpToPx(2));
                holder.badge.setText(R.string.status_selected_badge);
                holder.badge.setBackgroundResource(R.drawable.profile_badge_background);
                holder.badge.setTextColor(getColor(R.color.xt_accent));
                holder.badge.setVisibility(View.VISIBLE);
            } else {
                // 未选中：标准卡片背景、普通边框、不显示徽章
                holder.card.setCardBackgroundColor(getColor(R.color.xt_surface));
                holder.card.setStrokeColor(getColor(R.color.xt_border));
                holder.card.setStrokeWidth(dpToPx(1));
                holder.badge.setVisibility(View.GONE);
            }

            // 无论何时，所有卡片均保持 1.0f 完全可用，可随时点击切换！
            holder.selectArea.setAlpha(1.0f);
            holder.selectArea.setOnClickListener(v -> selectProfile(item));
            holder.editButton.setOnClickListener(v -> openProfileEditor(item.id));
            holder.moreButton.setOnClickListener(v -> showProfileMenu(v, item));
        }

        @Override
        public int getItemCount() {
            return items.size();
        }

        private int dpToPx(int dp) {
            return (int) (dp * getResources().getDisplayMetrics().density + 0.5f);
        }
    }

    private static final class ProfileViewHolder extends RecyclerView.ViewHolder {
        final com.google.android.material.card.MaterialCardView card;
        final View selectArea;
        final TextView name;
        final TextView summary;
        final TextView badge;
        final TextView subBadge;
        final MaterialButton editButton;
        final View moreButton;

        ProfileViewHolder(@NonNull View itemView) {
            super(itemView);
            card = (com.google.android.material.card.MaterialCardView) itemView;
            selectArea = itemView.findViewById(R.id.profile_select_area);
            name = itemView.findViewById(R.id.text_profile_name);
            summary = itemView.findViewById(R.id.text_profile_summary);
            badge = itemView.findViewById(R.id.text_profile_badge);
            subBadge = itemView.findViewById(R.id.text_profile_sub_badge);
            editButton = itemView.findViewById(R.id.button_profile_edit);
            moreButton = itemView.findViewById(R.id.button_profile_more);
        }
    }
}
