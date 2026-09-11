package com.x.tunnel;

import android.content.Intent;
import android.net.VpnService;
import android.os.Bundle;
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
    private TextView textStatusValue;
    private TextView textActiveProfile;
    private TextView textModeValue;
    private TextView textProfileCount;
    private MaterialSwitch switchGlobal;
    private MaterialButton buttonManageApps;
    private MaterialButton buttonNewProfile;
    private MaterialButton buttonEditCurrent;
    private MaterialButton buttonControl;

    // 订阅相关控件
    private TextInputEditText inputSubUrl;
    private AutoCompleteTextView dropdownSyncInterval;
    private TextView textLastSyncTime;
    private MaterialButton buttonSyncSub;
    private boolean isSyncing = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);

        prefs = new Preferences(this);

        textStatusValue = findViewById(R.id.text_status_value);
        textActiveProfile = findViewById(R.id.text_active_profile);
        textModeValue = findViewById(R.id.text_mode_value);
        textProfileCount = findViewById(R.id.text_profile_count);
        switchGlobal = findViewById(R.id.switch_global);
        buttonManageApps = findViewById(R.id.button_manage_apps);
        buttonNewProfile = findViewById(R.id.button_new_profile);
        buttonEditCurrent = findViewById(R.id.button_edit_current);
        buttonControl = findViewById(R.id.button_control);

        inputSubUrl = findViewById(R.id.input_sub_url);
        dropdownSyncInterval = findViewById(R.id.dropdown_sync_interval);
        textLastSyncTime = findViewById(R.id.text_last_sync_time);
        buttonSyncSub = findViewById(R.id.button_sync_sub);

        setupSubscriptionUi();

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
                String status = com.x.tunnel.tunnel.Tunnel.GetTunnelStatus();
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

    @Override
    protected void onResume() {
        super.onResume();
        updateUi();
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
                prefs.setSubUrl(s.toString().trim());
            }
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
        List<ProfileItem> items = getSortedProfileItems();
        adapter.setItems(items, prefs.getCurrentProfileId(), prefs.getEnable());
        textProfileCount.setText(getString(R.string.profile_count, items.size()));
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

    private void selectProfile(ProfileItem item) {
        if (prefs.getEnable()) {
            Toast.makeText(this, R.string.toast_profile_running_locked, Toast.LENGTH_SHORT).show();
            return;
        }
        if (!item.id.equals(prefs.getCurrentProfileId())) {
            prefs.setCurrentProfileId(item.id);
            updateUi();
            Toast.makeText(this, R.string.toast_select_profile_saved, Toast.LENGTH_SHORT).show();
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
        private boolean locked;

        void setItems(List<ProfileItem> newItems, String selectedId, boolean isLocked) {
            items.clear();
            items.addAll(newItems);
            currentProfileId = selectedId;
            locked = isLocked;
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
            holder.name.setText(item.name);
            holder.summary.setText(item.summary);
            holder.badge.setVisibility(selected ? View.VISIBLE : View.GONE);
            holder.subBadge.setVisibility(item.isSub ? View.VISIBLE : View.GONE);
            holder.card.setStrokeColor(getColor(selected ? R.color.xt_accent : R.color.xt_border));
            holder.card.setStrokeWidth(selected ? 2 : 1);
            holder.selectArea.setAlpha(locked && !selected ? 0.72f : 1.0f);
            holder.selectArea.setOnClickListener(v -> selectProfile(item));
            holder.editButton.setOnClickListener(v -> openProfileEditor(item.id));
            holder.moreButton.setOnClickListener(v -> showProfileMenu(v, item));
        }

        @Override
        public int getItemCount() {
            return items.size();
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
