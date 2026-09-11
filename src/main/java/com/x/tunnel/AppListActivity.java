package com.x.tunnel;

import android.Manifest;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.View;
import android.view.ViewGroup;
import android.widget.CheckBox;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.google.android.material.appbar.MaterialToolbar;
import com.google.android.material.textfield.TextInputEditText;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AppListActivity extends AppCompatActivity {
    private Preferences prefs;
    private AppAdapter adapter;
    private TextView textSelectedCount;
    private boolean isChanged = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_app_list);

        prefs = new Preferences(this);

        MaterialToolbar toolbar = findViewById(R.id.toolbar);
        toolbar.setNavigationOnClickListener(v -> finish());
        textSelectedCount = findViewById(R.id.text_selected_count);
        TextInputEditText inputSearch = findViewById(R.id.input_search);

        RecyclerView recyclerView = findViewById(R.id.recycler_apps);
        recyclerView.setLayoutManager(new LinearLayoutManager(this));
        adapter = new AppAdapter(this, loadPackages());
        recyclerView.setAdapter(adapter);
        updateSelectedCount();

        inputSearch.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {
            }

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {
                adapter.applyFilter(s == null ? "" : s.toString());
                updateSelectedCount();
            }

            @Override
            public void afterTextChanged(Editable s) {
            }
        });
    }

    @Override
    protected void onPause() {
        super.onPause();
        if (isChanged) {
            Set<String> apps = new HashSet<>();
            for (AppItem item : adapter.allItems) {
                if (item.selected) {
                    apps.add(item.packageInfo.packageName);
                }
            }
            prefs.setApps(apps);
        }
    }

    private List<AppItem> loadPackages() {
        Set<String> selectedApps = prefs.getApps();
        PackageManager pm = getPackageManager();
        List<AppItem> items = new ArrayList<>();

        for (PackageInfo info : pm.getInstalledPackages(PackageManager.GET_PERMISSIONS)) {
            if (info.packageName.equals(getPackageName())) {
                continue;
            }
            if (info.requestedPermissions == null) {
                continue;
            }
            if (!Arrays.asList(info.requestedPermissions).contains(Manifest.permission.INTERNET)) {
                continue;
            }
            String label = info.applicationInfo.loadLabel(pm).toString();
            items.add(new AppItem(info, label, selectedApps.contains(info.packageName)));
        }

        Collections.sort(items, new Comparator<AppItem>() {
            @Override
            public int compare(AppItem a, AppItem b) {
                if (a.selected != b.selected) {
                    return a.selected ? -1 : 1;
                }
                return a.label.compareToIgnoreCase(b.label);
            }
        });
        return items;
    }

    private void updateSelectedCount() {
        int count = 0;
        for (AppItem item : adapter.allItems) {
            if (item.selected) {
                count++;
            }
        }
        textSelectedCount.setText(getString(R.string.selected_apps_count, count));
    }

    private final class AppAdapter extends RecyclerView.Adapter<AppViewHolder> {
        private final Context context;
        private final List<AppItem> allItems = new ArrayList<>();
        private final List<AppItem> filteredItems = new ArrayList<>();
        private String activeFilter = "";

        AppAdapter(Context context, List<AppItem> items) {
            this.context = context;
            allItems.addAll(items);
            filteredItems.addAll(items);
        }

        void applyFilter(String query) {
            activeFilter = query == null ? "" : query.trim().toLowerCase();
            filteredItems.clear();
            for (AppItem item : allItems) {
                if (activeFilter.isEmpty() || item.label.toLowerCase().contains(activeFilter)) {
                    filteredItems.add(item);
                }
            }
            notifyDataSetChanged();
        }

        @NonNull
        @Override
        public AppViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
            View view = getLayoutInflater().inflate(R.layout.appitem, parent, false);
            return new AppViewHolder(view);
        }

        @Override
        public void onBindViewHolder(@NonNull AppViewHolder holder, int position) {
            AppItem item = filteredItems.get(position);
            ApplicationInfo appInfo = item.packageInfo.applicationInfo;
            holder.icon.setImageDrawable(appInfo.loadIcon(context.getPackageManager()));
            holder.name.setText(item.label);
            holder.checkBox.setChecked(item.selected);
            holder.itemView.setOnClickListener(v -> {
                item.selected = !item.selected;
                holder.checkBox.setChecked(item.selected);
                isChanged = true;
                sortSelectedFirst();
                updateSelectedCount();
            });
        }

        @Override
        public int getItemCount() {
            return filteredItems.size();
        }

        private void sortSelectedFirst() {
            Collections.sort(allItems, new Comparator<AppItem>() {
                @Override
                public int compare(AppItem a, AppItem b) {
                    if (a.selected != b.selected) {
                        return a.selected ? -1 : 1;
                    }
                    return a.label.compareToIgnoreCase(b.label);
                }
            });
            applyFilter(activeFilter);
        }
    }

    private static final class AppViewHolder extends RecyclerView.ViewHolder {
        final ImageView icon;
        final TextView name;
        final CheckBox checkBox;

        AppViewHolder(@NonNull View itemView) {
            super(itemView);
            icon = itemView.findViewById(R.id.icon);
            name = itemView.findViewById(R.id.name);
            checkBox = itemView.findViewById(R.id.checked);
        }
    }

    private static final class AppItem {
        final PackageInfo packageInfo;
        final String label;
        boolean selected;

        AppItem(PackageInfo packageInfo, String label, boolean selected) {
            this.packageInfo = packageInfo;
            this.label = label;
            this.selected = selected;
        }
    }
}
