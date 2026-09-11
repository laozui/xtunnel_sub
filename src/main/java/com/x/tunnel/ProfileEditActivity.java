package com.x.tunnel;

import android.os.Bundle;
import android.widget.ArrayAdapter;
import android.widget.AutoCompleteTextView;
import android.widget.Toast;

import androidx.appcompat.app.AppCompatActivity;

import com.google.android.material.appbar.MaterialToolbar;
import com.google.android.material.button.MaterialButton;
import com.google.android.material.materialswitch.MaterialSwitch;
import com.google.android.material.textfield.TextInputEditText;

import java.util.UUID;

public class ProfileEditActivity extends AppCompatActivity {
    public static final String EXTRA_PROFILE_ID = "profile_id";

    private Preferences prefs;
    private String profileId;
    private boolean isNewProfile;

    private TextInputEditText inputProfileName;
    private TextInputEditText inputWssAddr;
    private TextInputEditText inputToken;
    private TextInputEditText inputPrefIp;
    private AutoCompleteTextView inputIpsPref;
    private TextInputEditText inputWsConn;
    private TextInputEditText inputUdpBlockPorts;
    private TextInputEditText inputEchDns;
    private TextInputEditText inputEchDomain;
    private MaterialSwitch switchDisableEch;
    private MaterialSwitch switchInsecure;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile_edit);

        prefs = new Preferences(this);
        profileId = getIntent().getStringExtra(EXTRA_PROFILE_ID);
        isNewProfile = profileId == null || profileId.isEmpty();

        MaterialToolbar toolbar = findViewById(R.id.toolbar);
        toolbar.setTitle(isNewProfile ? R.string.new_profile_title : R.string.edit_profile_title);
        toolbar.setNavigationOnClickListener(v -> finish());

        inputProfileName = findViewById(R.id.input_profile_name);
        inputWssAddr = findViewById(R.id.input_wss_addr);
        inputToken = findViewById(R.id.input_token);
        inputPrefIp = findViewById(R.id.input_pref_ip);
        inputIpsPref = findViewById(R.id.input_ips_pref);
        inputWsConn = findViewById(R.id.input_ws_conn);
        inputUdpBlockPorts = findViewById(R.id.input_udp_block_ports);
        inputEchDns = findViewById(R.id.input_ech_dns);
        inputEchDomain = findViewById(R.id.input_ech_domain);
        switchDisableEch = findViewById(R.id.switch_disable_ech);
        switchInsecure = findViewById(R.id.switch_insecure);

        inputIpsPref.setAdapter(new ArrayAdapter<>(this, android.R.layout.simple_list_item_1, getResources().getStringArray(R.array.ips_pref_entries)));
        inputIpsPref.setOnClickListener(v -> inputIpsPref.showDropDown());

        switchInsecure.setOnCheckedChangeListener((buttonView, isChecked) -> {
            if (isChecked) {
                switchDisableEch.setChecked(true);
                switchDisableEch.setEnabled(false);
            } else {
                switchDisableEch.setEnabled(true);
            }
        });

        MaterialButton buttonCancel = findViewById(R.id.button_cancel);
        MaterialButton buttonSave = findViewById(R.id.button_save);
        buttonCancel.setOnClickListener(v -> finish());
        buttonSave.setOnClickListener(v -> saveProfile());

        bindProfile();
    }

    private void bindProfile() {
        if (isNewProfile) {
            inputProfileName.setText("");
            inputWssAddr.setText("");
            inputToken.setText("");
            inputPrefIp.setText("");
            inputIpsPref.setText(getResources().getStringArray(R.array.ips_pref_entries)[0], false);
            inputWsConn.setText(String.valueOf(3));
            inputUdpBlockPorts.setText("443");
            inputEchDns.setText("https://doh.pub/dns-query");
            inputEchDomain.setText("cloudflare-ech.com");
            switchDisableEch.setChecked(false);
            switchInsecure.setChecked(false);
            switchDisableEch.setEnabled(true);
            return;
        }

        inputProfileName.setText(prefs.getProfileName(profileId));
        inputWssAddr.setText(prefs.getWssAddr(profileId));
        inputToken.setText(prefs.getToken(profileId));
        inputPrefIp.setText(prefs.getPrefIp(profileId));
        inputIpsPref.setText(getIpsPrefLabel(prefs.getIpsPref(profileId)), false);
        inputWsConn.setText(String.valueOf(prefs.clampWsConn(prefs.getWsConn(profileId))));
        inputUdpBlockPorts.setText(prefs.getUdpBlockPorts(profileId));
        inputEchDns.setText(prefs.getEchDns(profileId));
        inputEchDomain.setText(prefs.getEchDomain(profileId));
        switchDisableEch.setChecked(prefs.getDisableEch(profileId));
        switchInsecure.setChecked(prefs.getInsecure(profileId));
        switchDisableEch.setEnabled(!switchInsecure.isChecked());
    }

    private void saveProfile() {
        String profileName = textOf(inputProfileName).trim();
        if (profileName.isEmpty()) {
            Toast.makeText(this, R.string.toast_name_empty, Toast.LENGTH_SHORT).show();
            return;
        }
        if (prefs.hasProfileName(profileName, isNewProfile ? null : profileId)) {
            Toast.makeText(this, R.string.toast_profile_exists, Toast.LENGTH_SHORT).show();
            return;
        }

        String targetId = profileId;
        if (isNewProfile) {
            targetId = UUID.randomUUID().toString();
            prefs.addProfile(targetId, profileName);
        } else {
            prefs.setProfileName(targetId, profileName);
            // 手动编辑过的节点，不再被标记为自动订阅节点，避免同步时被自动清理
            prefs.setProfileIsSub(targetId, false);
        }

        prefs.setWssAddr(targetId, textOf(inputWssAddr).trim());
        prefs.setToken(targetId, textOf(inputToken));
        prefs.setPrefIp(targetId, textOf(inputPrefIp).trim());
        prefs.setIpsPref(targetId, getIpsPrefValue(inputIpsPref.getText() == null ? "" : inputIpsPref.getText().toString()));
        prefs.setWsConn(targetId, prefs.clampWsConn(parseWsConn()));
        prefs.setUdpBlockPorts(targetId, textOf(inputUdpBlockPorts).trim());
        prefs.setEchDns(targetId, textOf(inputEchDns).trim());
        prefs.setEchDomain(targetId, textOf(inputEchDomain).trim());
        prefs.setInsecure(targetId, switchInsecure.isChecked());
        prefs.setDisableEch(targetId, switchInsecure.isChecked() || switchDisableEch.isChecked());
        prefs.setCurrentProfileId(targetId);

        Toast.makeText(this, isNewProfile ? R.string.toast_profile_created : R.string.toast_profile_updated, Toast.LENGTH_SHORT).show();
        finish();
    }

    private int parseWsConn() {
        try {
            return Integer.parseInt(textOf(inputWsConn).trim());
        } catch (Exception ignored) {
            return 3;
        }
    }

    private String textOf(TextInputEditText input) {
        return input.getText() == null ? "" : input.getText().toString();
    }

    private String getIpsPrefLabel(String value) {
        String[] entries = getResources().getStringArray(R.array.ips_pref_entries);
        if (value == null || value.isEmpty()) {
            return entries[0];
        }
        String normalized = value.replace(" ", "");
        if ("4,6".equals(normalized)) {
            return entries[1];
        }
        if ("6,4".equals(normalized)) {
            return entries[2];
        }
        if ("4".equals(normalized)) {
            return entries[3];
        }
        if ("6".equals(normalized)) {
            return entries[4];
        }
        return entries[0];
    }

    private String getIpsPrefValue(String label) {
        String[] entries = getResources().getStringArray(R.array.ips_pref_entries);
        if (entries[1].equals(label)) {
            return "4,6";
        }
        if (entries[2].equals(label)) {
            return "6,4";
        }
        if (entries[3].equals(label)) {
            return "4";
        }
        if (entries[4].equals(label)) {
            return "6";
        }
        return "";
    }
}
