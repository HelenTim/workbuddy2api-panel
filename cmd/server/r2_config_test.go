package main

import (
	"slices"
	"strings"
	"testing"
)

func TestR2ConfigParseAndNormalize(t *testing.T) {
	c, err := ParseConfig([]byte(`{"r2":{"base_url":"https://wb.example.workers.dev/","auth_key":"k","prefix":"/panel//","sync_interval_seconds":0}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.R2.BaseURL != "https://wb.example.workers.dev" {
		t.Errorf("base_url 应去掉尾斜杠，得到 %q", c.R2.BaseURL)
	}
	if c.R2.Prefix != "panel/" {
		t.Errorf("prefix 应归一化为 panel/，得到 %q", c.R2.Prefix)
	}
	if c.R2.SyncIntervalSeconds != 60 {
		t.Errorf("sync_interval_seconds<=0 应回落 60，得到 %d", c.R2.SyncIntervalSeconds)
	}
	// 缺省 true 靠 Default() 赋值：JSON 未写 sync_deletes 时必须仍为 true，
	// 否则请求归档轮换后远端会无限增长。
	if !c.R2.SyncDeletes {
		t.Error("sync_deletes 缺省应为 true")
	}
}

func TestR2ConfigRequiresAuthKey(t *testing.T) {
	_, err := ParseConfig([]byte(`{"r2":{"base_url":"https://x.workers.dev"}}`))
	if err == nil || !strings.Contains(err.Error(), "auth_key") {
		t.Fatalf("缺少 auth_key 应报错，得到 %v", err)
	}
}

func TestR2ConfigRejectsBadURL(t *testing.T) {
	if _, err := ParseConfig([]byte(`{"r2":{"base_url":"ftp://x","auth_key":"k"}}`)); err == nil {
		t.Fatal("非 http(s) 的 base_url 应报错")
	}
}

func TestR2EnvOverrides(t *testing.T) {
	t.Setenv("WB2A_R2_BASE_URL", "https://env.workers.dev")
	t.Setenv("WB2A_R2_AUTH_KEY", "env-key")
	t.Setenv("WB2A_R2_PREFIX", "envp/")
	t.Setenv("WB2A_R2_SYNC_INTERVAL_SECONDS", "300")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.R2.BaseURL != "https://env.workers.dev" || c.R2.AuthKey != "env-key" || c.R2.Prefix != "envp/" {
		t.Fatalf("env 覆盖未生效: %+v", c.R2)
	}
	if c.R2.SyncIntervalSeconds != 300 {
		t.Fatalf("间隔 env 覆盖未生效: %d", c.R2.SyncIntervalSeconds)
	}
	managed := Default().EnvironmentManagedFields()
	for _, want := range []string{"r2.base_url", "r2.auth_key", "r2.prefix", "r2.sync_interval_seconds"} {
		if !slices.Contains(managed, want) {
			t.Fatalf("托管清单缺少 %s: %v", want, managed)
		}
	}
}

func TestR2ChangesRequireRestart(t *testing.T) {
	current, err := ParseConfig([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	next, err := ParseConfig([]byte(`{"r2":{"base_url":"https://x.workers.dev","auth_key":"k"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if fields := restartRequiredFields(current, next); !slices.Contains(fields, "r2") {
		t.Fatalf("r2 配置变化应要求重启，得到 %v", fields)
	}
}
