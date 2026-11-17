package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func orchardValidConfig(t *testing.T) ScaleSetConfig {
	t.Helper()
	return ScaleSetConfig{Provider: "orchard", RegistrationURL: "https://github.com/example/project", ScaleSetName: "macos", Token: "test-token", RunnerImage: "registry.example/macos:runner", MaxRunners: 2, Orchard: OrchardConfig{URL: "http://127.0.0.1:6120", ServiceAccount: "runner", TokenEnv: "ORCHARD_TOKEN", StateDir: t.TempDir(), CPU: 4, Memory: 8192, RunnerDir: "/opt/actions-runner"}}
}

func TestOrchardConfigValidatesAdmissionAndSecretBoundary(t *testing.T) {
	ss := orchardValidConfig(t)
	if err := ss.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		modify func(*ScaleSetConfig)
	}{
		{"warm guests", func(c *ScaleSetConfig) { c.MinRunners = 1 }},
		{"implicit CPU", func(c *ScaleSetConfig) { c.Orchard.CPU = 0 }},
		{"implicit RAM", func(c *ScaleSetConfig) { c.Orchard.Memory = 0 }},
		{"URL credential", func(c *ScaleSetConfig) { c.Orchard.URL = "https://user:secret@orchard.example" }},
		{"remote HTTP", func(c *ScaleSetConfig) { c.Orchard.URL = "http://orchard.example" }},
		{"URL query", func(c *ScaleSetConfig) { c.Orchard.URL = "https://orchard.example?token=secret" }},
		{"relative state", func(c *ScaleSetConfig) { c.Orchard.StateDir = "state" }},
		{"missing token source", func(c *ScaleSetConfig) { c.Orchard.TokenEnv = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := ss
			tc.modify(&candidate)
			if candidate.Validate() == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}

func TestLoadOrchardConfigurationAndSelectors(t *testing.T) {
	v := viper.New()
	v.SetConfigType("toml")
	err := v.ReadConfig(strings.NewReader(`provider="orchard"
url="https://github.com/example/project"
name="macos"
token="test-token"
max-runners=2
runner-image="registry.example/macos:runner"
[orchard]
url="https://orchard.example"
service-account="runner"
token-env="ORCHARD_TOKEN"
state-dir="/var/lib/runscaler"
cpu=4
memory=8192
runner-dir="/opt/actions-runner"
poll-interval="3s"
[orchard.worker-labels]
platform="macos"
`))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(v)
	if err != nil {
		t.Fatal(err)
	}
	sets := cfg.ResolveScaleSets()
	if len(sets) != 1 {
		t.Fatal("wrong scale set count")
	}
	ss := sets[0]
	if len(cfg.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", cfg.Warnings)
	}
	if err = ss.Validate(); err != nil {
		t.Fatal(err)
	}
	if !ss.IsOrchard() || ss.IsDocker() || ss.IsTart() || ss.Orchard.WorkerLabels["platform"] != "macos" || ss.Orchard.EffectivePollInterval() != 3*time.Second {
		t.Fatalf("wrong provider configuration: %+v", ss.Orchard)
	}
}
