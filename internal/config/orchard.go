package config

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

// OrchardConfig configures a remote Orchard controller and an externally
// installed guest starter. Credentials are read only from named environment
// variables; neither runner JIT secrets nor credentials belong in URLs.
type OrchardConfig struct {
	URL            string            `mapstructure:"url"`
	ServiceAccount string            `mapstructure:"service-account"`
	TokenEnv       string            `mapstructure:"token-env"`
	StateDir       string            `mapstructure:"state-dir"`
	CPU            int               `mapstructure:"cpu"`
	Memory         int               `mapstructure:"memory"`
	WorkerLabels   map[string]string `mapstructure:"worker-labels"`
	Username       string            `mapstructure:"username"`
	PasswordEnv    string            `mapstructure:"password-env"`
	RunnerDir      string            `mapstructure:"runner-dir"`
	StarterCommand string            `mapstructure:"starter-command"`
	PollInterval   time.Duration     `mapstructure:"poll-interval"`
	ReadyTimeout   time.Duration     `mapstructure:"ready-timeout"`
}

func (c OrchardConfig) EffectivePollInterval() time.Duration {
	if c.PollInterval > 0 {
		return c.PollInterval
	}
	return 2 * time.Second
}

func (c OrchardConfig) EffectiveReadyTimeout() time.Duration {
	if c.ReadyTimeout > 0 {
		return c.ReadyTimeout
	}
	return 5 * time.Minute
}

func (c OrchardConfig) EffectiveStarterCommand() string {
	if c.StarterCommand != "" {
		return c.StarterCommand
	}
	return "/usr/local/libexec/runscaler-orchard-runner"
}

func (ss ScaleSetConfig) orchardErrors() []error {
	c := ss.Orchard
	var errors []error
	add := func(message string) { errors = append(errors, fmt.Errorf("orchard.%s", message)) }
	u, err := url.ParseRequestURI(c.URL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		add("url must be an HTTP(S) controller URL without credentials, query or fragment")
	} else if u.Scheme != "https" {
		host := u.Hostname()
		ip := net.ParseIP(host)
		if u.Scheme != "http" || !(host == "localhost" || ip != nil && ip.IsLoopback()) {
			add("url requires HTTPS except on loopback")
		}
	}
	if c.ServiceAccount == "" || strings.ContainsAny(c.ServiceAccount, ":\r\n") {
		add("service-account is required and must not contain colon or newlines")
	}
	if !appKeyEnvName.MatchString(c.TokenEnv) {
		add("token-env must name an environment variable")
	}
	if c.PasswordEnv != "" && !appKeyEnvName.MatchString(c.PasswordEnv) {
		add("password-env must name an environment variable")
	}
	if !filepath.IsAbs(c.StateDir) || filepath.Clean(c.StateDir) != c.StateDir || c.StateDir == "/" {
		add("state-dir must be a clean absolute persistent directory other than /")
	}
	if c.CPU <= 0 || c.Memory <= 0 {
		add("cpu and memory must be positive explicit budgets")
	}
	if c.PollInterval < 0 || c.ReadyTimeout < 0 {
		add("poll-interval and ready-timeout must not be negative")
	}
	if !filepath.IsAbs(c.RunnerDir) || strings.ContainsAny(c.RunnerDir, "\r\n") {
		add("runner-dir must be an absolute guest path")
	}
	if strings.ContainsAny(c.EffectiveStarterCommand(), "\r\n") {
		add("starter-command must be one fixed command without newlines")
	}
	if ss.MinRunners != 0 {
		errors = append(errors, fmt.Errorf("orchard requires min-runners = 0; guests start only for demand"))
	}
	return errors
}
