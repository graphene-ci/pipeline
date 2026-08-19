// Package cliconfig is the user-side connection configuration shared by
// every graphene client tool: the pipeline binary's own subcommands
// (push, run) and graphenectl. Named contexts live in one file under the
// user's config directory — the project repository never carries tokens.
package cliconfig

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// EnvContext overrides the current context name.
const EnvContext = "GRAPHENE_CONTEXT"

// Context is one named installation the tools talk to.
type Context struct {
	// Server is the installation's single door, host:port.
	Server string `yaml:"server"`
	// Token authenticates every plane behind the door.
	Token string `yaml:"token"`
	// Namespace scopes the work; empty means the token's default.
	Namespace string `yaml:"namespace,omitempty"`
	// Insecure switches the connection to plaintext (dev contours).
	Insecure bool `yaml:"insecure,omitempty"`
	// BaseImage overrides the built-in base of self-built worker
	// images — air-gapped installations mirror their own.
	BaseImage string `yaml:"baseImage,omitempty"`
}

// Config is the whole file: named contexts and the current choice.
type Config struct {
	Current  string             `yaml:"current"`
	Contexts map[string]Context `yaml:"contexts"`
}

// Path is the config file location.
func Path() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "graphene", "config.yaml"), nil
}

// Load reads the config file; a missing file is an empty config, not an
// error — the caller decides whether a context is required.
func Load() (Config, error) {
	path, err := Path()
	if err != nil {
		return Config{}, err
	}
	raw, err := os.ReadFile(path) //nolint:gosec // the fixed user-config path, not user input
	if os.IsNotExist(err) {
		return Config{}, nil
	}
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// Resolve picks the context: the explicit name, else EnvContext, else
// the file's current. The name is reported back for messages.
func Resolve(explicit string) (Context, string, error) {
	cfg, err := Load()
	if err != nil {
		return Context{}, "", err
	}
	name := explicit
	if name == "" {
		name = os.Getenv(EnvContext)
	}
	if name == "" {
		name = cfg.Current
	}
	if name == "" {
		path, _ := Path()
		return Context{}, "", fmt.Errorf("no context: set `current` in %s or pass --context", path)
	}
	ctx, ok := cfg.Contexts[name]
	if !ok {
		return Context{}, "", fmt.Errorf("context %q is not defined", name)
	}
	if ctx.Server == "" {
		return Context{}, "", fmt.Errorf("context %q has no server", name)
	}
	return ctx, name, nil
}
