// Package cliconfig is the user-side connection configuration shared by
// every graphene client tool: the pipeline binary's own subcommands
// (push, run) and graphenectl. Named contexts live in one file under the
// user's config directory — the project repository never carries tokens.
//
// Resolution order, kubeconfig-style:
//   - the file: --config / GRAPHENE_CONFIG, else ~/.config/graphene/config.yaml;
//   - the context: an explicit name, else GRAPHENE_CONTEXT, else `current`;
//   - field overrides on top: GRAPHENE_ADDRESS, GRAPHENE_TOKEN,
//     GRAPHENE_NAMESPACE, GRAPHENE_INSECURE — with a server and a token
//     in the environment no file is needed at all (CI).
package cliconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Environment variables of the resolution chain. The field overrides
// reuse the wire names the worker roles already speak — one vocabulary.
const (
	// EnvConfig points at the config file (kubeconfig's KUBECONFIG).
	EnvConfig = "GRAPHENE_CONFIG"
	// EnvContext overrides the current context name.
	EnvContext = "GRAPHENE_CONTEXT"

	envAddress   = "GRAPHENE_ADDRESS"
	envToken     = "GRAPHENE_TOKEN" //nolint:gosec // the env var NAME, not a credential
	envNamespace = "GRAPHENE_NAMESPACE"
	envInsecure  = "GRAPHENE_INSECURE"
)

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

// Path is the config file location: the explicit override, else the
// user config directory.
func Path() (string, error) {
	if p := os.Getenv(EnvConfig); p != "" {
		return p, nil
	}
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
	raw, err := os.ReadFile(path) //nolint:gosec // the user's own config path
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

// Save writes the config back where Path points, creating the
// directory on first use. The file holds tokens: 0600.
func Save(cfg Config) error {
	path, err := Path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}

// Resolve picks the context: the explicit name, else EnvContext, else
// the file's current — then lays the environment field overrides on
// top. With GRAPHENE_ADDRESS and GRAPHENE_TOKEN set, no file and no
// context are needed: the environment IS the context (named "env").
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
	ctx, found := cfg.Contexts[name]
	if !found && name != "" && explicit != "" {
		// An explicitly named context MUST exist; the env cannot
		// silently paper over a typo.
		return Context{}, "", fmt.Errorf("context %q is not defined", name)
	}
	overlaid := overlay(ctx)
	if overlaid.Server == "" {
		path, _ := Path()
		return Context{}, "", fmt.Errorf(
			"no connection: set `current` in %s, pass --context, or set %s and %s",
			path, envAddress, envToken)
	}
	if !found {
		name = "env"
	}
	return overlaid, name, nil
}

// overlay lays the environment field overrides over a context.
func overlay(ctx Context) Context {
	if v := os.Getenv(envAddress); v != "" {
		ctx.Server = v
	}
	if v := os.Getenv(envToken); v != "" {
		ctx.Token = v
	}
	if v := os.Getenv(envNamespace); v != "" {
		ctx.Namespace = v
	}
	if v := os.Getenv(envInsecure); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			ctx.Insecure = b
		}
	}
	return ctx
}
