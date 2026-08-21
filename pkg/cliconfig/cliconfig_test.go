package cliconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvConfig, path)
	return path
}

func TestResolveCurrentFromFile(t *testing.T) {
	writeConfig(t, "current: dev\ncontexts:\n  dev: {server: 'srv:7233', token: t1}\n")
	cc, name, err := Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if name != "dev" || cc.Server != "srv:7233" || cc.Token != "t1" {
		t.Fatalf("got %q %+v", name, cc)
	}
}

func TestResolveExplicitBeatsCurrentAndEnv(t *testing.T) {
	writeConfig(t, "current: dev\ncontexts:\n  dev: {server: a, token: t}\n  prod: {server: b, token: t}\n")
	t.Setenv(EnvContext, "dev")
	_, name, err := Resolve("prod")
	if err != nil {
		t.Fatal(err)
	}
	if name != "prod" {
		t.Fatalf("explicit must win, got %q", name)
	}
}

func TestResolveExplicitUnknownFails(t *testing.T) {
	writeConfig(t, "current: dev\ncontexts:\n  dev: {server: a, token: t}\n")
	t.Setenv(envAddress, "env-srv") // must NOT paper over the typo
	if _, _, err := Resolve("prdo"); err == nil {
		t.Fatal("want an error for an unknown explicit context")
	}
}

func TestResolveEnvOverlay(t *testing.T) {
	writeConfig(t, "current: dev\ncontexts:\n  dev: {server: 'srv:7233', token: t1, namespace: team}\n")
	t.Setenv(envToken, "ci-token")
	t.Setenv(envNamespace, "prod")
	cc, _, err := Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if cc.Token != "ci-token" || cc.Namespace != "prod" || cc.Server != "srv:7233" {
		t.Fatalf("overlay wrong: %+v", cc)
	}
}

func TestResolveEnvOnly(t *testing.T) {
	t.Setenv(EnvConfig, filepath.Join(t.TempDir(), "absent.yaml"))
	t.Setenv(envAddress, "ci:7233")
	t.Setenv(envToken, "ci-token")
	t.Setenv(envInsecure, "1")
	cc, name, err := Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if name != "env" || cc.Server != "ci:7233" || !cc.Insecure {
		t.Fatalf("env-only context wrong: %q %+v", name, cc)
	}
}

func TestResolveNothing(t *testing.T) {
	t.Setenv(EnvConfig, filepath.Join(t.TempDir(), "absent.yaml"))
	if _, _, err := Resolve(""); err == nil {
		t.Fatal("want an error with no file and no env")
	}
}

func TestSaveRoundTrip(t *testing.T) {
	t.Setenv(EnvConfig, filepath.Join(t.TempDir(), "sub", "config.yaml"))
	in := Config{Current: "dev", Contexts: map[string]Context{"dev": {Server: "s", Token: "t"}}}
	if err := Save(in); err != nil {
		t.Fatal(err)
	}
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Current != "dev" || got.Contexts["dev"].Server != "s" {
		t.Fatalf("round trip lost data: %+v", got)
	}
	path, _ := Path()
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config with tokens must be 0600, got %v", info.Mode().Perm())
	}
}
