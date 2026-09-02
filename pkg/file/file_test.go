package file

import (
	"embed"
	"testing"
)

//go:embed testdata/sample.txt
var sampleFS embed.FS

func TestSourceValidate(t *testing.T) {
	if err := (Source{}).Validate(); err == nil {
		t.Fatal("empty source must be refused")
	}
	if err := FromBytes([]byte("x")).Validate(); err != nil {
		t.Fatalf("FromBytes must be valid: %v", err)
	}
	if err := FromSecret("s").Validate(); err != nil {
		t.Fatalf("FromSecret must be valid: %v", err)
	}
	two := Source{Bytes: []byte("x"), Secret: "s"}
	if err := two.Validate(); err == nil {
		t.Fatal("two origins must be refused")
	}
}

func TestFromEmbed(t *testing.T) {
	s := FromEmbed(sampleFS, "testdata/sample.txt")
	if string(s.Bytes) != "hello embed\n" {
		t.Fatalf("embed content wrong: %q", s.Bytes)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("embed source must be valid: %v", err)
	}
	miss := FromEmbed(sampleFS, "testdata/nope.txt")
	if err := miss.Validate(); err == nil {
		t.Fatal("missing embed file must yield an invalid (empty) source")
	}
}
