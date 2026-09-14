package pipeline

import (
	"context"
	"testing"
)

func TestCLIBearerNamespace(t *testing.T) {
	for _, namespace := range []string{"", "t-stroppy-live"} {
		t.Run(namespace, func(t *testing.T) {
			creds := cliBearer{token: "test-token", namespace: namespace}
			md, err := creds.GetRequestMetadata(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if md["authorization"] != "Bearer test-token" || md["x-graphene-namespace"] != namespace {
				t.Fatal("CLI credentials lost the selected namespace or authorization")
			}
			if _, exists := md["x-graphene-namespace"]; exists != (namespace != "") {
				t.Fatal("empty namespace must use the server default")
			}
		})
	}
}
