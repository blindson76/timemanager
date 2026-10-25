package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	c, err := loadConfig("cluster.yaml", "node-2")
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeID != "node-2" || c.RaftAddr != "127.0.0.1:9002" || c.HttpAddr != "127.0.0.1:8002" || c.BootstrapDelay != 5 || c.ExpectedBootstrap != 3 {
		t.Fatalf("unexpected config: %+v", c)
	}
	if len(c.Peers) != 4 || c.Peers[1].ID != "node-2" || c.Peers[1].HTTPAddr != c.HttpAddr {
		t.Fatalf("unexpected peers: %+v", c.Peers)
	}
}

func TestLoadConfigRejectsInvalidConfig(t *testing.T) {
	const valid = `bootstrap_delay: 5
expected_bootstrap: 1
members:
  - node_id: node-1
    raft_addr: 127.0.0.1:9001
    http_addr: 127.0.0.1:8001
`
	for _, tc := range []struct{ name, data, nodeID string }{
		{"missing node", valid, ""},
		{"unknown node", valid, "node-9"},
		{"malformed YAML", "members: [", "node-1"},
		{"unknown field", strings.Replace(valid, "bootstrap_delay", "typo", 1), "node-1"},
		{"multiple documents", valid + "---\n{}\n", "node-1"},
		{"negative delay", strings.Replace(valid, ": 5", ": -1", 1), "node-1"},
		{"too many expected", strings.Replace(valid, "bootstrap: 1", "bootstrap: 2", 1), "node-1"},
		{"invalid address", strings.Replace(valid, "127.0.0.1:9001", "invalid", 1), "node-1"},
		{"duplicate address", strings.Replace(valid, "127.0.0.1:9001", "127.0.0.1:8001", 1), "node-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cluster.yaml")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadConfig(path, tc.nodeID); err == nil {
				t.Fatal("expected config error")
			}
		})
	}
	if _, err := loadConfig(filepath.Join(t.TempDir(), "missing.yaml"), "node-1"); err == nil {
		t.Fatal("expected missing file error")
	}
}
