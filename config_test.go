package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
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

func TestLoadConfigServices(t *testing.T) {
	c, err := loadConfig("cluster.yaml", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range c.Services {
		names = append(names, d.name())
	}
	if !slices.Equal(names, []string{"mongo", "kafka", "billing", "audit"}) {
		t.Fatalf("services = %v", names)
	}
	m := c.Services[0].Mongo
	if m == nil || m.ReplicaSet != "rs0" || m.Replicas != 3 || len(m.Members) != 3 || m.ReportWindow != 15*time.Second || m.AllowForceReconfig {
		t.Fatalf("mongo spec = %+v", m)
	}
	k := c.Services[1].Service
	if k == nil || k.StartTimeout != defaultStartTimeout*time.Second || k.RetryDelay != defaultRetryDelay*time.Second {
		t.Fatalf("defaults not applied: %+v", k)
	}
}

func TestParseServicesRejectsInvalid(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	for name, services := range map[string][]serviceConfig{
		"empty name":           {{Name: ""}},
		"duplicate":            {{Name: "a"}, {Name: "a"}},
		"reserved name":        {{Name: "timesync"}},
		"unknown dep":          {{Name: "a", DependsOn: []string{"b"}}},
		"negative delay":       {{Name: "a", RetryDelay: -1}},
		"unknown kind":         {{Name: "a", Kind: "redis"}},
		"mongo block no kind":  {{Name: "a", Mongo: &mongoConfig{}}},
		"mongo unknown member": {{Name: "m", Kind: "mongo", Mongo: &mongoConfig{Members: []string{"n9"}}}},
		"mongo dup member":     {{Name: "m", Kind: "mongo", Mongo: &mongoConfig{Members: []string{"n1", "n1"}}}},
		"mongo replicas > 7":   {{Name: "m", Kind: "mongo", Mongo: &mongoConfig{Replicas: 8}}},
		"mongo replicas > n":   {{Name: "m", Kind: "mongo", Mongo: &mongoConfig{Members: []string{"n1"}, Replicas: 2}}},
		"mongo start_timeout":  {{Name: "m", Kind: "mongo", StartTimeout: 5}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseServices(services, nodes); err == nil {
				t.Fatal("expected error")
			}
		})
	}

	defs, err := parseServices([]serviceConfig{{Name: "b", DependsOn: []string{"a"}}, {Name: "a", DependsOn: []string{"timesync"}, StartTimeout: 5}}, nodes)
	if err != nil || defs[1].Service.StartTimeout != 5*time.Second {
		t.Fatalf("forward reference or custom timeout failed: %+v, %v", defs, err)
	}

	defs, err = parseServices([]serviceConfig{{Name: "db", Kind: "mongo"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if m := defs[0].Mongo; m.ReplicaSet != "db" || !slices.Equal(m.Members, nodes) || m.Replicas != 3 || m.OrderTimeout != defaultOrderTimeout*time.Second {
		t.Fatalf("mongo defaults = %+v", m)
	}
}
