package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"example.com/mystate/controllers/service"
	"github.com/hashicorp/raft"
	"gopkg.in/yaml.v3"
)

// configFile is the single cluster definition shared by every member.
const configFile = "cluster.yaml"

type AppConfig struct {
	NodeID            string
	RaftAddr          string
	HttpAddr          string
	Peers             []peerServer
	BootstrapDelay    int
	ExpectedBootstrap int
	Services          []service.Spec
}

// Default service timings, in seconds, when cluster.yaml omits them.
const (
	defaultStartTimeout = 60
	defaultRetryDelay   = 10
)

type peerServer struct {
	ID       raft.ServerID
	Addr     raft.ServerAddress
	HTTPAddr string
}

type clusterFile struct {
	BootstrapDelay    int             `yaml:"bootstrap_delay"`
	ExpectedBootstrap int             `yaml:"expected_bootstrap"`
	Members           []memberConfig  `yaml:"members"`
	Services          []serviceConfig `yaml:"services"`
}

type serviceConfig struct {
	Name         string   `yaml:"name"`
	DependsOn    []string `yaml:"depends_on"`
	StartTimeout int      `yaml:"start_timeout"` // seconds
	RetryDelay   int      `yaml:"retry_delay"`   // seconds
}

type memberConfig struct {
	NodeID   string `yaml:"node_id"`
	RaftAddr string `yaml:"raft_addr"`
	HttpAddr string `yaml:"http_addr"`
}

// parseConfig reads the node ID from the command line and everything else
// from the shared cluster file.
func parseConfig() (*AppConfig, error) {
	nodeID := flag.String("node-id", "", "ID of this node, must match a member in "+configFile)
	flag.Parse()
	if flag.NArg() > 0 {
		return nil, fmt.Errorf("unexpected arguments: %v", flag.Args())
	}
	return loadConfig(configFile, *nodeID)
}

func loadConfig(path, nodeID string) (*AppConfig, error) {
	if nodeID == "" {
		return nil, errors.New("--node-id is required")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var f clusterFile
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: expected a single YAML document", path)
	}

	if f.BootstrapDelay < 0 {
		return nil, fmt.Errorf("bootstrap_delay must not be negative")
	}
	if len(f.Members) == 0 {
		return nil, fmt.Errorf("no members defined")
	}
	if f.ExpectedBootstrap < 1 || f.ExpectedBootstrap > len(f.Members) {
		return nil, fmt.Errorf("expected_bootstrap must be between 1 and %d", len(f.Members))
	}

	c := &AppConfig{
		NodeID:            nodeID,
		BootstrapDelay:    f.BootstrapDelay,
		ExpectedBootstrap: f.ExpectedBootstrap,
	}
	seen := make(map[string]bool)
	for _, m := range f.Members {
		if m.NodeID == "" {
			return nil, fmt.Errorf("member with empty node_id")
		}
		for _, key := range []string{"id:" + m.NodeID, m.RaftAddr, m.HttpAddr} {
			if seen[key] {
				return nil, fmt.Errorf("member %s: duplicate %q", m.NodeID, key)
			}
			seen[key] = true
		}
		for _, addr := range []string{m.RaftAddr, m.HttpAddr} {
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return nil, fmt.Errorf("member %s: invalid address %q: %w", m.NodeID, addr, err)
			}
		}

		c.Peers = append(c.Peers, peerServer{ID: raft.ServerID(m.NodeID), Addr: raft.ServerAddress(m.RaftAddr), HTTPAddr: m.HttpAddr})
		if m.NodeID == nodeID {
			c.RaftAddr, c.HttpAddr = m.RaftAddr, m.HttpAddr
		}
	}
	if c.RaftAddr == "" {
		return nil, fmt.Errorf("node %q is not a member in %s", nodeID, path)
	}

	if c.Services, err = parseServices(f.Services); err != nil {
		return nil, err
	}
	return c, nil
}

// parseServices validates service names and references and applies default
// timings. Dependency cycles are rejected later by the executor.
func parseServices(services []serviceConfig) ([]service.Spec, error) {
	names := map[string]bool{timesyncController: true}
	for _, s := range services {
		if s.Name == "" {
			return nil, errors.New("service with empty name")
		}
		if names[s.Name] {
			return nil, fmt.Errorf("duplicate or reserved service name %q", s.Name)
		}
		names[s.Name] = true
	}

	specs := make([]service.Spec, 0, len(services))
	for _, s := range services {
		for _, dep := range s.DependsOn {
			if !names[dep] {
				return nil, fmt.Errorf("service %s: depends on unknown service %q", s.Name, dep)
			}
		}
		if s.StartTimeout < 0 || s.RetryDelay < 0 {
			return nil, fmt.Errorf("service %s: start_timeout and retry_delay must not be negative", s.Name)
		}
		if s.StartTimeout == 0 {
			s.StartTimeout = defaultStartTimeout
		}
		if s.RetryDelay == 0 {
			s.RetryDelay = defaultRetryDelay
		}
		specs = append(specs, service.Spec{
			Name:         s.Name,
			DependsOn:    s.DependsOn,
			StartTimeout: time.Duration(s.StartTimeout) * time.Second,
			RetryDelay:   time.Duration(s.RetryDelay) * time.Second,
		})
	}
	return specs, nil
}
