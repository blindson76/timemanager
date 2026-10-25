package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"time"

	"example.com/mystate/controllers/mongo"
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
	Services          []serviceDef
}

// serviceDef is one configured controller: exactly one field is set.
type serviceDef struct {
	Service *service.Spec
	Mongo   *mongo.Spec
}

func (d serviceDef) name() string {
	if d.Mongo != nil {
		return d.Mongo.Name
	}
	return d.Service.Name
}

// Defaults, in seconds unless noted, when cluster.yaml omits a value.
const (
	defaultStartTimeout = 60
	defaultRetryDelay   = 10
	defaultReportWindow = 15
	defaultOrderTimeout = 120
	defaultReplicas     = 3 // capped at the number of eligible members
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
	Name string `yaml:"name"`
	// Kind selects the controller: "" (generic lifecycle) or "mongo".
	Kind         string       `yaml:"kind"`
	DependsOn    []string     `yaml:"depends_on"`
	StartTimeout int          `yaml:"start_timeout"` // seconds, generic only
	RetryDelay   int          `yaml:"retry_delay"`   // seconds
	Mongo        *mongoConfig `yaml:"mongo"`
}

type mongoConfig struct {
	ReplicaSet         string   `yaml:"replica_set"` // default: service name
	Members            []string `yaml:"members"`     // default: every cluster member
	Replicas           int      `yaml:"replicas"`
	ReportWindow       int      `yaml:"report_window"` // seconds
	OrderTimeout       int      `yaml:"order_timeout"` // seconds
	AllowForceReconfig bool     `yaml:"allow_force_reconfig"`
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

	nodes := make([]string, len(c.Peers))
	for i, p := range c.Peers {
		nodes[i] = string(p.ID)
	}
	if c.Services, err = parseServices(f.Services, nodes); err != nil {
		return nil, err
	}
	return c, nil
}

// parseServices validates service names, references and kind-specific
// settings and applies defaults. Dependency cycles are rejected later by the
// executor. nodes are the cluster member IDs.
func parseServices(services []serviceConfig, nodes []string) ([]serviceDef, error) {
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

	defs := make([]serviceDef, 0, len(services))
	for _, s := range services {
		for _, dep := range s.DependsOn {
			if !names[dep] {
				return nil, fmt.Errorf("service %s: depends on unknown service %q", s.Name, dep)
			}
		}
		if s.StartTimeout < 0 || s.RetryDelay < 0 {
			return nil, fmt.Errorf("service %s: start_timeout and retry_delay must not be negative", s.Name)
		}
		if s.RetryDelay == 0 {
			s.RetryDelay = defaultRetryDelay
		}

		switch s.Kind {
		case "":
			if s.Mongo != nil {
				return nil, fmt.Errorf("service %s: mongo settings require kind: mongo", s.Name)
			}
			if s.StartTimeout == 0 {
				s.StartTimeout = defaultStartTimeout
			}
			defs = append(defs, serviceDef{Service: &service.Spec{
				Name:         s.Name,
				DependsOn:    s.DependsOn,
				StartTimeout: seconds(s.StartTimeout),
				RetryDelay:   seconds(s.RetryDelay),
			}})
		case "mongo":
			spec, err := parseMongo(s, nodes)
			if err != nil {
				return nil, fmt.Errorf("service %s: %w", s.Name, err)
			}
			defs = append(defs, serviceDef{Mongo: spec})
		default:
			return nil, fmt.Errorf("service %s: unknown kind %q", s.Name, s.Kind)
		}
	}
	return defs, nil
}

func parseMongo(s serviceConfig, nodes []string) (*mongo.Spec, error) {
	m := mongoConfig{}
	if s.Mongo != nil {
		m = *s.Mongo
	}
	if s.StartTimeout != 0 {
		return nil, errors.New("start_timeout does not apply to mongo; use mongo.order_timeout")
	}
	if m.ReplicaSet == "" {
		m.ReplicaSet = s.Name
	}
	if len(m.Members) == 0 {
		m.Members = slices.Clone(nodes)
	}
	seen := map[string]bool{}
	for _, n := range m.Members {
		if !slices.Contains(nodes, n) {
			return nil, fmt.Errorf("mongo member %q is not a cluster member", n)
		}
		if seen[n] {
			return nil, fmt.Errorf("duplicate mongo member %q", n)
		}
		seen[n] = true
	}
	if m.Replicas == 0 {
		m.Replicas = min(defaultReplicas, len(m.Members))
	}
	if m.Replicas < 1 || m.Replicas > 7 || m.Replicas > len(m.Members) {
		return nil, fmt.Errorf("replicas must be between 1 and min(7, %d members)", len(m.Members))
	}
	if m.ReportWindow < 0 || m.OrderTimeout < 0 {
		return nil, errors.New("report_window and order_timeout must not be negative")
	}
	if m.ReportWindow == 0 {
		m.ReportWindow = defaultReportWindow
	}
	if m.OrderTimeout == 0 {
		m.OrderTimeout = defaultOrderTimeout
	}
	return &mongo.Spec{
		Name:               s.Name,
		DependsOn:          s.DependsOn,
		ReplicaSet:         m.ReplicaSet,
		Members:            m.Members,
		Replicas:           m.Replicas,
		ReportWindow:       seconds(m.ReportWindow),
		OrderTimeout:       seconds(m.OrderTimeout),
		RetryDelay:         seconds(s.RetryDelay),
		AllowForceReconfig: m.AllowForceReconfig,
	}, nil
}

func seconds(n int) time.Duration { return time.Duration(n) * time.Second }
