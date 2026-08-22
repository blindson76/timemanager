package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type AppConfig struct {
	NodeID            string
	RaftAddr          string
	HttpAddr          string
	BootstrapServers  []string
	BootstrapDelay    int
	ExpectedBootstrap int
}

func parseConfig() *AppConfig {
	c := &AppConfig{
		NodeID:            "node1",
		RaftAddr:          "127.0.0.1:9001",
		HttpAddr:          "127.0.0.1:8001",
		BootstrapServers:  []string{"node-1@127.0.0.1:9091"},
		BootstrapDelay:    5,
		ExpectedBootstrap: 3,
	}
	flag.StringVar(&c.NodeID, "node-id", c.NodeID, "Node ID")
	flag.StringVar(&c.RaftAddr, "raft-addr", c.RaftAddr, "Raft address")
	flag.StringVar(&c.HttpAddr, "http-addr", c.HttpAddr, "HTTP address")
	flag.Func("bootstrap-servers", "Bootstrap servers (comma-separated)", func(s string) error {
		c.BootstrapServers = strings.Split(s, ",")
		if i := slices.Index(c.BootstrapServers, c.RaftAddr); i >= 0 {
			c.BootstrapServers = slices.Delete(c.BootstrapServers, i, i+1)
		}
		return nil
	})
	flag.IntVar(&c.BootstrapDelay, "bootstrap-delay", c.BootstrapDelay, "Bootstrap delay in seconds")
	flag.IntVar(&c.ExpectedBootstrap, "expected-bootstrap", c.ExpectedBootstrap, "Expected number of bootstrap nodes")
	flag.Parse()
	return c
}

type TimeFSM struct {
	mu   sync.RWMutex
	data map[string]string
}
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func NewTimeFSM() *TimeFSM {
	return &TimeFSM{
		data: make(map[string]string),
	}
}
func (f *TimeFSM) Apply(log *raft.Log) interface{} {
	var cmd Command

	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Op {

	case "set":
		f.data[cmd.Key] = cmd.Value

	case "delete":
		delete(f.data, cmd.Key)

	default:
		return fmt.Errorf("unknown command: %s", cmd.Op)
	}

	return nil
}
func (f *TimeFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	data := make(map[string]string, len(f.data))

	for k, v := range f.data {
		data[k] = v
	}

	return &FSMSnapshot{
		data: data,
	}, nil
}

func (f *TimeFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data := make(map[string]string)

	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.data = data

	return nil
}

type FSMSnapshot struct {
	data map[string]string
}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *FSMSnapshot) Release() {
}

func main() {

	config := parseConfig()
	fmt.Printf("Starting node with config: %+v\n", config)

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.Logger = nil // Disable logging for simplicity
	raftConfig.SnapshotInterval = 30 * time.Second
	raftConfig.SnapshotThreshold = 100

	snapshotStore := raft.NewInmemSnapshotStore()
	logStore := raft.NewInmemStore()

	transport, err := raft.NewTCPTransport(config.RaftAddr, nil, 3, 10*time.Second, nil)
	if err != nil {
		fmt.Printf("Failed to create transport: %v\n", err)
		return
	}

	fsm := NewTimeFSM()

	r, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		fmt.Printf("Failed to create Raft node: %v\n", err)
		return
	}
	bootstrapServers, others, err := waitForBootstrap(context.Background(), config.BootstrapServers, config.ExpectedBootstrap-1)
	if err != nil {
		fmt.Printf("Failed to wait for bootstrap servers: %v\n", err)
		return
	}

	fmt.Printf("initial bootstrap servers ready: %+v\n", bootstrapServers)
	for other := range others {
		fmt.Printf("Other bootstrap server ready: %s\n", other)
	}
	fmt.Printf("Raft node created with ID: %+v\n", r.LastIndex())
	select {}

}

func waitForBootstrap(
	ctx context.Context,
	servers []string,
	expected int,
) ([]string, <-chan string, error) {

	if expected <= 0 || expected > len(servers) {
		return nil, nil, fmt.Errorf(
			"invalid expected count: %d, total servers: %d",
			expected,
			len(servers),
		)
	}

	readyNodes := make(chan string, len(servers))

	var wg sync.WaitGroup

	for _, server := range servers {
		server := server

		wg.Add(1)

		go func() {
			defer wg.Done()

			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				conn, err := net.DialTimeout("tcp", server, 2*time.Second)

				if err == nil {
					conn.Close()

					fmt.Printf(
						"Successfully connected to bootstrap server: %s\n",
						server,
					)

					select {
					case readyNodes <- server:
					case <-ctx.Done():
					}

					return
				}

				select {
				case <-ctx.Done():
					return

				case <-ticker.C:
					// retry
				}
			}
		}()
	}

	// Close channel only when every server worker has finished.
	go func() {
		wg.Wait()
		close(readyNodes)
	}()

	fmt.Printf(
		"Waiting for %d/%d bootstrap servers...\n",
		expected,
		len(servers),
	)

	bootstrapServers := make([]string, 0, expected)

	for len(bootstrapServers) < expected {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()

		case ready, ok := <-readyNodes:
			if !ok {
				return nil, nil, fmt.Errorf(
					"only %d/%d bootstrap servers became ready",
					len(bootstrapServers),
					expected,
				)
			}

			fmt.Printf("Bootstrap server ready: %s\n", ready)

			bootstrapServers = append(bootstrapServers, ready)
		}
	}

	// Remaining ready servers will continue arriving here.
	return bootstrapServers, readyNodes, nil
}
