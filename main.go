package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sort"
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
		BootstrapServers:  []string{"node-1@127.0.0.1:9091@127.0.0.1:8001"},
		BootstrapDelay:    5,
		ExpectedBootstrap: 3,
	}
	flag.StringVar(&c.NodeID, "node-id", c.NodeID, "Node ID")
	flag.StringVar(&c.RaftAddr, "raft-addr", c.RaftAddr, "Raft address")
	flag.StringVar(&c.HttpAddr, "http-addr", c.HttpAddr, "HTTP address")
	flag.Func("bootstrap-servers", "Bootstrap servers, comma-separated node-id@raft-host:port@http-host:port entries", func(s string) error {
		c.BootstrapServers = strings.Split(s, ",")
		return nil
	})
	flag.IntVar(&c.BootstrapDelay, "bootstrap-delay", c.BootstrapDelay, "Bootstrap delay in seconds")
	flag.IntVar(&c.ExpectedBootstrap, "expected-bootstrap", c.ExpectedBootstrap, "Expected number of bootstrap nodes")
	flag.Parse()

	return c
}

type peerServer struct {
	ID       raft.ServerID
	Addr     raft.ServerAddress
	HTTPAddr string
}

func parsePeers(servers []string) ([]peerServer, error) {
	peers := make([]peerServer, 0, len(servers))

	for _, s := range servers {
		parts := strings.Split(s, "@")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid bootstrap server %q, expected format node-id@raft-host:port@http-host:port", s)
		}

		peers = append(peers, peerServer{ID: raft.ServerID(parts[0]), Addr: raft.ServerAddress(parts[1]), HTTPAddr: parts[2]})
	}

	return peers, nil
}

type clusterState struct {
	State  string       `json:"state"`
	Leader string       `json:"leader"`
	Peers  []peerServer `json:"peers,omitempty"`
}

// serveClusterState exposes this node's raft state over HTTP so peers can
// tell, without any local persistence, whether the cluster is already up.
func serveClusterService(addr string, r *raft.Raft) {
	mux := http.NewServeMux()
	mux.HandleFunc("/state", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(clusterState{
			State:  r.State().String(),
			Leader: string(r.Leader()),
			Peers: func() []peerServer {
				servers := r.GetConfiguration().Configuration().Servers
				peers := make([]peerServer, len(servers))
				for i, s := range servers {
					peers[i] = peerServer{ID: s.ID, Addr: s.Address, HTTPAddr: ""} // HTTPAddr is not available from raft.Server
				}
				return peers
			}(),
		})
	})

	mux.HandleFunc("/join", func(w http.ResponseWriter, req *http.Request) {
		// Handle join requests here
		requestedID := req.URL.Query().Get("id")
		w.Write([]byte("id:" + requestedID))
	})

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			fmt.Printf("Status server stopped: %v\n", err)
		}
	}()
}

// discoverClusterLeader asks every peer for its known raft leader. If any
// peer reports one, the cluster has already been bootstrapped by someone
// else (or before a restart), and this node must not bootstrap again.
func discoverClusterLeader(ctx context.Context, peers []peerServer, timeout time.Duration) *clusterState {
	if len(peers) == 0 {
		return nil
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultChan := make(chan *clusterState, len(peers))

	// 1. WaitGroup ile kaç tane goroutine olduğunu takip ediyoruz
	var wg sync.WaitGroup

	for _, p := range peers {
		wg.Add(1)
		go func(peer peerServer) {
			defer wg.Done() // İş bitince (başarılı veya başarısız) sayacı 1 azalt

			client := &http.Client{Timeout: timeout}
			req, err := http.NewRequestWithContext(cancelCtx, http.MethodGet, "http://"+peer.HTTPAddr+"/state", nil)
			if err != nil {
				return // Başarısız: direkt çıkıyor, kanala veri atmıyor
			}

			resp, err := client.Do(req)
			if err != nil {
				return // Başarısız: direkt çıkıyor, kanala veri atmıyor
			}
			defer resp.Body.Close()

			var state clusterState
			if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
				return // Başarısız: direkt çıkıyor, kanala veri atmıyor
			}

			if state.Leader != "" {
				select {
				case resultChan <- &state: // Başarılı: Lideri kanala atıyor
				case <-cancelCtx.Done():
				}
			}
		}(p)
	}

	// 2. TÜM GOROUTINE'LERİN BİTMESİNİ BEKLEYEN ARKA PLAN İŞİ
	go func() {
		wg.Wait()         // Tüm goroutine'ler (başarılı/başarısız) bittiğinde burası tetiklenir
		close(resultChan) // Kanalı kapatıyoruz. Kapatılan kanaldan veri okumak tıkanmaz!
	}()

	// 3. ARTIK BURADA ASLA TAKILIP KALMAZ
	for {
		select {
		case leader, ok := <-resultChan:
			if !ok {
				// Eğer tüm peer'lar başarısız olduysa, wg.Wait() çalışır ve kanal kapatılır.
				// Kanal kapatıldığı an ok değeri 'false' olur ve kod beklemeden BURADAN ÇIKAR.
				return nil
			}
			// Eğer bir tane bile başarılı sonuç geldiyse anında lideri döner
			return leader
		case <-ctx.Done():
			return nil
		}
	}
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
	fmt.Printf("Applying command: %+v\n", cmd)

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
	fmt.Printf("Creating snapshot with data: %+v\n", f.data)

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

	fmt.Printf("Restoring snapshot from reader: %+v\n", rc)
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
	fmt.Printf("Persisting snapshot with data: %+v\n", s.data)
	if err := json.NewEncoder(sink).Encode(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *FSMSnapshot) Release() {
}

// go run . --node-id node-1 --raft-addr 127.0.0.1:9001 --http-addr 127.0.0.1:8001 --bootstrap-servers node-1@127.0.0.1:9001@127.0.0.1:8001,node-2@127.0.0.1:9002@127.0.0.1:8002,node-3@127.0.0.1:9003@127.0.0.1:8003,node-4@127.0.0.1:9004@12.0.0.1:8004 --expected-bootstrap 3
func main() {

	ctx := context.Background()
	config := parseConfig()
	fmt.Printf("Starting node with config: %+v\n", config)

	peers, err := parsePeers(config.BootstrapServers)
	if err != nil {
		fmt.Printf("Invalid bootstrap servers: %v\n", err)
		return
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.LogOutput = io.Discard // Disable logging for simplicity
	raftConfig.SnapshotInterval = 5 * time.Second
	raftConfig.SnapshotThreshold = 10
	raftConfig.TrailingLogs = 2

	snapshotStore := raft.NewInmemSnapshotStore()
	logStore := raft.NewInmemStore()

	transport, err := raft.NewTCPTransport(config.RaftAddr, nil, 3, 10*time.Second, io.Discard)
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
	var otherChan <-chan peerServer

	eventChan := make(chan raft.Observation, 50)
	observer := raft.NewObserver(eventChan, true, func(o *raft.Observation) bool {
		return true
	})
	r.RegisterObserver(observer)
	serveClusterService(config.HttpAddr, r)
	otherPeers := make([]peerServer, len(peers))
	copy(otherPeers, peers)
	slices.DeleteFunc(otherPeers, func(p peerServer) bool {
		return p.ID == raftConfig.LocalID
	})

	// Ask peers whether the cluster already has a leader instead of relying
	// on local state, since the in-memory stores don't survive a restart.
	leader := discoverClusterLeader(ctx, otherPeers, 2*time.Second)

	fmt.Printf("Discovered cluster leader: %s\n", leader)
	if leader != nil {
		fmt.Printf("Cluster already has a leader (%v), skipping bootstrap.\n", leader)
		if !slices.ContainsFunc(leader.Peers, func(p peerServer) bool {
			return p.ID == raftConfig.LocalID
		}) {
			fmt.Printf("Local node ID %s is not part of the cluster, joining as a follower.\n", raftConfig.LocalID)

		}
	} else {
		fmt.Printf("No leader discovered, waiting for %d/%d bootstrap servers...\n", config.ExpectedBootstrap, len(peers))
		orderedPeers := make([]peerServer, len(peers))
		copy(orderedPeers, peers)
		sort.Slice(orderedPeers, func(i, j int) bool {
			return orderedPeers[i].ID < orderedPeers[j].ID
		})
		fmt.Printf("Ordered bootstrap servers: %v\n", orderedPeers)
		peerIndex := slices.IndexFunc(orderedPeers, func(p peerServer) bool {
			return p.ID == raftConfig.LocalID
		})
		if peerIndex == -1 {
			fmt.Printf("Local node ID %s not found in bootstrap servers\n", raftConfig.LocalID)
			return
		}

		initialPeers, others, err := waitForBootstrap(ctx, otherPeers, config.ExpectedBootstrap-1)
		if err != nil {
			fmt.Printf("Failed to wait for bootstrap servers: %v\n", err)
			return
		}
		otherChan = others
		bootStrapDelay := time.Duration(2*(peerIndex-1)+config.BootstrapDelay) * time.Second
		bootStrapTimer := time.NewTimer(bootStrapDelay)
		fmt.Printf("Bootstrap timer set for %v (peer index: %d)\n", bootStrapDelay, peerIndex)
	loop:
		for {

			select {
			case <-bootStrapTimer.C:
				fmt.Printf("Bootstrapping with: %v...\n", initialPeers)

				servers := make([]raft.Server, 0, len(peers)+1)
				servers = append(servers, raft.Server{ID: raftConfig.LocalID, Address: raft.ServerAddress(config.RaftAddr)})
				for _, p := range initialPeers {
					servers = append(servers, raft.Server{ID: p.ID, Address: p.Addr})
				}

				err = r.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
				if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
					fmt.Printf("Failed to bootstrap cluster: %v\n", err)
					return
				}

				fmt.Printf("Cluster bootstrap initiated successfully. %v\n", err)
				break loop
			case peer, ok := <-others:
				if ok {
					initialPeers = append(initialPeers, peer)
					fmt.Printf("Other bootstrap server ready: %s\n", peer)
				}
			case evet := <-eventChan:
				switch e := evet.Data.(type) {
				case raft.RaftState:
					fmt.Printf("Raft state observation: %v\n", e)
				case raft.LeaderObservation:
					if e.LeaderAddr != "" {
						fmt.Printf("Leader elected: %v\n", e)
						break loop
					}
				case raft.PeerObservation:
					fmt.Printf("Peer observation: %v\n", e)
					bootStrapTimer.Stop()
					break loop
				}
			case <-ctx.Done():
				fmt.Printf("Context cancelled while waiting to bootstrap: %v\n", ctx.Err())
				return

			}
		}
		fmt.Printf("Bootstrap completed with %d/%d servers ready.\n", len(initialPeers), config.ExpectedBootstrap)
	}

	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	counter := 0
loop2:
	for {
		select {
		case <-ticker.C:
			if r.State() == raft.Leader {
				Command := Command{Op: "set", Key: "time", Value: fmt.Sprintf("%d", counter)}
				counter++
				if data, err := json.Marshal(Command); err == nil {
					r.Apply(data, time.Second*3).Error()
				}
			}
		case peer, ok := <-otherChan:
			if ok {
				fmt.Printf("Other bootstrap server ready: %s\n", peer)
				if err := r.AddVoter(peer.ID, peer.Addr, 0, 0).Error(); err != nil {
					fmt.Printf("Failed to add voter: %v\n", err)
				} else {
					fmt.Printf("Added voter: %s\n", peer)
				}
			}
		case <-ctx.Done():
			fmt.Printf("Context cancelled while monitoring Raft state: %v\n", ctx.Err())
			break loop2
		}
	}
	fmt.Printf("Raft node started with ID: %s\n", raftConfig.LocalID)
	select {}

}

func waitForBootstrap(
	ctx context.Context,
	servers []peerServer,
	expected int,
) ([]peerServer, <-chan peerServer, error) {

	if expected <= 0 || expected > len(servers) {
		return nil, nil, fmt.Errorf(
			"invalid expected count: %d, total servers: %d",
			expected,
			len(servers),
		)
	}

	readyNodes := make(chan peerServer, len(servers))

	var wg sync.WaitGroup

	for _, server := range servers {
		server := server

		wg.Add(1)

		go func() {
			defer wg.Done()

			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				conn, err := net.DialTimeout("tcp", string(server.Addr), 2*time.Second)

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

	bootstrapServers := make([]peerServer, 0, expected)

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

//go run . --node-id node-1 --raft-addr 127.0.0.1:9001 --http-addr 127.0.0.1:8001 --bootstrap-servers node-1@127.0.0.1:9001@127.0.0.1:8001,node-2@127.0.0.1:9002@127.0.0.1:8002,node-3@127.0.0.1:9003@127.0.0.1:8003,node-4@127.0.0.1:9004@12.0.0.1:8004 --expected-bootstrap 3
