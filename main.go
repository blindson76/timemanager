package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"example.com/mystate/controllers/mongo"
	"example.com/mystate/controllers/service"
	"example.com/mystate/executor"
	timesm "example.com/mystate/time"
	"github.com/hashicorp/raft"
)

// timesyncController is the controller name of the time state machine.
const timesyncController = "timesync"

// eventRequest is the body of POST /order: an external event for the leader
// to replicate to one controller.
type eventRequest struct {
	Controller string `json:"controller"`
	Event      string `json:"event"`
	Args       any    `json:"args,omitempty"`
}

type clusterState struct {
	State  string       `json:"state"`
	Leader string       `json:"leader"`
	Peers  []peerServer `json:"peers,omitempty"`
}

// serveClusterState exposes this node's raft state over HTTP so peers can
// tell, without any local persistence, whether the cluster is already up.
func serveClusterService(addr string, r *raft.Raft, fsm *executor.Executor, nodeID string) {
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

	mux.HandleFunc("GET /controllers", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fsm.Controllers())
	})

	mux.HandleFunc("GET /controllers/{name}", func(w http.ResponseWriter, req *http.Request) {
		data := fsm.Data(req.PathValue("name"))
		if data == nil {
			http.NotFound(w, req)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	})

	mux.HandleFunc("/join", func(w http.ResponseWriter, req *http.Request) {
		// Handle join requests here
		requestedID := req.URL.Query().Get("id")
		w.Write([]byte("id:" + requestedID))
	})

	mux.HandleFunc("/order", func(w http.ResponseWriter, req *http.Request) {
		_, id := r.LeaderWithID()
		if id == "" {
			http.Error(w, "no leader elected", http.StatusServiceUnavailable)
			return
		}
		if string(id) != nodeID {
			http.Redirect(w, req, "http://"+nodeHttp[string(id)]+"/order", http.StatusTemporaryRedirect)
			return
		}
		var ev eventRequest
		if err := json.NewDecoder(req.Body).Decode(&ev); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if err := fsm.Propose(ev.Controller, ev.Event, ev.Args); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			fmt.Printf("Status server stopped: %v\n", err)
		}
	}()
}

func discoverClusterLeader(ctx context.Context, peers []peerServer, timeout time.Duration) *clusterState {
	if len(peers) == 0 {
		return nil
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultChan := make(chan *clusterState, len(peers))

	var wg sync.WaitGroup

	for _, p := range peers {
		wg.Add(1)
		go func(peer peerServer) {
			defer wg.Done()

			client := &http.Client{Timeout: timeout}
			req, err := http.NewRequestWithContext(cancelCtx, http.MethodGet, "http://"+peer.HTTPAddr+"/state", nil)
			if err != nil {
				return
			}

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var state clusterState
			if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
				return
			}

			if state.Leader != "" {
				select {
				case resultChan <- &state:
				case <-cancelCtx.Done():
				}
			}
		}(p)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for {
		select {
		case leader, ok := <-resultChan:
			if !ok {
				return nil
			}
			return leader
		case <-ctx.Done():
			return nil
		}
	}
}

var (
	nodeHttp map[string]string = make(map[string]string)
)

// go run . --node-id node-1
func main() {

	ctx := context.Background()
	config, err := parseConfig()
	if err != nil {
		fmt.Printf("Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	peers := config.Peers
	for _, p := range peers {
		nodeHttp[string(p.ID)] = p.HTTPAddr
	}
	fmt.Printf("Starting node with config: %+v\n", config)

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

	timeMachine := timesm.BuildMachine()
	ctrls := []executor.Controller{
		executor.NewController(timesyncController, timeMachine, timesm.InitialStatus()).
			WithHealth(timesm.Health).
			WithActorOptions(timeMachine.WithObservers(&timesm.LogObserver{})),
	}
	for _, def := range config.Services {
		if def.Mongo != nil {
			ctrls = append(ctrls, mongo.New(*def.Mongo))
		} else {
			ctrls = append(ctrls, service.New(*def.Service))
		}
	}
	fsm, err := executor.New(ctrls...)
	if err != nil {
		fmt.Printf("Failed to create executor: %v\n", err)
		return
	}
	r, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		fmt.Printf("Failed to create Raft node: %v\n", err)
		return
	}
	fsm.Attach(r)
	var otherChan <-chan peerServer

	eventChan := make(chan raft.Observation, 50)
	observer := raft.NewObserver(eventChan, true, func(o *raft.Observation) bool {
		return true
	})
	r.RegisterObserver(observer)
	serveClusterService(config.HttpAddr, r, fsm, config.NodeID)
	otherPeers := make([]peerServer, len(peers))
	copy(otherPeers, peers)
	otherPeers = slices.DeleteFunc(otherPeers, func(p peerServer) bool {
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
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-eventChan:
				switch ev := e.Data.(type) {
				case raft.LeaderObservation:
					leader := r.State() == raft.Leader
					if leader {
						fmt.Printf("I am the leader now: %v\n", ev)
					}
					fsm.SetLeader(leader)
				}
			}
		}
	}()

	ticker := time.NewTicker(2000 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		cmd := eventRequest{Controller: timesyncController, Event: string(timesm.EventReport), Args: timesm.TimeReport{
			PeerID:            config.NodeID,
			OrderNumber:       1,
			TimeMode:          timesm.TimeModeAuto,
			AvailableServsers: []string{"server-1", "server-2"},
		}}
		data, err := json.Marshal(cmd)
		if err != nil {
			fmt.Printf("Failed to marshal report: %v\n", err)
			return
		}

		for {
			_, leader := r.LeaderWithID()
			if leader == "" {
				time.Sleep(time.Second * 2)
				continue
			}
			leaderHttp := nodeHttp[string(leader)]
			select {
			case <-ctx.Done():
				return
			default:
				resp, err := http.Post("http://"+leaderHttp+"/order", "application/json", bytes.NewReader(data))
				if err != nil {
					fmt.Printf("Failed to post to leader: %v\n", err)
					continue
				}
				defer resp.Body.Close()

				if resp.StatusCode/100 != 2 {
					b, _ := io.ReadAll(resp.Body)
					fmt.Printf("Leader response error: %s %s\n", resp.Status, string(b))
					continue
				}
				return
			}
		}
	}()

loop2:

	for {
		select {
		case peer, ok := <-otherChan:
			if ok && r.State() == raft.Leader {
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

// go run . --node-id node-1
