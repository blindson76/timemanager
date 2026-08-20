package main

import (
	"bufio"

	"bytes"

	"context"

	"encoding/json"

	"fmt"

	"io"

	"log"

	"net"

	"net/http"

	"os"

	"strconv"

	"strings"

	"sync"

	"time"

	"github.com/floodfx/gstate"

	"github.com/hashicorp/raft"
)

// Tip tanımlamaları ve modeller öncekiyle aynı

type SystemState string

type SystemEvent string

const (
	StateProcessing SystemState = "Processing"

	StateCollecting SystemState = "Collecting"

	StateWindowing SystemState = "Windowing"

	StateDone SystemState = "Done"

	EventReport SystemEvent = "Report"
)

type SystemStatus struct {
	Reports []string `json:"reports"`
}

func (s SystemStatus) Clone() SystemStatus {

	reports := make([]string, len(s.Reports))

	copy(reports, s.Reports)

	return SystemStatus{Reports: reports}

}

type CommandType string

const CommandSendReport CommandType = "SendReport"

type LogCommand struct {
	Op CommandType `json:"op"`

	Value string `json:"value"`
}

// FSM Tanımı

type GStateFSM struct {
	mu sync.Mutex

	machine *gstate.Machine[SystemState, SystemEvent, SystemStatus]

	actor *gstate.Actor[SystemState, SystemEvent, SystemStatus]

	pendingReport string
}

func NewGStateFSM() *GStateFSM {

	fsm := &GStateFSM{}

	builder := gstate.New[SystemState, SystemEvent, SystemStatus]("distributed-test").
		Initial(StateProcessing).
		State(StateProcessing, func(sb *gstate.StateBuilder[SystemState, SystemEvent, SystemStatus]) {

			sb.Initial(StateCollecting)

			sb.On(EventReport).Assign(func(ss SystemStatus) SystemStatus {

				if fsm.pendingReport != "" {

					ss.Reports = append(ss.Reports, fsm.pendingReport)

					fmt.Printf("📝 [gstate Assign] Rapor eklendi: %s\n", fsm.pendingReport)

					fsm.pendingReport = ""

				}

				return ss

			})

			sb.State(StateCollecting, func(sb *gstate.StateBuilder[SystemState, SystemEvent, SystemStatus]) {

				sb.Always().Guard(func(ss SystemStatus) bool { return len(ss.Reports) >= 3 }).GoTo(StateWindowing)

			})

			sb.State(StateWindowing, func(sb *gstate.StateBuilder[SystemState, SystemEvent, SystemStatus]) {

				sb.After(5 * time.Second).GoTo(StateDone)

			})

			sb.Always().Guard(func(ss SystemStatus) bool { return len(ss.Reports) >= 6 }).GoTo(StateDone)

		}).
		State(StateDone, func(sb *gstate.StateBuilder[SystemState, SystemEvent, SystemStatus]) {

			sb.Type(gstate.Final)

			sb.Entry(func(ss SystemStatus) SystemStatus {

				fmt.Printf("\n🎉 [FSM] Final duruma ulaşıldı: %+v\n", ss)

				return ss

			})

		})

	fsm.machine = builder.Build()

	fsm.actor = gstate.Start(fsm.machine, SystemStatus{}, fsm.machine.WithObservers(&loggingObs{}))

	return fsm

}

type loggingObs struct {
	gstate.BaseObserver[SystemState, SystemEvent, SystemStatus]
}

func (l *loggingObs) OnTransition(ctx context.Context, e *gstate.TransitionEvent[SystemState, SystemEvent, SystemStatus]) {

	log.Printf("🔄 [%s] %s --%s--> %s | %+v", e.ActorID, e.From, e.Event, e.To, e.Data().Reports)

}

func (f *GStateFSM) Apply(l *raft.Log) interface{} {

	f.mu.Lock()

	defer f.mu.Unlock()

	var cmd LogCommand

	if err := json.Unmarshal(l.Data, &cmd); err != nil {

		return err

	}

	if cmd.Op == CommandSendReport {

		f.pendingReport = cmd.Value

		f.actor.Send(EventReport)

	}

	return nil

}

func (f *GStateFSM) Snapshot() (raft.FSMSnapshot, error) {

	f.mu.Lock()

	defer f.mu.Unlock()

	data, _ := json.Marshal(f.actor.Snapshot())

	return &GStateSnapshot{data: data}, nil

}

func (f *GStateFSM) Restore(rc io.ReadCloser) error {

	f.mu.Lock()

	defer f.mu.Unlock()

	defer rc.Close()

	var loaded gstate.Snapshot[SystemState, SystemStatus]

	if err := json.NewDecoder(rc).Decode(&loaded); err != nil {

		return err

	}

	f.actor.Stop()

	f.actor = gstate.Hydrate(f.machine, loaded, f.machine.WithObservers(&loggingObs{}))

	fmt.Println("♻️ [FSM] Durum başarıyla Raft snapshot'ından geri yüklendi.")

	return nil

}

type GStateSnapshot struct{ data []byte }

func (s *GStateSnapshot) Persist(sink raft.SnapshotSink) error {

	_, err := sink.Write(s.data)

	sink.Close()

	return err

}

func (s *GStateSnapshot) Release() {}

type JoinRequest struct {
	NodeID string `json:"node_id"`

	RaftAddr string `json:"raft_addr"`
}

type ClusterMember struct {
	NodeID string

	RaftAddr string
}

type AppConfig struct {
	NodeID string

	RaftPort string

	HTTPPort string

	BootstrapExpected int

	BootstrapDelay time.Duration

	ClusterMembers []ClusterMember
}

var nodeRegistry map[string]string

func usage() string {

	return "Usage: go run main.go --node-name node-1 --raft-port 5001 --http-port 8001 --bootstrap-expected 3 --bootstrap-delay 5s --cluster-members 127.0.0.1:5001 127.0.0.1:5002 127.0.0.1:5003"

}

func parseAppConfig(args []string) (AppConfig, error) {

	cfg := AppConfig{BootstrapDelay: 5 * time.Second}

	for i := 0; i < len(args); i++ {

		switch args[i] {

		case "--node-name", "-node-name":

			i++

			if i >= len(args) {

				return cfg, fmt.Errorf("%s değeri eksik", args[i-1])

			}

			cfg.NodeID = args[i]

		case "--raft-port", "-raft-port":

			i++

			if i >= len(args) {

				return cfg, fmt.Errorf("%s değeri eksik", args[i-1])

			}

			cfg.RaftPort = args[i]

		case "--http-port", "-http-port", "http-port":

			i++

			if i >= len(args) {

				return cfg, fmt.Errorf("%s değeri eksik", args[i-1])

			}

			cfg.HTTPPort = args[i]

		case "--bootstrap-expected", "-bootstrap-expected":

			i++

			if i >= len(args) {

				return cfg, fmt.Errorf("%s deÄŸeri eksik", args[i-1])

			}

			expected, err := strconv.Atoi(args[i])

			if err != nil {

				return cfg, fmt.Errorf("gecersiz bootstrap expected %q: %w", args[i], err)

			}

			cfg.BootstrapExpected = expected

		case "--bootstrap-delay", "-bootstrap-delay", "--bootstrap-time", "-bootstrap-time":

			i++

			if i >= len(args) {

				return cfg, fmt.Errorf("%s deÄŸeri eksik", args[i-1])

			}

			delay, err := time.ParseDuration(args[i])

			if err != nil {

				return cfg, fmt.Errorf("gecersiz bootstrap delay %q: %w", args[i], err)

			}

			cfg.BootstrapDelay = delay

		case "--cluster-members", "-cluster-members":

			i++

			if i >= len(args) {

				return cfg, fmt.Errorf("%s değeri eksik", args[i-1])

			}

			for i < len(args) && !strings.HasPrefix(args[i], "-") {

				for _, rawAddr := range strings.Split(args[i], ",") {

					addr := strings.TrimSpace(rawAddr)

					if addr == "" {

						continue

					}

					cfg.ClusterMembers = append(cfg.ClusterMembers, ClusterMember{

						NodeID: fmt.Sprintf("node-%d", len(cfg.ClusterMembers)+1),

						RaftAddr: addr,
					})

				}

				i++

			}

			i--

		default:

			return cfg, fmt.Errorf("bilinmeyen argüman: %s", args[i])

		}

	}

	if cfg.NodeID == "" || cfg.RaftPort == "" || cfg.HTTPPort == "" || len(cfg.ClusterMembers) == 0 {

		return cfg, fmt.Errorf("eksik argüman")

	}

	if cfg.BootstrapExpected == 0 {

		cfg.BootstrapExpected = len(cfg.ClusterMembers)

	}

	if cfg.BootstrapExpected < 1 || cfg.BootstrapExpected > len(cfg.ClusterMembers) {

		return cfg, fmt.Errorf("bootstrap expected %d, 1 ile cluster member sayisi %d arasinda olmali", cfg.BootstrapExpected, len(cfg.ClusterMembers))

	}

	if cfg.BootstrapDelay < 0 {

		return cfg, fmt.Errorf("bootstrap delay negatif olamaz")

	}

	if _, err := strconv.Atoi(cfg.RaftPort); err != nil {

		return cfg, fmt.Errorf("geçersiz raft portu %q: %w", cfg.RaftPort, err)

	}

	if _, err := strconv.Atoi(cfg.HTTPPort); err != nil {

		return cfg, fmt.Errorf("geçersiz HTTP portu %q: %w", cfg.HTTPPort, err)

	}

	for _, member := range cfg.ClusterMembers {

		if _, _, err := net.SplitHostPort(member.RaftAddr); err != nil {

			return cfg, fmt.Errorf("geçersiz cluster member adresi %q: %w", member.RaftAddr, err)

		}

	}

	nodeExists := false
	nodeRaftAddr := ""
	for _, member := range cfg.ClusterMembers {
		if member.NodeID == cfg.NodeID {
			nodeExists = true
			nodeRaftAddr = member.RaftAddr
			break
		}
	}
	if !nodeExists {
		return cfg, fmt.Errorf("node name %q cluster members icinde yok; adresler sirayla node-1, node-2 olarak adlandirilir", cfg.NodeID)
	}
	_, nodeRaftPort, err := net.SplitHostPort(nodeRaftAddr)
	if err != nil {
		return cfg, err
	}
	if nodeRaftPort != cfg.RaftPort {
		return cfg, fmt.Errorf("node name %q icin raft port %s bekleniyor, %s verildi", cfg.NodeID, nodeRaftPort, cfg.RaftPort)
	}

	return cfg, nil

}

func buildHTTPRegistry(members []ClusterMember, raftPort, httpPort string) (map[string]string, error) {

	raftPortNum, err := strconv.Atoi(raftPort)

	if err != nil {

		return nil, err

	}

	httpPortNum, err := strconv.Atoi(httpPort)

	if err != nil {

		return nil, err

	}

	portOffset := httpPortNum - raftPortNum

	registry := make(map[string]string, len(members))

	for _, member := range members {

		host, port, err := net.SplitHostPort(member.RaftAddr)

		if err != nil {

			return nil, err

		}

		raftMemberPort, err := strconv.Atoi(port)

		if err != nil {

			return nil, err

		}

		registry[member.NodeID] = net.JoinHostPort(host, strconv.Itoa(raftMemberPort+portOffset))

	}

	return registry, nil

}

func raftAddrForNode(members []ClusterMember, nodeID string) (string, bool) {

	for _, member := range members {

		if member.NodeID == nodeID {

			return member.RaftAddr, true

		}

	}

	return "", false

}

func countAvailableMembers(members []ClusterMember, localRaftAddr string, timeout time.Duration) int {

	available := 0

	for _, member := range members {

		if member.RaftAddr == localRaftAddr {

			available++

			continue

		}

		conn, err := net.DialTimeout("tcp", member.RaftAddr, timeout)

		if err != nil {

			continue

		}

		conn.Close()

		available++

	}

	return available

}

func waitForBootstrapQuorum(members []ClusterMember, localRaftAddr string, expected int, delay time.Duration) {

	for {

		available := countAvailableMembers(members, localRaftAddr, 500*time.Millisecond)

		if available < expected {

			log.Printf("[Bootstrap] %d/%d node reachable; waiting for bootstrap expected %d", available, len(members), expected)

			time.Sleep(time.Second)

			continue

		}

		log.Printf("[Bootstrap] bootstrap expected reached: %d/%d node reachable; waiting %s before bootstrapping", available, len(members), delay)

		time.Sleep(delay)

		available = countAvailableMembers(members, localRaftAddr, 500*time.Millisecond)

		if available >= expected {

			log.Printf("[Bootstrap] bootstrap delay completed with %d/%d node reachable", available, len(members))

			return

		}

		log.Printf("[Bootstrap] node availability dropped to %d/%d during bootstrap delay; waiting again", available, len(members))

	}

}

func bootstrapCluster(r *raft.Raft, servers []raft.Server) error {

	err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error()

	if err == nil {

		log.Printf("[Bootstrap] cluster initialized")

		return nil

	}

	if strings.Contains(err.Error(), "bootstrap only works on new clusters") {

		log.Printf("[Bootstrap] cluster already initialized by another node")

		return nil

	}

	return err

}

func main() {

	appConfig, err := parseAppConfig(os.Args[1:])

	if err != nil {

		log.Fatalf("%v\n%s", err, usage())

	}

	nodeID := appConfig.NodeID

	raftPort := appConfig.RaftPort

	httpPort := appConfig.HTTPPort

	nodeRegistry, err = buildHTTPRegistry(appConfig.ClusterMembers, raftPort, httpPort)

	if err != nil {

		log.Fatalf("HTTP registry oluşturulamadı: %v", err)

	}

	raftAddr, ok := raftAddrForNode(appConfig.ClusterMembers, nodeID)

	if !ok {

		log.Fatalf("Node adresi bulunamadÄ±: %s", nodeID)

	}

	config := raft.DefaultConfig()

	config.LocalID = raft.ServerID(nodeID)

	config.LogOutput = io.Discard

	fsm := NewGStateFSM()

	store := raft.NewInmemStore()

	snapshotStore := raft.NewInmemSnapshotStore()

	transport, err := raft.NewTCPTransport(raftAddr, nil, 3, 10*time.Second, io.Discard)

	if err != nil {

		log.Fatalf("Transport hatası: %v", err)

	}

	r, err := raft.NewRaft(config, fsm, store, store, snapshotStore, transport)

	if err != nil {

		log.Fatalf("Raft başlatılamadı: %v", err)

	}

	raftServers := make([]raft.Server, 0, len(appConfig.ClusterMembers))

	for _, member := range appConfig.ClusterMembers {

		raftServers = append(raftServers, raft.Server{

			ID: raft.ServerID(member.NodeID),

			Address: raft.ServerAddress(member.RaftAddr),
		})

	}

	waitForBootstrapQuorum(appConfig.ClusterMembers, raftAddr, appConfig.BootstrapExpected, appConfig.BootstrapDelay)

	if err := bootstrapCluster(r, raftServers); err != nil {

		log.Fatalf("Kume baslatilamadi: %v", err)

	}

	fmt.Printf("Kume otomatik yapilandirildi: %+v\n", raftServers)
	forwardToLeader := func(w http.ResponseWriter, req *http.Request, bodyBytes []byte) bool {

		if r.State() == raft.Leader {

			return false

		}

		_, leaderID := r.LeaderWithID()

		if leaderID == "" {

			http.Error(w, "Kümede şu an aktif bir lider bulunamadı.", http.StatusServiceUnavailable)

			return true

		}

		leaderHTTPAddr, exists := nodeRegistry[string(leaderID)]

		if !exists {

			http.Error(w, "Liderin HTTP adresi bulunamadı.", http.StatusServiceUnavailable)

			return true

		}

		leaderURL := fmt.Sprintf("http://%s%s", leaderHTTPAddr, req.URL.Path)

		log.Printf("🔀 [Forwarding] İstek Lidere Yönlendiriliyor -> %s", leaderURL)

		proxyReq, err := http.NewRequest(req.Method, leaderURL, bytes.NewBuffer(bodyBytes))

		if err != nil {

			http.Error(w, err.Error(), http.StatusInternalServerError)

			return true

		}

		proxyReq.Header = req.Header

		client := &http.Client{Timeout: 5 * time.Second}

		resp, err := client.Do(proxyReq)

		if err != nil {

			http.Error(w, fmt.Sprintf("Lidere yönlendirme hatası: %v", err), http.StatusBadGateway)

			return true

		}

		defer resp.Body.Close()

		w.WriteHeader(resp.StatusCode)

		io.Copy(w, resp.Body)

		return true

	}

	http.HandleFunc("/join", func(w http.ResponseWriter, req *http.Request) {

		bodyBytes, _ := io.ReadAll(req.Body)

		if forwardToLeader(w, req, bodyBytes) {

			return

		}

		var joinReq JoinRequest

		if err := json.Unmarshal(bodyBytes, &joinReq); err != nil {

			http.Error(w, err.Error(), http.StatusBadRequest)

			return

		}

		log.Printf("🔗 [LIDER] Yeni düğüm ekleniyor -> ID: %s, Adres: %s", joinReq.NodeID, joinReq.RaftAddr)

		future := r.AddVoter(raft.ServerID(joinReq.NodeID), raft.ServerAddress(joinReq.RaftAddr), 0, 0)

		if err := future.Error(); err != nil {

			http.Error(w, fmt.Sprintf("Node eklenemedi: %v", err), http.StatusInternalServerError)

			return

		}

		w.Write([]byte("Başarıyla eklendi"))

	})

	http.HandleFunc("/report", func(w http.ResponseWriter, req *http.Request) {

		bodyBytes, _ := io.ReadAll(req.Body)

		if forwardToLeader(w, req, bodyBytes) {

			return

		}

		var reportCmd struct {
			Value string `json:"value"`
		}

		if err := json.Unmarshal(bodyBytes, &reportCmd); err != nil {

			http.Error(w, err.Error(), http.StatusBadRequest)

			return

		}

		cmd := LogCommand{Op: CommandSendReport, Value: reportCmd.Value}

		data, _ := json.Marshal(cmd)

		applyFuture := r.Apply(data, 5*time.Second)

		if err := applyFuture.Error(); err != nil {

			http.Error(w, err.Error(), http.StatusInternalServerError)

			return

		}

		w.Write([]byte("Rapor işlendi."))

	})

	go func() {

		log.Printf("🌐 HTTP Sunucu Başlatıldı: %s", httpPort)

		if err := http.ListenAndServe("127.0.0.1:"+httpPort, nil); err != nil {

			log.Fatalf("HTTP sunucu hatası: %v", err)

		}

	}()

	// 💡 YENİ: AÇILIŞTA OTOMATİK RAPOR YAYINLAMA RUTİNİ

	go func() {

		// Düğümün tamamen ayağa kalkması ve (eğer follower ise) kümeye bağlanması için biraz bekliyoruz

		time.Sleep(4 * time.Second)

		myReportValue := fmt.Sprintf("Startup-Report-From-%s", nodeID)

		payload, _ := json.Marshal(map[string]string{"value": myReportValue})

		// Kendi üzerimizdeki yerel HTTP endpoint'e istek atıyoruz.

		// Eğer follower isek, yazdığımız HTTP katmanı bunu otomatik olarak Lidere forward edecektir.

		localURL := fmt.Sprintf("http://127.0.0.1:%s/report", httpPort)

		log.Printf("🤖 [Auto-Startup] Otomatik rapor gönderiliyor: %s", myReportValue)

		resp, err := http.Post(localURL, "application/json", bytes.NewBuffer(payload))

		if err != nil {

			log.Printf("❌ [Auto-Startup] Rapor gönderme hatası: %v", err)

			return

		}

		defer resp.Body.Close()

		log.Printf("✅ [Auto-Startup] Rapor başarıyla küme kuyruğuna iletildi. Durum kodu: %d", resp.StatusCode)

	}()

	// CLI İzleme

	go func() {

		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {

			if strings.TrimSpace(scanner.Text()) == "status" {

				fsm.mu.Lock()

				fmt.Printf("📊 [%s - %s] Durum: %v | Raporlar: %+v\n", nodeID, r.State(), fsm.actor.States(), fsm.actor.Snapshot().Data.Reports)

				fsm.mu.Unlock()

			}

		}

	}()

	select {}

}
