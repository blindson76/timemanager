package main

 

import (

    "bufio"

    "bytes"

    "context"

    "encoding/json"

    "fmt"

    "io"

    "log"

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

    StateWindowing  SystemState = "Windowing"

    StateDone       SystemState = "Done"

    EventReport     SystemEvent = "Report"

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

    Op    CommandType `json:"op"`

    Value string      `json:"value"`

}

 

// FSM Tanımı

type GStateFSM struct {

    mu            sync.Mutex

    machine       *gstate.Machine[SystemState, SystemEvent, SystemStatus]

    actor         *gstate.Actor[SystemState, SystemEvent, SystemStatus]

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

    NodeID   string `json:"node_id"`

    RaftAddr string `json:"raft_addr"`

}

 

var nodeRegistry = map[string]string{

    "node-1": "127.0.0.1:8001",

    "node-2": "127.0.0.1:8002",

    "node-3": "127.0.0.1:8003",

}

 

func main() {

    if len(os.Args) < 5 {

        log.Fatalf("Kullanım: go run main.go <node-id> <raft-port> <http-port> <bootstrap-true/false> [join-http-url]")

    }

 

    nodeID := os.Args[1]

    raftPort := os.Args[2]

    httpPort := os.Args[3]

    bootstrap, _ := strconv.ParseBool(os.Args[4])

 

    raftAddr := fmt.Sprintf("127.0.0.1:%s", raftPort)

 

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

 

    if bootstrap {

        configuration := raft.Configuration{

            Servers: []raft.Server{{ID: config.LocalID, Address: transport.LocalAddr()}},

        }

        r.BootstrapCluster(configuration)

        fmt.Println("🚀 Küme Lider olarak başlatıldı.")

    }

 

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

 

    if len(os.Args) == 6 {

        joinTargetURL := os.Args[5]

        time.Sleep(2 * time.Second)

        reqBody, _ := json.Marshal(JoinRequest{NodeID: nodeID, RaftAddr: raftAddr})

        resp, err := http.Post(joinTargetURL, "application/json", bytes.NewBuffer(reqBody))

        if err != nil || resp.StatusCode != http.StatusOK {

            log.Fatalf("❌ Kümeye katılım sağlanamadı: %v", err)

        }

        fmt.Println("✅ Kümeye başarıyla dahil olundu!")

    }

 

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

 