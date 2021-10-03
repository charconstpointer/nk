package main

import (
	"io"
	"log"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

func main() {
	n := 3 // Number of nodes
	for i := 0; i < n; i++ {
	}

	// Create raft dependencies
	configs := newConfigs(n)
	stores := newStores(n)
	snapshots := newSnapshotStores(n)
	transports := newTranports(n)

	// Bootstrap initial configuration
	configuration := newConfiguration(n)
	for i := 0; i < n; i++ {
		err := raft.BootstrapCluster(
			configs[i],
			stores[i],
			stores[i],
			snapshots[i],
			transports[i],
			configuration,
		)
		if err != nil {
			log.Fatalf("bootstrap error: %v", err)
		}
	}

	// Setup notification channels
	notifyChs := make([]chan bool, n)
	for i := 0; i < n; i++ {
		notifyChs[i] = make(chan bool, 0)
		configs[i].NotifyCh = notifyChs[i]
	}

	// Start raft instances.
	fsms := newFSMs(n)
	rafts := make([]*raft.Raft, n)
	for i := 0; i < n; i++ {
		raft, err := raft.NewRaft(
			configs[i],
			fsms[i],
			stores[i],
			stores[i],
			snapshots[i],
			transports[i],
		)
		if err != nil {
			log.Fatalf("start error: %v", err)
		}
		rafts[i] = raft
	}

	// Wait for a leader to be elected.
	var i int
	select {
	case <-notifyChs[0]:
		i = 0
	case <-notifyChs[1]:
		i = 1
	case <-notifyChs[2]:
		i = 2
	}
	if rafts[i].State() != raft.Leader {
		log.Fatal("notified channel triggered even if not is not leader")
	}

	err := rafts[i].Apply([]byte("hello"), time.Second).Error()
	if err != nil {
		log.Fatal(err.Error())
	}
	time.Sleep(time.Second * 3)
	err = rafts[i].Apply([]byte("w"), time.Second).Error()
	if err != nil {
		log.Fatal(err.Error())
	}
	time.Sleep(time.Second * 10)
}

// Create a new set of in-memory configs.
func newConfigs(n int) []*raft.Config {
	configs := make([]*raft.Config, n)
	for i := 0; i < n; i++ {
		config := raft.DefaultConfig()

		// Set low timeouts
		config.LocalID = raft.ServerID(strconv.Itoa(i))
		config.HeartbeatTimeout = 20 * time.Millisecond
		config.ElectionTimeout = 20 * time.Millisecond
		config.CommitTimeout = 1 * time.Millisecond
		config.LeaderLeaseTimeout = 10 * time.Millisecond

		configs[i] = config
	}
	return configs
}

// Create a new set of dummy FSMs.
func newFSMs(n int) []raft.FSM {
	fsms := make([]raft.FSM, n)
	for i := 0; i < n; i++ {
		fsms[i] = &FSM{
			ID: i,
		}
	}
	return fsms
}

// Create a new set of in-memory log/stable stores.
func newStores(n int) []*raft.InmemStore {
	stores := make([]*raft.InmemStore, n)
	for i := 0; i < n; i++ {
		stores[i] = raft.NewInmemStore()
	}
	return stores
}

// Create a new set of in-memory snapshot stores.
func newSnapshotStores(n int) []raft.SnapshotStore {
	stores := make([]raft.SnapshotStore, n)
	for i := 0; i < n; i++ {
		stores[i] = raft.NewInmemSnapshotStore()
	}
	return stores
}

// Create a new set of in-memory transports, all connected to each other.
func newTranports(n int) []raft.LoopbackTransport {
	transports := make([]raft.LoopbackTransport, n)
	for i := 0; i < n; i++ {
		addr := raft.ServerAddress(strconv.Itoa(i))
		_, transports[i] = raft.NewInmemTransport(addr)
	}

	for _, t1 := range transports {
		for _, t2 := range transports {
			if t2 == t1 {
				continue
			}
			t1.Connect(t2.LocalAddr(), t2)
		}
	}
	return transports
}

// Create a new raft bootstrap configuration containing all nodes.
func newConfiguration(n int) raft.Configuration {
	servers := make([]raft.Server, n)
	for i := 0; i < n; i++ {
		addr := strconv.Itoa(i)
		servers[i] = raft.Server{
			ID:      raft.ServerID(addr),
			Address: raft.ServerAddress(addr),
		}
	}

	configuration := raft.Configuration{}
	configuration.Servers = servers

	return configuration

}

// Dummy FSM
type FSM struct {
	ID     int
	myData []string
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	f.myData = append(f.myData, string(l.Data))
	log.Printf("DATA [%d]: %s", f.ID, f.myData)
	log.Printf("LOG [%d]: %s", f.ID, string(l.Data))
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return &FSMSnapshot{}, nil }

func (f *FSM) Restore(io.ReadCloser) error { return nil }

type FSMSnapshot struct{}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (s *FSMSnapshot) Release() {}
