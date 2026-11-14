package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/broker"
	"github.com/ARIN00-web/messlessmq/internal/config"
	"github.com/ARIN00-web/messlessmq/internal/controller"
	"github.com/ARIN00-web/messlessmq/internal/group"
	"github.com/ARIN00-web/messlessmq/internal/interbroker"
	"github.com/ARIN00-web/messlessmq/internal/membership"
	"github.com/ARIN00-web/messlessmq/internal/network"
	"github.com/ARIN00-web/messlessmq/internal/observability"
	"github.com/ARIN00-web/messlessmq/internal/offsets"
	"github.com/ARIN00-web/messlessmq/internal/operations"
	"github.com/ARIN00-web/messlessmq/internal/protocol"
	"github.com/ARIN00-web/messlessmq/internal/replication"
	"github.com/ARIN00-web/messlessmq/internal/security"
	"github.com/ARIN00-web/messlessmq/internal/topic"
)

const defaultSegmentSize int64 = 64 * 1024 * 1024

type Options struct {
	Config          config.Config
	BootstrapTopics []TopicDefinition
	AdminAddress    string
	Logger          *slog.Logger
}

type Server struct {
	cfg                config.Config
	adminAddr          string
	logger             *slog.Logger
	registry           *topic.Registry
	topics             []*topic.Topic
	dispatch           *broker.Dispatcher
	controls           *operations.Controls
	limiter            *security.RateLimiter
	metrics            *observability.Registry
	requests           *observability.Counter
	failures           *observability.Counter
	produced           *observability.Counter
	consumed           *observability.Counter
	latency            *observability.Histogram
	members            *membership.Registry
	seeds              []membership.Seed
	assignmentStore    *assignmentRegistry
	desiredAssignments map[partitionKey]partitionReplicaAssignment
	replicator         *distributedAppender
	commits            *replication.CommitStore
	peerTransport      *interbroker.Transport
	controllerNode     *controller.DurableNode
	controllerRuntime  *controllerRuntime
	groupCoordinator   *group.Coordinator
	offsetStore        *offsets.Store
	clientKeys         *security.KeyStore
	clientAuthorizer   security.Authorizer
	health             *observability.HealthChecker
	diagnostics        *observability.Diagnostics

	failoverMu        sync.Mutex
	partitionFailures map[partitionKey]partitionFailureState
	isrMu             sync.Mutex
	replicaHealth     map[replicaHealthKey]replicaHealthState
	serveOnce         sync.Once
}

func New(options Options) (*Server, error) {
	cfg := options.Config
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate broker configuration: %w", err)
	}
	if options.Logger == nil {
		options.Logger = observability.NewLogger(os.Stdout, observability.ParseLogLevel(cfg.LogLevel), "broker")
	}
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	if cfg.PeerListenAddress != "" {
		if err := membership.EnsureIdentity(cfg.DataDir, cfg.ClusterID, cfg.BrokerID); err != nil {
			return nil, fmt.Errorf("verify persistent broker identity: %w", err)
		}
	}
	definitions, err := loadTopicCatalog(cfg.DataDir, options.BootstrapTopics)
	if err != nil {
		return nil, err
	}
	configuredAssignments, err := parseReplicaAssignments(cfg.ReplicaAssignments)
	if err != nil {
		return nil, fmt.Errorf("parse replica assignments: %w", err)
	}
	assignments, err := buildControllerAssignments(definitions, configuredAssignments, cfg.ControllerVoters, cfg.MinISR)
	if err != nil {
		return nil, fmt.Errorf("build controller partition assignments: %w", err)
	}
	if err := validateReplicaAssignments(definitions, assignments, cfg.MinISR); err != nil {
		return nil, fmt.Errorf("validate replica assignments: %w", err)
	}
	registry := topic.NewRegistry()
	opened := make([]*topic.Topic, 0, len(definitions))
	for _, definition := range definitions {
		registeredTopic, err := registry.CreateWithPartitionCount(definition.Name, definition.Partitions)
		if err != nil {
			_ = closeTopics(opened)
			return nil, err
		}
		if err := registeredTopic.OpenLogs(filepath.Join(cfg.DataDir, "logs"), defaultSegmentSize); err != nil {
			_ = closeTopics(opened)
			return nil, fmt.Errorf("open topic %q storage: %w", definition.Name, err)
		}
		opened = append(opened, registeredTopic)
	}

	commits, err := replication.OpenCommitStore(filepath.Join(cfg.DataDir, "metadata", "commits"))
	if err != nil {
		_ = closeTopics(opened)
		return nil, fmt.Errorf("open replication commit store: %w", err)
	}
	for _, registeredTopic := range opened {
		for _, partition := range registeredTopic.Partitions() {
			startOffset, logEnd, rangeErr := partition.OffsetRange()
			if rangeErr != nil {
				_ = closeTopics(opened)
				return nil, fmt.Errorf("read initial range for %s:%d: %w", registeredTopic.Name(), partition.ID(), rangeErr)
			}
			leaderID, leaderEpoch, initialHighWatermark := cfg.BrokerID, uint64(1), logEnd
			if assignment, assigned := assignments[partitionKey{Topic: registeredTopic.Name(), PartitionID: partition.ID()}]; assigned {
				leaderID, leaderEpoch, initialHighWatermark = assignment.LeaderID, assignment.LeaderEpoch, startOffset
			}
			state, initErr := commits.Initialize(registeredTopic.Name(), partition.ID(), initialHighWatermark, logEnd, leaderID, leaderEpoch)
			if initErr != nil {
				_ = closeTopics(opened)
				return nil, fmt.Errorf("initialize commit state for %s:%d: %w", registeredTopic.Name(), partition.ID(), initErr)
			}
			if state.HighWatermark < startOffset {
				if _, advanceErr := commits.Advance(registeredTopic.Name(), partition.ID(), startOffset, leaderID, leaderEpoch, logEnd); advanceErr != nil {
					_ = closeTopics(opened)
					return nil, fmt.Errorf("advance commit state after retention for %s:%d: %w", registeredTopic.Name(), partition.ID(), advanceErr)
				}
			}
		}
	}

	metrics := observability.NewRegistry()
	requests, err := metrics.NewCounter("messlessmq_requests_total", "Total client requests dispatched")
	if err != nil {
		_ = closeTopics(opened)
		return nil, err
	}
	failures, err := metrics.NewCounter("messlessmq_request_errors_total", "Total client requests that returned an error")
	if err != nil {
		_ = closeTopics(opened)
		return nil, err
	}
	produced, err := metrics.NewCounter("messlessmq_produced_messages_total", "Total messages successfully appended")
	if err != nil {
		_ = closeTopics(opened)
		return nil, err
	}
	consumed, err := metrics.NewCounter("messlessmq_consume_requests_total", "Total successful consume requests")
	if err != nil {
		_ = closeTopics(opened)
		return nil, err
	}
	latency, err := metrics.NewHistogram("messlessmq_request_duration_seconds", "Request dispatch duration in seconds", []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5})
	if err != nil {
		_ = closeTopics(opened)
		return nil, err
	}
	limiter, err := security.NewRateLimiter(cfg.RateLimitPerSecond, cfg.RateLimitBurst, cfg.MaxConnections)
	if err != nil {
		_ = closeTopics(opened)
		return nil, err
	}

	var members *membership.Registry
	var seeds []membership.Seed
	peerTransport := interbroker.NewTransport(5 * time.Second)
	if cfg.PeerListenAddress != "" {
		members, err = membership.NewRegistry(cfg.ClusterID, membership.Broker{
			ID: cfg.BrokerID, ClientAddress: cfg.AdvertiseAddress, PeerAddress: cfg.PeerAdvertiseAddress,
		})
		if err != nil {
			_ = closeTopics(opened)
			return nil, fmt.Errorf("initialize broker membership: %w", err)
		}
		seeds, err = membership.ParseSeeds(cfg.SeedPeers)
		if err != nil {
			_ = closeTopics(opened)
			return nil, err
		}
		if cfg.PeerTLSCertFile != "" {
			peerTLS, tlsErr := security.LoadClientTLSWithCertificate(cfg.PeerTLSCAFile, "", cfg.PeerTLSCertFile, cfg.PeerTLSKeyFile)
			if tlsErr != nil {
				_ = closeTopics(opened)
				return nil, fmt.Errorf("configure peer TLS client: %w", tlsErr)
			}
			peerTransport = interbroker.NewTLSTransport(5*time.Second, peerTLS)
		}
	}

	var durableController *controller.DurableNode
	var controllerRuntimeInstance *controllerRuntime
	if len(cfg.ControllerVoters) > 0 {
		if members == nil {
			_ = closeTopics(opened)
			return nil, errors.New("controller voters require cluster membership")
		}
		peers := make([]string, 0, len(cfg.ControllerVoters)-1)
		for _, voter := range cfg.ControllerVoters {
			if voter != cfg.BrokerID {
				peers = append(peers, voter)
			}
		}
		durableController, err = controller.OpenDurableNode(cfg.BrokerID, peers, filepath.Join(cfg.DataDir, "metadata", "controller"))
		if err != nil {
			_ = closeTopics(opened)
			return nil, fmt.Errorf("open durable controller state: %w", err)
		}
		controllerRuntimeInstance, err = newControllerRuntime(cfg.BrokerID, cfg.ControllerVoters, durableController, members, cfg, options.Logger)
		if err != nil {
			_ = closeTopics(opened)
			return nil, fmt.Errorf("initialize controller runtime: %w", err)
		}
		controllerRuntimeInstance.transport = peerTransport
	}

	dispatcher := broker.NewDispatcher(registry)
	assignmentStore := newAssignmentRegistry(assignments)
	controllerEnabled := len(cfg.ControllerVoters) > 0
	if controllerEnabled {
		// In controller mode no partition assignment is authoritative until its
		// command has been committed and applied by this broker's Raft node.
		assignmentStore.replace(nil)
	}
	var hasControllerAuthority func() bool
	if controllerRuntimeInstance != nil {
		hasControllerAuthority = controllerRuntimeInstance.HasAuthority
	}
	distributed := newDistributedAppender(cfg.BrokerID, members, assignmentStore, controllerEnabled, cfg.MinISR, commits, hasControllerAuthority)
	distributed.transport = peerTransport
	dispatcher.SetPartitionAppendFunc(distributed.Append)
	dispatcher.SetHighWatermarkLookup(func(topicName string, partitionID uint32) (uint64, error) {
		state, exists := commits.Get(topicName, partitionID)
		if !exists {
			return 0, fmt.Errorf("commit state missing for %s:%d: %w", topicName, partitionID, replication.ErrInvalidCommitState)
		}
		return state.HighWatermark, nil
	})
	dispatcher.SetMetadataHandler(&localMetadataHandler{brokerID: cfg.BrokerID, address: cfg.AdvertiseAddress, registry: registry, members: members, assignments: assignmentStore, controller: durableController})
	health := observability.NewHealthChecker()
	if err := health.Register("partition_logs", 2*time.Second, func(ctx context.Context) error {
		for _, registeredTopic := range opened {
			for _, partition := range registeredTopic.Partitions() {
				if err := ctx.Err(); err != nil {
					return err
				}
				if _, _, err := partition.OffsetRange(); err != nil {
					return fmt.Errorf("topic %s partition %d: %w", registeredTopic.Name(), partition.ID(), err)
				}
			}
		}
		return nil
	}); err != nil {
		_ = closeTopics(opened)
		return nil, err
	}

	offsetStore, err := offsets.Open(cfg.DataDir)
	if err != nil {
		_ = closeTopics(opened)
		return nil, fmt.Errorf("open consumer offset store: %w", err)
	}
	groupCoordinator, err := group.NewCoordinator(registry, offsetStore)
	if err != nil {
		_ = offsetStore.Close()
		_ = closeTopics(opened)
		return nil, fmt.Errorf("initialize consumer group coordinator: %w", err)
	}
	dispatcher.SetGroupHandler(broker.NewGroupHandler(groupCoordinator))
	var clientKeys *security.KeyStore
	var clientAuthorizer security.Authorizer
	if cfg.ClientAPIKey != "" {
		clientKeys = security.NewKeyStore()
		if err := clientKeys.Add(cfg.ClientAPIKey, security.Principal{ID: "configured-client", Roles: []string{"client"}}, time.Time{}); err != nil {
			_ = offsetStore.Close()
			_ = closeTopics(opened)
			return nil, err
		}
		acl := security.NewACLAuthorizer()
		if err := configureClientACL(acl, cfg.ClientACL); err != nil {
			_ = offsetStore.Close()
			_ = closeTopics(opened)
			return nil, err
		}
		clientAuthorizer = acl
	}
	server := &Server{
		cfg: cfg, adminAddr: strings.TrimSpace(options.AdminAddress), logger: options.Logger,
		registry: registry, topics: opened, dispatch: dispatcher, controls: operations.NewControls(),
		limiter: limiter, metrics: metrics, requests: requests, failures: failures,
		produced: produced, consumed: consumed, latency: latency, members: members, seeds: seeds,
		assignmentStore: assignmentStore, desiredAssignments: assignments, replicator: distributed, commits: commits,
		peerTransport:  peerTransport,
		controllerNode: durableController, controllerRuntime: controllerRuntimeInstance, health: health,
		groupCoordinator: groupCoordinator, offsetStore: offsetStore,
		clientKeys: clientKeys, clientAuthorizer: clientAuthorizer,
		diagnostics:   observability.NewDiagnostics(),
		replicaHealth: make(map[replicaHealthKey]replicaHealthState),
	}
	if server.controllerRuntime != nil {
		server.controllerRuntime.onCommit = server.applyCommittedControllerMetadata
	}
	return server, nil
}

func closeTopics(topics []*topic.Topic) error {
	var errs []error
	for i := len(topics) - 1; i >= 0; i-- {
		if err := topics[i].CloseLogs(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Server) Registry() *topic.Registry        { return s.registry }
func (s *Server) Metrics() *observability.Registry { return s.metrics }
func (s *Server) Membership() *membership.Registry { return s.members }

func (s *Server) Run(ctx context.Context) error {
	if s == nil {
		return errors.New("server is nil")
	}
	if ctx == nil {
		return errors.New("server context is nil")
	}
	stopPeer, err := s.startPeerRuntime(ctx)
	if err != nil {
		closeErr := closeTopics(s.topics)
		return errors.Join(err, closeErr)
	}
	if stopPeer != nil {
		defer stopPeer()
	}
	listener, err := net.Listen("tcp", s.cfg.ListenAddress)
	if err != nil {
		closeErr := closeTopics(s.topics)
		return errors.Join(fmt.Errorf("listen on %s: %w", s.cfg.ListenAddress, err), closeErr)
	}
	if s.cfg.TLSCertFile != "" {
		tlsConfig, err := security.LoadServerTLS(s.cfg.TLSCertFile, s.cfg.TLSKeyFile, s.cfg.TLSClientCAFile, s.cfg.RequireClientCert)
		if err != nil {
			_ = listener.Close()
			closeErr := closeTopics(s.topics)
			return errors.Join(fmt.Errorf("configure TLS: %w", err), closeErr)
		}
		listener = tls.NewListener(listener, tlsConfig)
	}
	s.logger.Info("broker listening", "broker_id", s.cfg.BrokerID, "address", listener.Addr().String(), "advertise_address", s.cfg.AdvertiseAddress, "topics", s.registry.List())
	return s.Serve(ctx, listener)
}

func (s *Server) startPeerRuntime(ctx context.Context) (func(), error) {
	if s == nil || s.members == nil {
		return nil, nil
	}
	listener, err := net.Listen("tcp", s.cfg.PeerListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on peer address %s: %w", s.cfg.PeerListenAddress, err)
	}
	if s.cfg.PeerTLSCertFile != "" {
		peerTLS, tlsErr := security.LoadServerTLS(s.cfg.PeerTLSCertFile, s.cfg.PeerTLSKeyFile, s.cfg.PeerTLSCAFile, s.cfg.RequirePeerCert)
		if tlsErr != nil {
			_ = listener.Close()
			return nil, fmt.Errorf("configure peer TLS server: %w", tlsErr)
		}
		listener = tls.NewListener(listener, peerTLS)
	}
	mux := interbroker.NewMux()
	handler, err := membership.NewHandler(s.members)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	if err := mux.Register(interbroker.MessageBrokerJoin, handler); err != nil {
		_ = listener.Close()
		return nil, err
	}
	if s.controllerRuntime != nil {
		controllerHandler, handlerErr := interbroker.NewDurableControllerHandler(s.controllerNode, func() {
			s.controllerRuntime.NotifyLeaderContact()
			s.applyCommittedControllerMetadata()
		})
		if handlerErr != nil {
			_ = listener.Close()
			return nil, handlerErr
		}
		if err := mux.Register(interbroker.MessageVoteRequest, controllerHandler); err != nil {
			_ = listener.Close()
			return nil, err
		}
		if err := mux.Register(interbroker.MessageAppendEntries, controllerHandler); err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	if len(s.desiredAssignments) > 0 {
		replicationHandler, handlerErr := newReplicationPeerHandler(s.cfg.BrokerID, s.members, s.registry, s.assignmentStore, s.commits, s.controllerNode)
		if handlerErr != nil {
			_ = listener.Close()
			return nil, handlerErr
		}
		if err := mux.Register(interbroker.MessageReplicationBatch, replicationHandler); err != nil {
			_ = listener.Close()
			return nil, err
		}
		if err := mux.Register(interbroker.MessageReplicationStatus, replicationHandler); err != nil {
			_ = listener.Close()
			return nil, err
		}
		if err := mux.Register(interbroker.MessageReplicationCommit, replicationHandler); err != nil {
			_ = listener.Close()
			return nil, err
		}
		if s.controllerNode != nil {
			if err := mux.Register(interbroker.MessagePartitionStatus, replicationHandler); err != nil {
				_ = listener.Close()
				return nil, err
			}
		}
	}
	peerServer, err := interbroker.NewServer(mux)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	runtimeCtx, cancelRuntime := context.WithCancel(ctx)
	peerDone := make(chan error, 1)
	go func() { peerDone <- peerServer.Serve(listener) }()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			cancelRuntime()
			_ = listener.Close()
			<-peerDone
		})
	}

	if len(s.seeds) > 0 {
		transport := s.peerTransport
		deadline := time.NewTimer(s.cfg.PeerJoinTimeout)
		defer deadline.Stop()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		var joinErr error
		for {
			joinCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			joinErr = s.members.Join(joinCtx, transport, s.seeds)
			cancel()
			if joinErr == nil {
				break
			}
			select {
			case <-ctx.Done():
				stop()
				return nil, fmt.Errorf("join broker cluster: %w", ctx.Err())
			case <-deadline.C:
				stop()
				return nil, fmt.Errorf("join broker cluster before timeout: %w", joinErr)
			case <-ticker.C:
			}
		}
	}
	snapshot := s.members.Snapshot()
	s.logger.Info("broker cluster membership ready", "cluster_id", snapshot.ClusterID, "version", snapshot.Version, "members", len(snapshot.Brokers), "peer_address", listener.Addr().String())
	if s.controllerRuntime != nil {
		s.controllerRuntime.Start(runtimeCtx)
		go s.runControllerMetadata(runtimeCtx)
		s.logger.Info("durable controller runtime started", "broker_id", s.cfg.BrokerID, "voters", s.cfg.ControllerVoters)
	}
	if s.replicator != nil && len(s.desiredAssignments) > 0 {
		go s.runReplicaCatchup(runtimeCtx)
	}
	if s.groupCoordinator != nil {
		go s.runGroupExpiration(runtimeCtx)
	}
	return stop, nil
}

func (s *Server) handleClusterMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.members == nil {
		http.Error(w, "cluster membership is not configured", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.members.Snapshot()); err != nil {
		return
	}
}

func (s *Server) handleControllerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.controllerRuntime == nil {
		http.Error(w, "controller runtime is not configured", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(s.controllerRuntime.Status()); err != nil {
		return
	}
}

func (s *Server) handleControllerMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.controllerNode == nil {
		http.Error(w, "controller metadata is not configured", http.StatusServiceUnavailable)
		return
	}
	snapshot := s.controllerNode.Metadata()
	view := struct {
		Version    uint64                           `json:"version"`
		Brokers    []controller.Broker              `json:"brokers"`
		Partitions []controller.PartitionAssignment `json:"partitions"`
	}{Version: snapshot.Version}
	for _, broker := range snapshot.Brokers {
		view.Brokers = append(view.Brokers, broker)
	}
	for _, assignment := range snapshot.Partitions {
		assignment.Replicas = append([]string(nil), assignment.Replicas...)
		view.Partitions = append(view.Partitions, assignment)
	}
	sort.Slice(view.Brokers, func(i, j int) bool { return view.Brokers[i].ID < view.Brokers[j].ID })
	sort.Slice(view.Partitions, func(i, j int) bool {
		if view.Partitions[i].Key.Topic != view.Partitions[j].Key.Topic {
			return view.Partitions[i].Key.Topic < view.Partitions[j].Key.Topic
		}
		return view.Partitions[i].Key.Partition < view.Partitions[j].Key.Partition
	})
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(view); err != nil {
		return
	}
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if s == nil || ctx == nil || listener == nil {
		return errors.New("server, context, and listener are required")
	}
	var runErr error
	started := false
	s.serveOnce.Do(func() { started = true })
	if !started {
		return errors.New("server can only be served once")
	}

	var adminServer *http.Server
	if s.adminAddr != "" {
		adminListener, err := net.Listen("tcp", s.adminAddr)
		if err != nil {
			_ = listener.Close()
			closeErr := closeTopics(s.topics)
			return errors.Join(fmt.Errorf("listen on admin address: %w", err), closeErr)
		}
		mux := http.NewServeMux()
		mux.Handle("/health/live", observability.LivenessHandler())
		mux.Handle("/health/ready", s.health.ReadinessHandler())
		mux.Handle("/diagnostics", s.diagnostics.Handler())
		if s.members != nil {
			mux.HandleFunc("/cluster/members", s.handleClusterMembers)
		}
		if s.controllerRuntime != nil {
			mux.HandleFunc("/controller/status", s.handleControllerStatus)
			mux.HandleFunc("/controller/metadata", s.handleControllerMetadata)
			mux.HandleFunc("/controller/reassignments/stage", s.handleStageReassignment)
			mux.HandleFunc("/controller/reassignments/finalize", s.handleFinalizeReassignment)
		}
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				w.Header().Set("Allow", http.MethodGet)
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			if err := s.metrics.WritePrometheus(w); err != nil {
				http.Error(w, "metrics unavailable", http.StatusInternalServerError)
			}
		})
		var adminHandler http.Handler = mux
		if s.cfg.AdminAPIKey != "" {
			keys := security.NewKeyStore()
			if err := keys.Add(s.cfg.AdminAPIKey, security.Principal{ID: "configured-admin", Roles: []string{"admin"}}, time.Time{}); err != nil {
				_ = adminListener.Close()
				_ = listener.Close()
				closeErr := closeTopics(s.topics)
				return errors.Join(fmt.Errorf("configure admin authentication: %w", err), closeErr)
			}
			authorizer := security.NewACLAuthorizer()
			if err := authorizer.Grant("admin", "admin", "read"); err != nil {
				_ = adminListener.Close()
				_ = listener.Close()
				closeErr := closeTopics(s.topics)
				return errors.Join(fmt.Errorf("configure admin authorization: %w", err), closeErr)
			}
			protected := http.NewServeMux()
			protected.Handle("/health/live", observability.LivenessHandler())
			protected.Handle("/health/ready", s.health.ReadinessHandler())
			protected.Handle("/", security.Protect(mux, keys, authorizer, "admin", "read"))
			adminHandler = protected
		}
		adminServer = &http.Server{Handler: adminHandler, ReadHeaderTimeout: 3 * time.Second}
		go func() {
			if err := adminServer.Serve(adminListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.logger.Error("admin server failed", "error", err)
			}
		}()
		s.logger.Info("admin endpoints listening", "address", adminListener.Addr().String())
	}

	shutdown := make(chan struct{})
	sem := make(chan struct{}, s.cfg.MaxConnections)
	var wg sync.WaitGroup
	var connsMu sync.Mutex
	conns := make(map[net.Conn]struct{})
	stopAccept := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopAccept:
		}
	}()

acceptLoop:
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || isClosedListener(err) {
				break acceptLoop
			}
			s.logger.Error("accept failed", "error", err)
			continue
		}
		select {
		case sem <- struct{}{}:
		default:
			s.logger.Warn("connection limit reached", "remote", conn.RemoteAddr().String())
			_ = conn.Close()
			continue
		}
		connsMu.Lock()
		conns[conn] = struct{}{}
		wg.Add(1)
		connsMu.Unlock()
		go func(conn net.Conn) {
			defer wg.Done()
			defer func() { <-sem; connsMu.Lock(); delete(conns, conn); connsMu.Unlock() }()
			remote := remoteIdentity(conn.RemoteAddr())
			dispatcher := &instrumentedDispatcher{next: s.dispatch, controls: s.controls, limiter: s.limiter, remote: remote, requests: s.requests, failures: s.failures, produced: s.produced, consumed: s.consumed, latency: s.latency, clientKeys: s.clientKeys, clientAuthorizer: s.clientAuthorizer}
			network.HandleConnectionWithTimeouts(conn, dispatcher, shutdown, s.cfg.RequestTimeout, s.cfg.RequestTimeout)
		}(conn)
	}
	close(stopAccept)

	s.controls.BeginDrain()
	drainCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	if err := s.controls.Drain(drainCtx); err != nil {
		s.logger.Warn("drain deadline reached; closing remaining client connections", "error", err)
	}
	cancel()
	close(shutdown)
	connsMu.Lock()
	for conn := range conns {
		_ = conn.Close()
	}
	connsMu.Unlock()
	wg.Wait()

	if adminServer != nil {
		adminCtx, adminCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := adminServer.Shutdown(adminCtx); err != nil {
			runErr = errors.Join(runErr, err)
		}
		adminCancel()
	}
	if err := closeTopics(s.topics); err != nil {
		runErr = errors.Join(runErr, err)
	}
	if s.offsetStore != nil {
		if err := s.offsetStore.Close(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}
	s.logger.Info("broker stopped", "broker_id", s.cfg.BrokerID)
	return runErr
}

func isClosedListener(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && strings.Contains(strings.ToLower(opErr.Err.Error()), "closed")
}

func remoteIdentity(addr net.Addr) string {
	if addr == nil {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err == nil && host != "" {
		return host
	}
	return addr.String()
}

type instrumentedDispatcher struct {
	next                                   RequestDispatcher
	controls                               *operations.Controls
	limiter                                *security.RateLimiter
	remote                                 string
	requests, failures, produced, consumed *observability.Counter
	latency                                *observability.Histogram
	clientKeys                             *security.KeyStore
	clientAuthorizer                       security.Authorizer
}

type RequestDispatcher interface {
	Dispatch(protocol.Request) ([]byte, error)
}

func (d *instrumentedDispatcher) AuthenticationRequired() bool {
	return d != nil && d.clientKeys != nil
}
func (d *instrumentedDispatcher) AuthenticateConnection(token string) (security.Principal, error) {
	if d == nil || d.clientKeys == nil {
		return security.Principal{}, security.ErrInvalidCredential
	}
	return d.clientKeys.Authenticate(token)
}
func (d *instrumentedDispatcher) AuthorizeConnection(principal security.Principal, request protocol.Request) error {
	if d == nil || d.clientAuthorizer == nil {
		return security.ErrForbidden
	}
	topics, action, err := requestTopics(request)
	if err != nil {
		return err
	}
	for _, topic := range topics {
		if !d.clientAuthorizer.Allowed(principal, topic, action) {
			return security.ErrForbidden
		}
	}
	return nil
}

func (d *instrumentedDispatcher) Dispatch(request protocol.Request) ([]byte, error) {
	operation := operations.OperationAdmin
	switch request.APIKey {
	case protocol.APIPRODUCE:
		operation = operations.OperationProduce
	case protocol.APICONSUME:
		operation = operations.OperationConsume
	}
	release, err := d.controls.Begin(operation)
	if err != nil {
		d.failures.Inc()
		return nil, err
	}
	defer release()
	allowed, _, err := d.limiter.Allow(d.remote)
	if err != nil {
		d.failures.Inc()
		return nil, err
	}
	if !allowed {
		d.failures.Inc()
		return nil, protocol.ErrRateLimited
	}
	_ = d.requests.Add(1)
	started := time.Now()
	payload, dispatchErr := d.next.Dispatch(request)
	_ = d.latency.Observe(time.Since(started).Seconds())
	if dispatchErr != nil {
		d.failures.Inc()
		return payload, dispatchErr
	}
	switch request.APIKey {
	case protocol.APIPRODUCE:
		if response, decodeErr := protocol.DecodeProduceResponse(payload); decodeErr == nil {
			_ = d.produced.Add(float64(len(response.Records)))
		}
	case protocol.APICONSUME:
		d.consumed.Inc()
	}
	return payload, nil
}
