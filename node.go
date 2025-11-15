package main

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/onflow/crypto"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-go/cmd/bootstrap/run"
	"github.com/onflow/flow-go/consensus"
	"github.com/onflow/flow-go/consensus/hotstuff"
	"github.com/onflow/flow-go/consensus/hotstuff/committees"
	"github.com/onflow/flow-go/consensus/hotstuff/notifications"
	"github.com/onflow/flow-go/consensus/hotstuff/notifications/pubsub"
	"github.com/onflow/flow-go/consensus/hotstuff/persister"
	hsig "github.com/onflow/flow-go/consensus/hotstuff/signature"
	"github.com/onflow/flow-go/consensus/hotstuff/timeoutaggregator"
	"github.com/onflow/flow-go/consensus/hotstuff/timeoutcollector"
	"github.com/onflow/flow-go/consensus/hotstuff/verification"
	"github.com/onflow/flow-go/consensus/hotstuff/voteaggregator"
	"github.com/onflow/flow-go/consensus/hotstuff/votecollector"
	synceng "github.com/onflow/flow-go/engine/common/synchronization"
	"github.com/onflow/flow-go/engine/consensus/compliance"
	"github.com/onflow/flow-go/engine/consensus/message_hub"
	"github.com/onflow/flow-go/model/bootstrap"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/model/flow/filter"
	"github.com/onflow/flow-go/module"
	"github.com/onflow/flow-go/module/buffer"
	builder "github.com/onflow/flow-go/module/builder/consensus"
	synccore "github.com/onflow/flow-go/module/chainsync"
	modulecompliance "github.com/onflow/flow-go/module/compliance"
	finalizer "github.com/onflow/flow-go/module/finalizer/consensus"
	"github.com/onflow/flow-go/module/id"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/onflow/flow-go/module/local"
	consensusMempools "github.com/onflow/flow-go/module/mempool/consensus"
	"github.com/onflow/flow-go/module/mempool/stdmap"
	"github.com/onflow/flow-go/module/metrics"
	msig "github.com/onflow/flow-go/module/signature"
	"github.com/onflow/flow-go/module/trace"
	"github.com/onflow/flow-go/state/protocol"
	bprotocol "github.com/onflow/flow-go/state/protocol/badger"
	"github.com/onflow/flow-go/state/protocol/blocktimer"
	"github.com/onflow/flow-go/state/protocol/events"
	protocol_state "github.com/onflow/flow-go/state/protocol/protocol_state/state"
	"github.com/onflow/flow-go/state/protocol/util"

	//	storage "github.com/onflow/flow-go/storage/badger"
	"github.com/onflow/flow-go/storage"
	storagemock "github.com/onflow/flow-go/storage/mock"
	"github.com/onflow/flow-go/storage/operation/pebbleimpl"
	"github.com/onflow/flow-go/storage/store"
	"github.com/onflow/flow-go/utils/unittest"
)

const hotstuffTimeout = 500 * time.Millisecond

// RandomBeaconNodeInfo stores information about participation in DKG process for consensus node
// contains private + public keys and participant index
// Each node has unique structure
type RandomBeaconNodeInfo struct {
	RandomBeaconPrivKey crypto.PrivateKey
	DKGParticipant      flow.DKGParticipant
}

type ConsensusParticipant struct {
	nodeInfo          bootstrap.NodeInfo
	beaconInfoByEpoch map[uint64]RandomBeaconNodeInfo
}

type ConsensusParticipants struct {
	lookup map[flow.Identifier]ConsensusParticipant // nodeID -> ConsensusParticipant
}

func NewConsensusParticipants(data *run.ParticipantData) *ConsensusParticipants {
	lookup := make(map[flow.Identifier]ConsensusParticipant)
	for _, participant := range data.Participants {
		lookup[participant.NodeID] = ConsensusParticipant{
			nodeInfo: participant.NodeInfo,
			beaconInfoByEpoch: map[uint64]RandomBeaconNodeInfo{
				1: {
					RandomBeaconPrivKey: participant.RandomBeaconPrivKey,
					DKGParticipant:      data.DKGCommittee[participant.NodeID],
				},
			},
		}
	}
	return &ConsensusParticipants{
		lookup: lookup,
	}
}

func (p *ConsensusParticipants) Lookup(nodeID flow.Identifier) *ConsensusParticipant {
	participant, ok := p.lookup[nodeID]
	if ok {
		return &participant
	}
	return nil
}

type epochInfo struct {
	finalView uint64
	counter   uint64
}

type Node struct {
	db                storage.DB
	dbDir             string
	index             int
	log               zerolog.Logger
	id                *flow.Identity
	compliance        *compliance.Engine
	sync              *synceng.Engine
	hot               module.HotStuff
	committee         *committees.Consensus
	voteAggregator    hotstuff.VoteAggregator
	timeoutAggregator hotstuff.TimeoutAggregator
	messageHub        *message_hub.MessageHub
	state             *bprotocol.ParticipantState
	headers           *store.Headers
	net               *Network
}

func createNode(
	participant *ConsensusParticipant,
	index int,
	identity *flow.Identity,
	rootSnapshot protocol.Snapshot,
	hub *Hub,
	stopper *Stopper,
	epochLookup module.EpochLookup,
) *Node {

	pdb, dbDir := TempPebbleDB()
	db := pebbleimpl.ToDB(pdb)
	metricsCollector := metrics.NewNoopCollector()
	tracer := trace.NewNoopTracer()

	lockManager := storage.NewTestingLockManager()

	all := store.InitAll(metricsCollector, db)

	/*
		headersDB := store.NewHeaders(metricsCollector, db)
		guaranteesDB := store.NewGuarantees(metricsCollector, db, storage.DefaultCacheSize)
		sealsDB := store.NewSeals(metricsCollector, db)
		indexDB := store.NewIndex(metricsCollector, db)
		resultsDB := store.NewExecutionResults(metricsCollector, db)
		receiptsDB := storage.NewExecutionReceipts(metricsCollector, db, resultsDB, storage.DefaultCacheSize)
		payloadsDB := storage.NewPayloads(db, indexDB, guaranteesDB, sealsDB, receiptsDB, resultsDB)
		blocksDB := storage.NewBlocks(db, headersDB, payloadsDB)
		qcsDB := storage.NewQuorumCertificates(metricsCollector, db, storage.DefaultCacheSize)
		setupsDB := storage.NewEpochSetups(metricsCollector, db)
		commitsDB := storage.NewEpochCommits(metricsCollector, db)
		protocolStateDB := storage.NewEpochProtocolStateEntries(metricsCollector, setupsDB, commitsDB, db,
			storage.DefaultEpochProtocolStateCacheSize, storage.DefaultProtocolStateIndexCacheSize)
		protocokKVStoreDB := storage.NewProtocolKVStore(metricsCollector, db,
			storage.DefaultProtocolKVStoreCacheSize, storage.DefaultProtocolKVStoreByBlockIDCacheSize)
		versionBeaconDB := store.NewVersionBeacons(badgerimpl.ToDB(db))
	*/

	protocolStateEvents := events.NewDistributor()

	localID := identity.GetNodeID()

	log := unittest.Logger().With().
		Int("index", index).
		Hex("node_id", localID[:]).
		Logger()

	state, err := bprotocol.Bootstrap(
		metricsCollector,
		db,
		lockManager,
		all.Headers,
		all.Seals,
		all.Results,
		all.Blocks,
		all.QuorumCertificates,
		all.EpochSetups,
		all.EpochCommits,
		all.EpochProtocolStateEntries,
		all.ProtocolKVStore,
		all.VersionBeacons,
		rootSnapshot,
	)
	if err != nil {
		panic(err)
	}

	blockTimer, err := blocktimer.NewBlockTimer(uint64(1*time.Millisecond), uint64(90*time.Second))
	if err != nil {
		panic(err)
	}

	fullState, err := bprotocol.NewFullConsensusState(
		log,
		tracer,
		protocolStateEvents,
		state,
		all.Index,
		all.Payloads,
		blockTimer,
		util.MockReceiptValidator(),
		util.MockSealValidator(all.Seals),
	)
	if err != nil {
		panic(err)
	}

	node := &Node{
		db:    db,
		dbDir: dbDir,
		index: index,
		id:    identity,
	}

	stopper.AddNode(node)

	counterConsumer := &CounterConsumer{
		finalized: func(total uint) {
			stopper.onFinalizedTotal(node.id.GetNodeID(), total)
		},
	}

	// log with node index
	logConsumer := notifications.NewLogConsumer(log)
	hotstuffDistributor := pubsub.NewDistributor()
	hotstuffDistributor.AddConsumer(counterConsumer)
	hotstuffDistributor.AddConsumer(logConsumer)

	if participant.nodeInfo.NodeID != localID {
		panic(fmt.Errorf("participant.nodeInfo.NodeID != localID"))
	}
	privateKeys, err := participant.nodeInfo.PrivateKeys()
	if err != nil {
		panic(err)
	}

	// make local
	me, err := local.New(identity.IdentitySkeleton, privateKeys.StakingKey)
	if err != nil {
		panic(err)
	}

	// add a network for this node to the hub
	net := hub.AddNetwork(localID, node)

	guaranteeLimit, sealLimit := uint(1000), uint(1000)
	guarantees := stdmap.NewGuarantees(guaranteeLimit)

	receipts := consensusMempools.NewExecutionTree()

	seals := stdmap.NewIncorporatedResultSeals(sealLimit)

	mutableProtocolState := protocol_state.NewMutableProtocolState(
		log,
		all.EpochProtocolStateEntries,
		all.ProtocolKVStore,
		state.Params(),
		all.Headers,
		all.Results,
		all.EpochSetups,
		all.EpochCommits,
	)

	// initialize the block builder
	build, err := builder.NewBuilder(
		metricsCollector,
		fullState,
		all.Headers,
		all.Seals,
		all.Index,
		all.Blocks,
		all.Results,
		all.Receipts,
		mutableProtocolState,
		guarantees,
		consensusMempools.NewIncorporatedResultSeals(seals, all.Receipts),
		receipts,
		tracer,
	)
	if err != nil {
		panic(err)
	}

	// initialize the pending blocks cache
	cache := buffer.NewPendingBlocks()

	rootHeader, err := rootSnapshot.Head()
	if err != nil {
		panic(err)
	}

	rootQC, err := rootSnapshot.QuorumCertificate()
	if err != nil {
		panic(err)
	}

	committee, err := committees.NewConsensusCommittee(state, localID)
	if err != nil {
		panic(err)
	}
	protocolStateEvents.AddConsumer(committee)

	// initialize the block finalizer
	final := finalizer.NewFinalizer(db.Reader(), all.Headers, fullState, trace.NewNoopTracer())

	syncCore, err := synccore.New(log, synccore.DefaultConfig(), metricsCollector, rootHeader.ChainID)
	if err != nil {
		panic(err)
	}

	voteAggregationDistributor := pubsub.NewVoteAggregationDistributor()
	voteAggregationDistributor.AddVoteAggregationConsumer(logConsumer)

	forks, err := consensus.NewForks(rootHeader, all.Headers, final, hotstuffDistributor, rootHeader, rootQC)
	if err != nil {
		panic(err)
	}

	validator := consensus.NewValidator(metricsCollector, committee)
	if err != nil {
		panic(err)
	}

	keys := &storagemock.SafeBeaconKeys{}
	// there is Random Beacon key for this epoch
	keys.On("RetrieveMyBeaconPrivateKey", mock.Anything).Return(
		func(epochCounter uint64) crypto.PrivateKey {
			dkgInfo, ok := participant.beaconInfoByEpoch[epochCounter]
			if !ok {
				return nil
			}
			return dkgInfo.RandomBeaconPrivKey
		},
		func(epochCounter uint64) bool {
			_, ok := participant.beaconInfoByEpoch[epochCounter]
			return ok
		},
		nil)

	// use epoch aware store for testing scenarios where epoch changes
	beaconKeyStore := hsig.NewEpochAwareRandomBeaconKeyStore(epochLookup, keys)

	signer := verification.NewCombinedSigner(me, beaconKeyStore)

	persist, err := persister.New(db, rootHeader.ChainID, lockManager)
	if err != nil {
		panic(err)
	}

	livenessData, err := persist.GetLivenessData()
	if err != nil {
		panic(err)
	}

	voteProcessorFactory := votecollector.NewCombinedVoteProcessorFactory(committee, voteAggregationDistributor.OnQcConstructedFromVotes)

	createCollectorFactoryMethod := votecollector.NewStateMachineFactory(log, voteAggregationDistributor, voteProcessorFactory.Create)
	voteCollectors := voteaggregator.NewVoteCollectors(log, livenessData.CurrentView, workerpool.New(2), createCollectorFactoryMethod)

	voteAggregator, err := voteaggregator.NewVoteAggregator(
		log,
		metricsCollector,
		metricsCollector,
		metricsCollector,
		voteAggregationDistributor,
		livenessData.CurrentView,
		voteCollectors,
	)
	if err != nil {
		panic(err)
	}

	timeoutAggregationDistributor := pubsub.NewTimeoutAggregationDistributor()
	timeoutAggregationDistributor.AddTimeoutCollectorConsumer(logConsumer)

	timeoutProcessorFactory := timeoutcollector.NewTimeoutProcessorFactory(
		log,
		timeoutAggregationDistributor,
		committee,
		validator,
		msig.ConsensusTimeoutTag,
	)
	timeoutCollectorsFactory := timeoutcollector.NewTimeoutCollectorFactory(
		log,
		timeoutAggregationDistributor,
		timeoutProcessorFactory,
	)
	timeoutCollectors := timeoutaggregator.NewTimeoutCollectors(
		log,
		metricsCollector,
		livenessData.CurrentView,
		timeoutCollectorsFactory,
	)

	timeoutAggregator, err := timeoutaggregator.NewTimeoutAggregator(
		log,
		metricsCollector,
		metricsCollector,
		metricsCollector,
		livenessData.CurrentView,
		timeoutCollectors,
	)
	if err != nil {
		panic(err)
	}

	hotstuffModules := &consensus.HotstuffModules{
		Forks:                       forks,
		Validator:                   validator,
		Notifier:                    hotstuffDistributor,
		Committee:                   committee,
		Signer:                      signer,
		Persist:                     persist,
		VoteCollectorDistributor:    voteAggregationDistributor.VoteCollectorDistributor,
		TimeoutCollectorDistributor: timeoutAggregationDistributor.TimeoutCollectorDistributor,
		VoteAggregator:              voteAggregator,
		TimeoutAggregator:           timeoutAggregator,
	}

	// initialize hotstuff
	hot, err := consensus.NewParticipant(
		log,
		metricsCollector,
		metricsCollector,
		build,
		rootHeader,
		[]*flow.ProposalHeader{},
		hotstuffModules,
		consensus.WithMinTimeout(hotstuffTimeout),
		func(cfg *consensus.ParticipantConfig) {
			cfg.MaxTimeoutObjectRebroadcastInterval = hotstuffTimeout
		},
	)
	if err != nil {
		panic(err)
	}

	// initialize the compliance engine
	compCore, err := compliance.NewCore(
		log,
		metricsCollector,
		metricsCollector,
		metricsCollector,
		metricsCollector,
		hotstuffDistributor,
		tracer,
		all.Headers,
		all.Payloads,
		fullState,
		cache,
		syncCore,
		validator,
		hot,
		voteAggregator,
		timeoutAggregator,
		modulecompliance.DefaultConfig(),
	)
	if err != nil {
		panic(err)
	}

	comp, err := compliance.NewEngine(log, me, compCore)
	if err != nil {
		panic(err)
	}

	identities, err := state.Final().Identities(filter.And(
		filter.HasRole[flow.Identity](flow.RoleConsensus),
		filter.Not(filter.HasNodeID[flow.Identity](me.NodeID())),
	))
	if err != nil {
		panic(err)
	}
	idProvider := id.NewFixedIdentifierProvider(identities.NodeIDs())

	spamConfig, err := synceng.NewSpamDetectionConfig()
	if err != nil {
		panic(err)
	}

	// initialize the synchronization engine
	sync, err := synceng.New(
		log,
		metricsCollector,
		net,
		me,
		state,
		all.Blocks,
		comp,
		syncCore,
		idProvider,
		spamConfig,
		func(cfg *synceng.Config) {
			// use a small pool and scan interval for sync engine
			cfg.ScanInterval = 500 * time.Millisecond
			cfg.PollInterval = time.Second
		},
	)
	if err != nil {
		panic(err)
	}

	messageHub, err := message_hub.NewMessageHub(
		log,
		metricsCollector,
		net,
		me,
		comp,
		hot,
		voteAggregator,
		timeoutAggregator,
		state,
		all.Payloads,
	)
	if err != nil {
		panic(err)
	}

	hotstuffDistributor.AddConsumer(messageHub)

	node.compliance = comp
	node.sync = sync
	node.state = fullState
	node.hot = hot
	node.committee = committee
	node.voteAggregator = hotstuffModules.VoteAggregator
	node.timeoutAggregator = hotstuffModules.TimeoutAggregator
	node.messageHub = messageHub
	node.headers = all.Headers
	node.net = net
	node.log = log

	return node
}

func buildEpochLookupList(epochs ...protocol.CommittedEpoch) []epochInfo {
	infos := make([]epochInfo, 0)
	for _, epoch := range epochs {
		infos = append(infos, epochInfo{
			finalView: epoch.FinalView(),
			counter:   epoch.Counter(),
		})
	}
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].finalView < infos[j].finalView
	})
	return infos
}

func createNodes(participants *ConsensusParticipants, rootSnapshot protocol.Snapshot, stopper *Stopper) (nodes []*Node, hub *Hub, runFor func(time.Duration)) {
	consensus, err := rootSnapshot.Identities(filter.HasRole[flow.Identity](flow.RoleConsensus))
	if err != nil {
		panic(err)
	}

	var epochViewLookup []epochInfo
	currentEpoch, err := rootSnapshot.Epochs().Current()
	if err != nil {

	}
	// Whether there is a next committed epoch depends on the test.
	nextEpoch, err := rootSnapshot.Epochs().NextCommitted()
	if err != nil { // the only acceptable error here is `protocol.ErrNextEpochNotCommitted`
		require.ErrorIs(t, err, protocol.ErrNextEpochNotCommitted)
		epochViewLookup = buildEpochLookupList(currentEpoch)
	} else {
		epochViewLookup = buildEpochLookupList(currentEpoch, nextEpoch)
	}

	epochLookup := &mockmodule.EpochLookup{}
	epochLookup.On("EpochForView", mock.Anything).Return(
		func(view uint64) uint64 {
			for _, info := range epochViewLookup {
				if view <= info.finalView {
					return info.counter
				}
			}
			return 0
		}, func(view uint64) error {
			if view > epochViewLookup[len(epochViewLookup)-1].finalView {
				return fmt.Errorf("unexpected epoch transition")
			} else {
				return nil
			}
		})

	hub = NewNetworkHub(unittest.Logger())
	nodes = make([]*Node, 0, len(consensus))
	for i, identity := range consensus {
		consensusParticipant := participants.Lookup(identity.NodeID)
		if consensusParticipant == nil {
			panic(fmt.Errorf("consensusParticipant is nil"))
		}
		node := createNode(consensusParticipant, i, identity, rootSnapshot, hub, stopper, epochLookup)
		nodes = append(nodes, node)
	}

	// create a context which will be used for all nodes
	ctx, cancel := context.WithCancel(context.Background())
	signalerCtx := irrecoverable.NewMockSignalerContext(t, ctx)

	// create a function to return which the test case can use to run the nodes for some maximum duration
	// and gracefully stop after.
	runFor = func(maxDuration time.Duration) {
		runNodes(signalerCtx, nodes)
		unittest.RequireCloseBefore(t, stopper.stopped, maxDuration, "expect to get signal from stopper before timeout")
		stopNodes(t, cancel, nodes)
	}

	stopper.WithStopFunc(func() {

	})

	return nodes, hub, runFor
}
