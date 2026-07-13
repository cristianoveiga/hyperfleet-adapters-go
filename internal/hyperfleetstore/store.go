package hyperfleetstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	storectrl "github.com/patjlm/storectrl"

	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/internal/common/hyperfleetapi"
	"github.com/openshift-hyperfleet/hyperfleet-adapters-go/pkg/logger"
)

const (
	defaultPollInterval = 10 * time.Second
	defaultEventLogMax  = 1000
	repollChanSize      = 64
)

// eventLogEntry records a mutation event for watch resumption.
type eventLogEntry struct {
	revision int64
	gvk      schema.GroupVersionKind
	event    storectrl.Event
}

// StoreOption configures a hyperfleetStore.
type StoreOption func(*hyperfleetStore)

// WithPollInterval sets the polling interval for the store.
func WithPollInterval(d time.Duration) StoreOption {
	return func(s *hyperfleetStore) {
		s.pollInterval = d
	}
}

// WithEventLogMax sets the maximum number of events retained for watch resumption.
func WithEventLogMax(n int) StoreOption {
	return func(s *hyperfleetStore) {
		s.eventLogMax = n
	}
}

// hyperfleetStore implements storectrl.Store backed by the HyperFleet HTTP API.
// It maintains an in-memory cache of clusters and node pools, updated by a
// polling loop, and supports the full Store interface including Watch with
// event-log-based resumption.
type hyperfleetStore struct {
	client       hyperfleetapi.Client
	scheme       *runtime.Scheme
	log          logger.Logger
	pollInterval time.Duration

	// in-memory cache for Get/List and diff computation
	mu       sync.RWMutex
	clusters map[string]*HyperFleetCluster  // key = clusterID
	npools   map[string]*HyperFleetNodePool // key = "clusterID/nodepoolID"

	// contentHashes tracks SHA-256 hashes for change detection
	clusterHashes map[string][32]byte
	npoolHashes   map[string][32]byte

	// local monotonic revision counter
	revision atomic.Int64

	// uidCounter for generating unique UIDs
	uidCounter atomic.Int64

	// bounded event log for Watch resumption
	eventLog    []eventLogEntry
	eventLogMax int

	// registered watchers
	clusterWatchers  []*pollingWatcher
	nodepoolWatchers []*pollingWatcher
	watcherMu        sync.Mutex

	// repollCh receives cluster IDs for immediate targeted re-fetch
	repollCh chan string
}

// New creates a new hyperfleetStore with the given client, scheme, and options.
func New(hfClient hyperfleetapi.Client, scheme *runtime.Scheme, log logger.Logger, opts ...StoreOption) *hyperfleetStore {
	s := &hyperfleetStore{
		client:        hfClient,
		scheme:        scheme,
		log:           log,
		pollInterval:  defaultPollInterval,
		clusters:      make(map[string]*HyperFleetCluster),
		npools:        make(map[string]*HyperFleetNodePool),
		clusterHashes: make(map[string][32]byte),
		npoolHashes:   make(map[string][32]byte),
		eventLogMax:   defaultEventLogMax,
		repollCh:      make(chan string, repollChanSize),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start begins the polling goroutine. It should be called once and blocks until
// ctx is cancelled.
func (s *hyperfleetStore) Start(ctx context.Context) {
	go s.pollLoop(ctx)
}

// TriggerRepoll signals the polling loop to immediately re-fetch the given cluster.
// This is non-blocking; the signal is dropped if the loop is busy.
func (s *hyperfleetStore) TriggerRepoll(clusterID string) {
	select {
	case s.repollCh <- clusterID:
	default:
		// drop: the loop is busy; the baseline ticker will catch up
	}
}

// pollLoop runs the main polling loop.
func (s *hyperfleetStore) pollLoop(ctx context.Context) {
	// Initial poll on startup
	s.pollAll(ctx)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.pollAll(ctx)
		case clusterID := <-s.repollCh:
			s.pollCluster(ctx, clusterID)
		case <-ctx.Done():
			return
		}
	}
}

// pollAll discovers all clusters and their node pools from the API.
func (s *hyperfleetStore) pollAll(ctx context.Context) {
	clusters, err := s.client.ListClusters(ctx)
	if err != nil {
		s.log.Errorf(ctx, "hyperfleetstore: list clusters: %v", err)
		return
	}

	// Track which cluster IDs are still alive
	seen := make(map[string]bool, len(clusters))

	for _, detail := range clusters {
		seen[detail.ID] = true
		s.pollCluster(ctx, detail.ID)
	}

	// Emit EventDeleted for clusters that disappeared
	s.mu.Lock()
	var deleted []string
	for id := range s.clusters {
		if !seen[id] {
			deleted = append(deleted, id)
		}
	}
	s.mu.Unlock()

	for _, id := range deleted {
		s.removeCluster(ctx, id)
	}
}

// pollCluster fetches the current state of a single cluster and its node pools,
// then emits Add/Modify/Delete events as needed.
func (s *hyperfleetStore) pollCluster(ctx context.Context, clusterID string) {
	detail, err := s.client.GetCluster(ctx, clusterID)
	if err != nil {
		var notFound *hyperfleetapi.NotFoundError
		if errors.As(err, &notFound) {
			s.removeCluster(ctx, clusterID)
			return
		}
		s.log.Errorf(ctx, "hyperfleetstore: get cluster %s: %v", clusterID, err)
		return
	}

	statuses, err := s.client.GetClusterStatuses(ctx, clusterID)
	if err != nil {
		s.log.Errorf(ctx, "hyperfleetstore: get cluster statuses %s: %v", clusterID, err)
		return
	}

	// Compute content hash for change detection
	hash := contentHash(detail, statuses)

	s.mu.Lock()
	existing, exists := s.clusters[clusterID]
	oldHash := s.clusterHashes[clusterID]
	s.mu.Unlock()

	if !exists {
		// New cluster
		rv := s.nextRevision()
		cluster := clusterFromAPI(detail, statuses, rv)
		cluster.SetUID(types.UID(s.generateUID()))

		s.mu.Lock()
		s.clusters[clusterID] = cluster
		s.clusterHashes[clusterID] = hash
		s.mu.Unlock()

		event := storectrl.Event{
			Type:   storectrl.EventAdded,
			Object: cluster.DeepCopyObject().(client.Object),
		}
		gvk := clusterGVK()
		s.logEvent(gvk, revisionInt(rv), event)
		s.notifyClusterWatchers(event)
		return
	}

	if hash == oldHash {
		return
	}

	// Changed cluster
	rv := s.nextRevision()
	cluster := clusterFromAPI(detail, statuses, rv)
	cluster.SetUID(existing.GetUID())

	s.mu.Lock()
	s.clusters[clusterID] = cluster
	s.clusterHashes[clusterID] = hash
	s.mu.Unlock()

	event := storectrl.Event{
		Type:   storectrl.EventModified,
		Object: cluster.DeepCopyObject().(client.Object),
	}
	gvk := clusterGVK()
	s.logEvent(gvk, revisionInt(rv), event)
	s.notifyClusterWatchers(event)

	// Poll node pools for this cluster
	s.pollNodePools(ctx, clusterID)
}

// pollNodePools fetches all node pools for the given cluster.
func (s *hyperfleetStore) pollNodePools(ctx context.Context, clusterID string) {
	npools, err := s.client.ListNodePools(ctx, clusterID)
	if err != nil {
		s.log.Errorf(ctx, "hyperfleetstore: list node pools for cluster %s: %v", clusterID, err)
		return
	}

	seen := make(map[string]bool, len(npools))
	for _, detail := range npools {
		seen[detail.ID] = true
		s.pollNodePool(ctx, clusterID, detail.ID)
	}

	// Remove node pools that disappeared
	s.mu.Lock()
	var deleted []string
	for key, np := range s.npools {
		if np.ClusterID == clusterID && !seen[np.GetName()] {
			deleted = append(deleted, key)
		}
	}
	s.mu.Unlock()

	for _, key := range deleted {
		s.removeNodePool(ctx, key)
	}
}

// pollNodePool fetches the current state of a single node pool and emits events.
func (s *hyperfleetStore) pollNodePool(ctx context.Context, clusterID, nodepoolID string) {
	detail, err := s.client.GetNodePool(ctx, clusterID, nodepoolID)
	if err != nil {
		var notFound *hyperfleetapi.NotFoundError
		if errors.As(err, &notFound) {
			key := npoolKey(clusterID, nodepoolID)
			s.removeNodePool(ctx, key)
			return
		}
		s.log.Errorf(ctx, "hyperfleetstore: get node pool %s/%s: %v", clusterID, nodepoolID, err)
		return
	}

	statuses, err := s.client.GetNodePoolStatuses(ctx, clusterID, nodepoolID)
	if err != nil {
		s.log.Errorf(ctx, "hyperfleetstore: get node pool statuses %s/%s: %v", clusterID, nodepoolID, err)
		return
	}

	hash := contentHash(detail, statuses)
	key := npoolKey(clusterID, nodepoolID)

	s.mu.Lock()
	existing, exists := s.npools[key]
	oldHash := s.npoolHashes[key]
	s.mu.Unlock()

	if !exists {
		rv := s.nextRevision()
		np := nodepoolFromAPI(detail, statuses, rv)
		np.SetUID(types.UID(s.generateUID()))

		s.mu.Lock()
		s.npools[key] = np
		s.npoolHashes[key] = hash
		s.mu.Unlock()

		event := storectrl.Event{
			Type:   storectrl.EventAdded,
			Object: np.DeepCopyObject().(client.Object),
		}
		gvk := nodepoolGVK()
		s.logEvent(gvk, revisionInt(rv), event)
		s.notifyNodePoolWatchers(event)
		return
	}

	if hash == oldHash {
		return
	}

	rv := s.nextRevision()
	np := nodepoolFromAPI(detail, statuses, rv)
	np.SetUID(existing.GetUID())

	s.mu.Lock()
	s.npools[key] = np
	s.npoolHashes[key] = hash
	s.mu.Unlock()

	event := storectrl.Event{
		Type:   storectrl.EventModified,
		Object: np.DeepCopyObject().(client.Object),
	}
	gvk := nodepoolGVK()
	s.logEvent(gvk, revisionInt(rv), event)
	s.notifyNodePoolWatchers(event)
}

// removeCluster removes a cluster from the cache and emits EventDeleted.
func (s *hyperfleetStore) removeCluster(ctx context.Context, clusterID string) {
	s.mu.Lock()
	cluster, exists := s.clusters[clusterID]
	if !exists {
		s.mu.Unlock()
		return
	}
	delete(s.clusters, clusterID)
	delete(s.clusterHashes, clusterID)
	s.mu.Unlock()

	rv := s.nextRevision()
	deletedCopy := cluster.DeepCopyObject().(client.Object)
	deletedCopy.SetResourceVersion(rv)

	event := storectrl.Event{
		Type:   storectrl.EventDeleted,
		Object: deletedCopy,
	}
	gvk := clusterGVK()
	s.logEvent(gvk, revisionInt(rv), event)
	s.notifyClusterWatchers(event)

	// Also remove all node pools belonging to this cluster
	s.mu.Lock()
	var npKeys []string
	for key, np := range s.npools {
		if np.ClusterID == clusterID {
			npKeys = append(npKeys, key)
		}
	}
	s.mu.Unlock()
	for _, key := range npKeys {
		s.removeNodePool(ctx, key)
	}
}

// removeNodePool removes a node pool from the cache and emits EventDeleted.
func (s *hyperfleetStore) removeNodePool(_ context.Context, key string) {
	s.mu.Lock()
	np, exists := s.npools[key]
	if !exists {
		s.mu.Unlock()
		return
	}
	delete(s.npools, key)
	delete(s.npoolHashes, key)
	s.mu.Unlock()

	rv := s.nextRevision()
	deletedCopy := np.DeepCopyObject().(client.Object)
	deletedCopy.SetResourceVersion(rv)

	event := storectrl.Event{
		Type:   storectrl.EventDeleted,
		Object: deletedCopy,
	}
	gvk := nodepoolGVK()
	s.logEvent(gvk, revisionInt(rv), event)
	s.notifyNodePoolWatchers(event)
}

// --- storectrl.Store implementation ---

// Get retrieves an object by namespace/name key. For HyperFleet types, this is
// a live read from the in-memory cache (updated by the polling loop).
func (s *hyperfleetStore) Get(_ context.Context, key client.ObjectKey, obj client.Object) error {
	switch obj.(type) {
	case *HyperFleetCluster:
		s.mu.RLock()
		cluster, exists := s.clusters[key.Name]
		s.mu.RUnlock()
		if !exists {
			return &storectrl.NotFoundError{Key: key.String()}
		}
		return copyObject(cluster, obj)

	case *HyperFleetNodePool:
		npKey := npoolKey(key.Namespace, key.Name)
		s.mu.RLock()
		np, exists := s.npools[npKey]
		s.mu.RUnlock()
		if !exists {
			return &storectrl.NotFoundError{Key: key.String()}
		}
		return copyObject(np, obj)

	default:
		return fmt.Errorf("hyperfleetstore: Get: unsupported type %T", obj)
	}
}

// List returns a snapshot of all objects of the given list type from the
// in-memory cache.
func (s *hyperfleetStore) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(listOpts)
	}

	// Set list ResourceVersion to the current global revision
	rv := strconv.FormatInt(s.revision.Load(), 10)
	if setter, ok := list.(interface{ SetResourceVersion(string) }); ok {
		setter.SetResourceVersion(rv)
	}

	switch list.(type) {
	case *HyperFleetClusterList:
		s.mu.RLock()
		items := make([]client.Object, 0, len(s.clusters))
		for _, c := range s.clusters {
			if listOpts.Namespace != "" && c.GetNamespace() != listOpts.Namespace {
				continue
			}
			items = append(items, c.DeepCopyObject().(client.Object))
		}
		s.mu.RUnlock()
		return populateListItems(list, items)

	case *HyperFleetNodePoolList:
		s.mu.RLock()
		items := make([]client.Object, 0, len(s.npools))
		for _, np := range s.npools {
			if listOpts.Namespace != "" && np.GetNamespace() != listOpts.Namespace {
				continue
			}
			items = append(items, np.DeepCopyObject().(client.Object))
		}
		s.mu.RUnlock()
		return populateListItems(list, items)

	default:
		return fmt.Errorf("hyperfleetstore: List: unsupported type %T", list)
	}
}

// Create adds a new object to the in-memory cache without calling the API.
// This is called by the storectrl cache during initial list-and-populate.
func (s *hyperfleetStore) Create(_ context.Context, obj client.Object) error {
	switch o := obj.(type) {
	case *HyperFleetCluster:
		key := client.ObjectKeyFromObject(obj)

		s.mu.Lock()
		defer s.mu.Unlock()

		if _, exists := s.clusters[key.Name]; exists {
			return &storectrl.AlreadyExistsError{Key: key.String()}
		}

		accessor, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		if accessor.GetUID() == "" {
			accessor.SetUID(types.UID(s.generateUID()))
		}

		rv := s.revision.Add(1)
		accessor.SetResourceVersion(strconv.FormatInt(rv, 10))

		stored := o.DeepCopyObject().(*HyperFleetCluster)
		s.clusters[key.Name] = stored

		event := storectrl.Event{
			Type:   storectrl.EventAdded,
			Object: stored.DeepCopyObject().(client.Object),
		}
		s.logEventLocked(clusterGVK(), rv, event)
		s.notifyClusterWatchersLocked(event)

		return copyObject(stored, obj)

	case *HyperFleetNodePool:
		key := client.ObjectKeyFromObject(obj)
		npKey := npoolKey(key.Namespace, key.Name)

		s.mu.Lock()
		defer s.mu.Unlock()

		if _, exists := s.npools[npKey]; exists {
			return &storectrl.AlreadyExistsError{Key: key.String()}
		}

		accessor, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		if accessor.GetUID() == "" {
			accessor.SetUID(types.UID(s.generateUID()))
		}

		rv := s.revision.Add(1)
		accessor.SetResourceVersion(strconv.FormatInt(rv, 10))

		stored := o.DeepCopyObject().(*HyperFleetNodePool)
		s.npools[npKey] = stored

		event := storectrl.Event{
			Type:   storectrl.EventAdded,
			Object: stored.DeepCopyObject().(client.Object),
		}
		s.logEventLocked(nodepoolGVK(), rv, event)
		s.notifyNodePoolWatchersLocked(event)

		return copyObject(stored, obj)

	default:
		return fmt.Errorf("hyperfleetstore: Create: unsupported type %T", obj)
	}
}

// Update replaces an existing object in the in-memory cache without calling the API.
// Returns ConflictError if the ResourceVersion does not match.
func (s *hyperfleetStore) Update(_ context.Context, obj client.Object) error {
	switch o := obj.(type) {
	case *HyperFleetCluster:
		key := client.ObjectKeyFromObject(obj)

		s.mu.Lock()
		defer s.mu.Unlock()

		stored, exists := s.clusters[key.Name]
		if !exists {
			return &storectrl.NotFoundError{Key: key.String()}
		}

		if obj.GetResourceVersion() != stored.GetResourceVersion() {
			return &storectrl.ConflictError{Key: key.String()}
		}

		rv := s.revision.Add(1)
		o.SetResourceVersion(strconv.FormatInt(rv, 10))

		updated := o.DeepCopyObject().(*HyperFleetCluster)
		s.clusters[key.Name] = updated

		event := storectrl.Event{
			Type:   storectrl.EventModified,
			Object: updated.DeepCopyObject().(client.Object),
		}
		s.logEventLocked(clusterGVK(), rv, event)
		s.notifyClusterWatchersLocked(event)

		return copyObject(updated, obj)

	case *HyperFleetNodePool:
		key := client.ObjectKeyFromObject(obj)
		npKey := npoolKey(key.Namespace, key.Name)

		s.mu.Lock()
		defer s.mu.Unlock()

		stored, exists := s.npools[npKey]
		if !exists {
			return &storectrl.NotFoundError{Key: key.String()}
		}

		if obj.GetResourceVersion() != stored.GetResourceVersion() {
			return &storectrl.ConflictError{Key: key.String()}
		}

		rv := s.revision.Add(1)
		o.SetResourceVersion(strconv.FormatInt(rv, 10))

		updated := o.DeepCopyObject().(*HyperFleetNodePool)
		s.npools[npKey] = updated

		event := storectrl.Event{
			Type:   storectrl.EventModified,
			Object: updated.DeepCopyObject().(client.Object),
		}
		s.logEventLocked(nodepoolGVK(), rv, event)
		s.notifyNodePoolWatchersLocked(event)

		return copyObject(updated, obj)

	default:
		return fmt.Errorf("hyperfleetstore: Update: unsupported type %T", obj)
	}
}

// Delete removes an object from the in-memory cache without calling the API.
// True deletions are detected by the polling loop.
func (s *hyperfleetStore) Delete(_ context.Context, obj client.Object) error {
	switch obj.(type) {
	case *HyperFleetCluster:
		key := client.ObjectKeyFromObject(obj)

		s.mu.Lock()
		defer s.mu.Unlock()

		stored, exists := s.clusters[key.Name]
		if !exists {
			return &storectrl.NotFoundError{Key: key.String()}
		}
		delete(s.clusters, key.Name)
		delete(s.clusterHashes, key.Name)

		rv := s.revision.Add(1)
		deletedCopy := stored.DeepCopyObject().(client.Object)
		deletedCopy.SetResourceVersion(strconv.FormatInt(rv, 10))

		event := storectrl.Event{
			Type:   storectrl.EventDeleted,
			Object: deletedCopy,
		}
		s.logEventLocked(clusterGVK(), rv, event)
		s.notifyClusterWatchersLocked(event)
		return nil

	case *HyperFleetNodePool:
		key := client.ObjectKeyFromObject(obj)
		npKey := npoolKey(key.Namespace, key.Name)

		s.mu.Lock()
		defer s.mu.Unlock()

		stored, exists := s.npools[npKey]
		if !exists {
			return &storectrl.NotFoundError{Key: key.String()}
		}
		delete(s.npools, npKey)
		delete(s.npoolHashes, npKey)

		rv := s.revision.Add(1)
		deletedCopy := stored.DeepCopyObject().(client.Object)
		deletedCopy.SetResourceVersion(strconv.FormatInt(rv, 10))

		event := storectrl.Event{
			Type:   storectrl.EventDeleted,
			Object: deletedCopy,
		}
		s.logEventLocked(nodepoolGVK(), rv, event)
		s.notifyNodePoolWatchersLocked(event)
		return nil

	default:
		return fmt.Errorf("hyperfleetstore: Delete: unsupported type %T", obj)
	}
}

// Watch returns a Watcher that streams change events for the given list type.
// If WatchFromRevision is provided, events after that revision are replayed
// from the event log before live events begin.
func (s *hyperfleetStore) Watch(_ context.Context, list client.ObjectList, opts ...client.ListOption) (storectrl.Watcher, error) {
	gvk, err := s.gvkForList(list)
	if err != nil {
		return nil, err
	}

	var fromRevision int64
	resuming := false
	for _, opt := range opts {
		if rv, ok := opt.(storectrl.WatchFromRevision); ok {
			fromRevision = int64(rv)
			resuming = true
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var replay []storectrl.Event
	if resuming {
		replay, err = s.eventsSince(fromRevision, gvk)
		if err != nil {
			return nil, err
		}
	}

	bufSize := 200
	if len(replay) > bufSize {
		bufSize = len(replay) + 100
	}
	w := newPollingWatcher(bufSize)

	// Pre-load replay events while holding the lock — no gap.
	for _, evt := range replay {
		w.ch <- evt
	}

	s.watcherMu.Lock()
	switch list.(type) {
	case *HyperFleetClusterList:
		s.clusterWatchers = append(s.clusterWatchers, w)
	case *HyperFleetNodePoolList:
		s.nodepoolWatchers = append(s.nodepoolWatchers, w)
	}
	s.watcherMu.Unlock()

	return w, nil
}

// Apply is not supported by the HyperFleet store.
func (s *hyperfleetStore) Apply(_ context.Context, _ runtime.ApplyConfiguration, _ ...client.ApplyOption) error {
	return fmt.Errorf("apply not supported by hyperfleet store")
}

// --- internal helpers ---

func (s *hyperfleetStore) nextRevision() string {
	rv := s.revision.Add(1)
	return strconv.FormatInt(rv, 10)
}

func (s *hyperfleetStore) generateUID() string {
	id := s.uidCounter.Add(1)
	return fmt.Sprintf("hf-uid-%d", id)
}

// logEvent appends an event to the bounded event log. Must be called without s.mu held.
func (s *hyperfleetStore) logEvent(gvk schema.GroupVersionKind, revision int64, event storectrl.Event) {
	s.mu.Lock()
	s.logEventLocked(gvk, revision, event)
	s.mu.Unlock()
}

// logEventLocked appends an event to the bounded event log. Must be called with s.mu held.
func (s *hyperfleetStore) logEventLocked(gvk schema.GroupVersionKind, revision int64, event storectrl.Event) {
	s.eventLog = append(s.eventLog, eventLogEntry{
		revision: revision,
		gvk:      gvk,
		event:    event,
	})
	if len(s.eventLog) > s.eventLogMax {
		excess := len(s.eventLog) - s.eventLogMax
		trimmed := make([]eventLogEntry, s.eventLogMax)
		copy(trimmed, s.eventLog[excess:])
		s.eventLog = trimmed
	}
}

// eventsSince returns events for the given GVK after fromRevision.
// Must be called with s.mu held.
func (s *hyperfleetStore) eventsSince(fromRevision int64, gvk schema.GroupVersionKind) ([]storectrl.Event, error) {
	currentRevision := s.revision.Load()

	if fromRevision >= currentRevision {
		return nil, nil
	}

	if len(s.eventLog) == 0 {
		return nil, &storectrl.RevisionTooOldError{
			RequestedRevision: fromRevision,
			OldestRevision:    currentRevision + 1,
		}
	}

	oldest := s.eventLog[0].revision
	if fromRevision+1 < oldest {
		return nil, &storectrl.RevisionTooOldError{
			RequestedRevision: fromRevision,
			OldestRevision:    oldest,
		}
	}

	var events []storectrl.Event
	for _, entry := range s.eventLog {
		if entry.revision > fromRevision && entry.gvk == gvk {
			events = append(events, entry.event)
		}
	}
	return events, nil
}

// notifyClusterWatchers sends an event to all registered cluster watchers.
// Stopped or overflowed watchers are removed.
func (s *hyperfleetStore) notifyClusterWatchers(event storectrl.Event) {
	s.watcherMu.Lock()
	defer s.watcherMu.Unlock()
	s.notifyClusterWatchersLocked(event)
}

// notifyClusterWatchersLocked sends events to cluster watchers. Must be called
// with watcherMu held OR during Create/Update/Delete which hold s.mu.
// Note: Create/Update/Delete hold s.mu but call the unlocked version, so
// we need the locked version to avoid double-lock. We use watcherMu for watcher
// list management.
func (s *hyperfleetStore) notifyClusterWatchersLocked(event storectrl.Event) {
	active := make([]*pollingWatcher, 0, len(s.clusterWatchers))
	for _, w := range s.clusterWatchers {
		if w.isStopped() {
			continue
		}
		w.send(event)
		if !w.isStopped() {
			active = append(active, w)
		}
	}
	s.clusterWatchers = active
}

// notifyNodePoolWatchers sends an event to all registered node pool watchers.
func (s *hyperfleetStore) notifyNodePoolWatchers(event storectrl.Event) {
	s.watcherMu.Lock()
	defer s.watcherMu.Unlock()
	s.notifyNodePoolWatchersLocked(event)
}

func (s *hyperfleetStore) notifyNodePoolWatchersLocked(event storectrl.Event) {
	active := make([]*pollingWatcher, 0, len(s.nodepoolWatchers))
	for _, w := range s.nodepoolWatchers {
		if w.isStopped() {
			continue
		}
		w.send(event)
		if !w.isStopped() {
			active = append(active, w)
		}
	}
	s.nodepoolWatchers = active
}

func (s *hyperfleetStore) gvkForList(list client.ObjectList) (schema.GroupVersionKind, error) {
	gvks, _, err := s.scheme.ObjectKinds(list)
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if len(gvks) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("hyperfleetstore: no GVK found for list type %T", list)
	}
	gvk := gvks[0]
	if len(gvk.Kind) > 4 && gvk.Kind[len(gvk.Kind)-4:] == "List" {
		gvk.Kind = gvk.Kind[:len(gvk.Kind)-4]
	}
	return gvk, nil
}

// contentHash computes a SHA-256 hash of the given values' JSON serialization.
func contentHash(values ...interface{}) [32]byte {
	h := sha256.New()
	for _, v := range values {
		b, _ := json.Marshal(v)
		h.Write(b) //nolint:errcheck
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

// npoolKey returns the map key for a node pool.
func npoolKey(clusterID, nodepoolID string) string {
	return clusterID + "/" + nodepoolID
}

// clusterGVK returns the GVK for HyperFleetCluster.
func clusterGVK() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("HyperFleetCluster")
}

// nodepoolGVK returns the GVK for HyperFleetNodePool.
func nodepoolGVK() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("HyperFleetNodePool")
}

// revisionInt parses a revision string to int64, returning 0 on error.
func revisionInt(rv string) int64 {
	n, _ := strconv.ParseInt(rv, 10, 64)
	return n
}

// copyObject copies src to dst via JSON round-trip.
func copyObject(src, dst interface{}) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dst)
}

// populateListItems sets the Items field of list using reflection.
func populateListItems(list client.ObjectList, items []client.Object) error {
	listVal := reflect.ValueOf(list)
	if listVal.Kind() == reflect.Ptr {
		listVal = listVal.Elem()
	}

	itemsField := listVal.FieldByName("Items")
	if !itemsField.IsValid() {
		return fmt.Errorf("list type %T does not have Items field", list)
	}
	if !itemsField.CanSet() {
		return fmt.Errorf("Items field of list type %T cannot be set", list)
	}

	itemsSlice := reflect.MakeSlice(itemsField.Type(), 0, len(items))
	for _, item := range items {
		itemVal := reflect.ValueOf(item)
		if itemVal.Kind() == reflect.Ptr {
			itemVal = itemVal.Elem()
		}
		itemsSlice = reflect.Append(itemsSlice, itemVal)
	}
	itemsField.Set(itemsSlice)
	return nil
}

// Ensure hyperfleetStore implements storectrl.Store.
var _ storectrl.Store = (*hyperfleetStore)(nil)
