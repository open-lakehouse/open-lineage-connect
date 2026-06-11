package forwarder

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"

	lineagev1 "github.com/open-lakehouse/open-lineage-service/gen/lineage/v1"
	tablewriterv1 "github.com/open-lakehouse/open-lineage-service/gen/table/v1"
	"github.com/open-lakehouse/open-lineage-service/gen/table/v1/tablewriterv1connect"
)

const (
	defaultBatchSize = 100
	defaultFlushMs   = 500
	defaultChanSize  = 1000
)

// entry pairs an event with the caller's bearer token so the background
// flusher can group events by token and forward each group under that token's
// Authorization header (per-user credential vending downstream).
type entry struct {
	token string
	event *lineagev1.OpenLineageEvent
}

type Forwarder struct {
	client  tablewriterv1connect.TableWriterServiceClient
	ch      chan entry
	done    chan struct{}
	wg      sync.WaitGroup
	batchSz int
	flush   time.Duration
}

// New creates a Forwarder that asynchronously sends events to the table
// writer service at baseURL. Events are buffered and flushed in batches.
func New(baseURL string, opts ...Option) *Forwarder {
	cfg := options{
		batchSize: defaultBatchSize,
		flushMs:   defaultFlushMs,
		chanSize:  defaultChanSize,
	}
	for _, o := range opts {
		o(&cfg)
	}

	f := &Forwarder{
		client: tablewriterv1connect.NewTableWriterServiceClient(
			http.DefaultClient,
			baseURL,
			connect.WithProtoJSON(),
		),
		ch:      make(chan entry, cfg.chanSize),
		done:    make(chan struct{}),
		batchSz: cfg.batchSize,
		flush:   time.Duration(cfg.flushMs) * time.Millisecond,
	}

	f.wg.Add(1)
	go f.run()
	return f
}

// Forward enqueues an event (with the caller's bearer token) for asynchronous
// delivery. It never blocks; if the channel is full the event is dropped and a
// warning is logged. The signature matches service.EventCallback.
func (f *Forwarder) Forward(token string, event *lineagev1.OpenLineageEvent) {
	select {
	case f.ch <- entry{token: token, event: event}:
	default:
		log.Printf("forwarder: channel full, dropping event")
	}
}

// Close drains the remaining events and shuts down the background goroutine.
func (f *Forwarder) Close() {
	close(f.done)
	f.wg.Wait()
}

func (f *Forwarder) run() {
	defer f.wg.Done()

	ticker := time.NewTicker(f.flush)
	defer ticker.Stop()

	// Buffer events grouped by token so each flush forwards one WriteBatch per
	// distinct caller, preserving per-user identity for downstream credential
	// vending. total tracks the aggregate buffered count for the size trigger.
	buf := make(map[string][]*lineagev1.OpenLineageEvent)
	total := 0

	flush := func() {
		if total == 0 {
			return
		}
		for token, events := range buf {
			f.sendBatch(token, events)
		}
		buf = make(map[string][]*lineagev1.OpenLineageEvent)
		total = 0
	}

	for {
		select {
		case e := <-f.ch:
			buf[e.token] = append(buf[e.token], e.event)
			total++
			if total >= f.batchSz {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-f.done:
			// Drain remaining events from the channel, then flush.
			for {
				select {
				case e := <-f.ch:
					buf[e.token] = append(buf[e.token], e.event)
					total++
				default:
					flush()
					return
				}
			}
		}
	}
}

func (f *Forwarder) sendBatch(token string, events []*lineagev1.OpenLineageEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := connect.NewRequest(&tablewriterv1.WriteBatchRequest{
		Events: events,
	})
	// Forward the caller's bearer token so the table-service can vend
	// per-user Unity Catalog credentials. Empty token => no header (anonymous
	// / UC-auth-disabled path).
	if token != "" {
		req.Header().Set("Authorization", "Bearer "+token)
	}

	resp, err := f.client.WriteBatch(ctx, req)
	if err != nil {
		log.Printf("forwarder: WriteBatch failed (%d events): %v", len(events), err)
		return
	}
	log.Printf("forwarder: wrote %d events (status=%s)", resp.Msg.Written, resp.Msg.Status)
}

type options struct {
	batchSize int
	flushMs   int
	chanSize  int
}

// Option configures the Forwarder.
type Option func(*options)

func WithBatchSize(n int) Option  { return func(o *options) { o.batchSize = n } }
func WithFlushMs(ms int) Option   { return func(o *options) { o.flushMs = ms } }
func WithChanSize(n int) Option   { return func(o *options) { o.chanSize = n } }
