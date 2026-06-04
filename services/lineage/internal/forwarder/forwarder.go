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

type Forwarder struct {
	client  tablewriterv1connect.TableWriterServiceClient
	ch      chan *lineagev1.OpenLineageEvent
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
		ch:      make(chan *lineagev1.OpenLineageEvent, cfg.chanSize),
		done:    make(chan struct{}),
		batchSz: cfg.batchSize,
		flush:   time.Duration(cfg.flushMs) * time.Millisecond,
	}

	f.wg.Add(1)
	go f.run()
	return f
}

// Forward enqueues an event for asynchronous delivery. It never blocks; if
// the channel is full the event is dropped and a warning is logged.
func (f *Forwarder) Forward(event *lineagev1.OpenLineageEvent) {
	select {
	case f.ch <- event:
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

	buf := make([]*lineagev1.OpenLineageEvent, 0, f.batchSz)

	for {
		select {
		case evt := <-f.ch:
			buf = append(buf, evt)
			if len(buf) >= f.batchSz {
				f.sendBatch(buf)
				buf = buf[:0]
			}
		case <-ticker.C:
			if len(buf) > 0 {
				f.sendBatch(buf)
				buf = buf[:0]
			}
		case <-f.done:
			// Drain remaining events from the channel.
			for {
				select {
				case evt := <-f.ch:
					buf = append(buf, evt)
				default:
					goto drained
				}
			}
		drained:
			if len(buf) > 0 {
				f.sendBatch(buf)
			}
			return
		}
	}
}

func (f *Forwarder) sendBatch(events []*lineagev1.OpenLineageEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := connect.NewRequest(&tablewriterv1.WriteBatchRequest{
		Events: events,
	})

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
