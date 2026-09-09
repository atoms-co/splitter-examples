package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.atoms.co/iox"
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/mapx"
	"go.atoms.co/lib/syncx"
	robotspb "go.atoms.co/splitter-examples/robots/proto"
	splitter "go.atoms.co/splitter/pkg/model"
)

// robotRange is a component responsible for handling all robots with IDs
// belonging to the range's shard. In a steady state a robot can be handled
// by a single range only. During transitions two ranges may own a single shard.
type robotRange struct {
	iox.AsyncCloser

	init iox.AsyncCloser

	shard  splitter.Shard
	robots map[robotID]*robotConnection

	inject chan func()
}

func newRobotRange(ctx context.Context, shard splitter.Shard) *robotRange {
	r := &robotRange{
		AsyncCloser: iox.NewAsyncCloser(),
		init:        iox.NewAsyncCloser(),
		shard:       shard,
		inject:      make(chan func()),
		robots:      map[robotID]*robotConnection{},
	}
	go r.process(ctx)
	r.init.Close()
	return r
}

// Handle processes a synchronous request to act on a robot.
func (r *robotRange) Handle(ctx context.Context, req *robotspb.ActionRequest) (*robotspb.ActionResponse, error) {
	log.Infof(ctx, "Handling request %v", protox.CompactTextString(req))
	return syncx.Txn1(ctx, r.txn, func() (*robotspb.ActionResponse, error) {
		id := robotNameToID(req.GetRobot())
		if rc, ok := r.robots[id]; ok {
			return nil, rc.handle(ctx, req)
		}
		log.Warnf(ctx, "Robot with name %s(%s) is not found in range %v. Registered robots: %v", req.GetRobot(), id, r.shard, mapx.Keys(r.robots))
		return nil, splitter.ErrNotFound
	})
}

// Connect handles an incoming connection from a robot
func (r *robotRange) Connect(ctx context.Context, id robotID, in <-chan *robotspb.ConnectMessage) (<-chan *robotspb.ConnectMessage, error) {
	return syncx.Txn1(ctx, r.txn, func() (<-chan *robotspb.ConnectMessage, error) {
		log.Infof(ctx, "Connecting robot %s to range %v", id, r.shard)
		if rc, ok := r.robots[id]; ok {
			// Close stale session of the previously connected robot
			rc.quit.Close()
		}

		out := make(chan *robotspb.ConnectMessage, 100)
		rc := newRobotConnection(id, out)
		r.robots[id] = rc
		session := rc.session

		go func() {
			defer rc.quit.Close()
			defer close(out)
		loop:
			for {
				select {
				case m, ok := <-in:
					if !ok {
						break loop
					}
					log.Infof(ctx, "Received message from robot %s: %v", id, protox.CompactTextString(m))
				case <-rc.quit.Closed():
					break loop
				case <-r.Closed():
					break loop
				}
			}
			log.Infof(ctx, "Robot %s is disconnected from range %v", id, r.shard)
			syncx.Txn0(ctx, r.txn, func() {
				if rc, ok := r.robots[id]; ok && rc.session == session {
					// Remove the robot if not reconnected with a new session
					delete(r.robots, id)
				}
			})
		}()
		return out, nil
	})
}

// Initialized returns a closer for when initialized.
func (r *robotRange) Initialized() iox.RAsyncCloser {
	return r.init
}

// Drain is called when a Grant is revoked. It returns a closer for when draining in complete.
func (r *robotRange) Drain(ctx context.Context, timeout time.Duration) iox.RAsyncCloser {
	time.AfterFunc(min(timeout, 3*time.Second), r.Close)
	return r
}

func (r *robotRange) process(ctx context.Context) {
	log.Infof(ctx, "Started processing range %v", r.shard)
	defer r.Close()
loop:
	for {
		select {
		case fn := <-r.inject:
			fn()
		case <-r.Closed():
			break loop
		}
	}
	log.Infof(ctx, "Finished processing range %v", r.shard)
}

// txn runs the given function in the main thread sync.
func (r *robotRange) txn(ctx context.Context, fn func() error) error {
	var wg sync.WaitGroup
	var err error

	wg.Add(1)
	select {
	case r.inject <- func() {
		defer wg.Done()
		err = fn()
	}:
		wg.Wait()
		return err
	case <-r.Closed():
		return splitter.ErrDraining
	case <-ctx.Done():
		return splitter.ErrDraining
	}
}

// robotConnection represents a single connected robot
type robotConnection struct {
	quit    iox.AsyncCloser
	id      robotID
	session uuid.UUID
	out     chan<- *robotspb.ConnectMessage
}

func newRobotConnection(id robotID, out chan<- *robotspb.ConnectMessage) *robotConnection {
	return &robotConnection{
		quit:    iox.NewAsyncCloser(),
		id:      id,
		session: uuid.New(),
		out:     out,
	}
}

func (c *robotConnection) handle(ctx context.Context, msg *robotspb.ActionRequest) error {
	switch {
	case msg.GetMove() != nil:
		m := &robotspb.ConnectMessage{
			Msg: &robotspb.ConnectMessage_Move{
				Move: msg.GetMove(),
			},
		}
		log.Infof(ctx, "Moving robot %s to %s", c.id, msg.GetMove().GetLocation())
		select {
		case c.out <- m:
			return nil
		case <-c.quit.Closed():
			return splitter.ErrDraining
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout")
		}
	default:
		return splitter.ErrInvalid
	}
}
