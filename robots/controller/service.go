package controller

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
	robotspb "go.atoms.co/splitter-examples/robots/proto"
)

type robotService struct {
	proxy Proxy
}

func newRobotService(proxy Proxy) *robotService {
	return &robotService{
		proxy: proxy,
	}
}

func (s *robotService) Connect(server robotspb.RobotService_ConnectServer) error {
	// Start processing the stream by converting gRPC stream to channels
	err := grpcx.Receive(server.Context(), server, s.handleConnection)
	if err != nil {
		log.Errorf(server.Context(), "Unable to connect to a controller: %v", err)
	}
	return err
}

func (s *robotService) handleConnection(ctx context.Context, in <-chan *robotspb.ConnectMessage) (<-chan *robotspb.ConnectMessage, error) {
	// Read register as a first message to find where to route the request
	msg, ok := chanx.TryRead(in, 5*time.Second)
	if !ok {
		return nil, fmt.Errorf("could not read from channel: timeout")
	}
	register := msg.GetRegister()
	if register == nil {
		return nil, fmt.Errorf("expected registration message, got: %s", protox.CompactTextString(msg))
	}

	id := robotNameToID(register.GetName())

	log.Infof(ctx, "Connecting robot %s(%v)", register.GetName(), id)

	// Connect to a range using the proxy. Closure of "out" is the signal to end the stream
	out := make(chan *robotspb.ConnectMessage, 100)
	go func() {
		// init is used to detect whether the "out" closure is going to happen by the connect handler
		// Connect calls the handler synchronously so there won't be a race between handler and
		// checking of init later
		init := atomic.Bool{}
		err := s.proxy.Connect(ctx, id, func(ctx context.Context, in2 <-chan *robotspb.ConnectMessage) (<-chan *robotspb.ConnectMessage, error) {
			init.Store(true)

			log.Infof(ctx, "Joining connected robot %s(%s) with a range", register.GetName(), id)

			quit := iox.NewAsyncCloser()

			// Copy messages from range to the outgoing channel to the robot
			go func() {
				defer quit.Close()
				defer close(out)
				for {
					select {
					case m, ok := <-in2:
						if !ok {
							return
						}
						log.Infof(ctx, "Received outgoing message: %s", protox.CompactTextString(m))
						select {
						case out <- m:
						case <-ctx.Done():
							return
						case <-quit.Closed():
							return
						}
					case <-ctx.Done():
						return
					case <-quit.Closed():
						return
					}
				}
			}()

			// Copy incoming messages from the robot to the range.
			out2 := make(chan *robotspb.ConnectMessage, 100)
			// Copy consumed register
			out2 <- msg
			go func() {
				defer quit.Close()
				defer close(out2)
				for {
					select {
					case m, ok := <-in:
						if !ok {
							return
						}
						log.Infof(ctx, "Received incoming message: %s", protox.CompactTextString(m))
						select {
						case out2 <- m:
						case <-ctx.Done():
							return
						case <-quit.Closed():
							return
						}
					case <-ctx.Done():
						return
					case <-quit.Closed():
						return
					}
				}
			}()
			return out2, nil
		})
		if !init.Load() {
			close(out)
		}
		if err != nil {
			log.Errorf(ctx, "Unable to connect to a range: %v", err)
			return
		}
		log.Infof(ctx, "Robot %s(%v) is disconnected", register.GetName(), id)
	}()
	return out, nil
}

func (s *robotService) Act(ctx context.Context, request *robotspb.ActionRequest) (*robotspb.ActionResponse, error) {
	key := robotNameToID(request.GetRobot())
	return s.proxy.Handle(ctx, key, 5*time.Second, request)
}
