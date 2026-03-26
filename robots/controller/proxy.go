package controller

import (
	"context"
	"time"

	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
	robotspb "go.atoms.co/splitter-examples/robots/proto"
	splitter "go.atoms.co/splitter/pkg/model"
)

type splitterRangeProxy = splitter.Proxy[robotspb.RobotServiceClient, robotID, *robotRange]

// Proxy is a component that forwards calls to a range that owns the given robot.
// Range can be located in the current process or could be on a remote instance.
// Proxy can be used by other components, e.g., if they need to initiate a request
// to a key in some Splitter domain, or by gRPC services where external requests need
// to be forwarded to the owning ranges.
type Proxy interface {
	// Handle calls the range Handle method with the given request and timeout.
	Handle(ctx context.Context, id robotID, timeout time.Duration, req *robotspb.ActionRequest) (*robotspb.ActionResponse, error)
	// Connect calls the range Connect method and uses the given handler to process incoming and outgoing stream messages
	Connect(ctx context.Context, id robotID, handler grpcx.Handler[*robotspb.ConnectMessage, *robotspb.ConnectMessage]) error
}

type proxy struct {
	proxy splitterRangeProxy
}

func NewProxy(p splitterRangeProxy) Proxy {
	return &proxy{
		proxy: p,
	}
}

func (p *proxy) Handle(ctx context.Context, id robotID, timeout time.Duration, req *robotspb.ActionRequest) (*robotspb.ActionResponse, error) {
	resp, err := splitter.HandleWithRetryEx(ctx, timeout, p.proxy, id, robotspb.RobotServiceClient.Act, req, splitter.ToGRPCError, func(r *robotRange) (*robotspb.ActionResponse, error) {
		return r.Handle(ctx, req)
	})
	if err != nil {
		log.Errorf(ctx, "Invoking %s failed: %v", protox.CompactTextString(req), err)
		return nil, err
	}
	return resp, nil
}

func (p *proxy) Connect(ctx context.Context, id robotID, handler grpcx.Handler[*robotspb.ConnectMessage, *robotspb.ConnectMessage]) error {
	client, err := p.proxy.Resolve(ctx, id)
	loc, _ := p.proxy.Location(id)
	log.Infof(ctx, "Resolved a robot %s with an error: %v. Location: %v", id, err, loc)
	if err != nil { // err == nil indicates that the range is located on a remote instance
		if r, ok := p.proxy.Lookup(id); ok {
			return grpcx.ShortCircuit(ctx, handler, func(ctx context.Context, in <-chan *robotspb.ConnectMessage) (<-chan *robotspb.ConnectMessage, error) {
				return r.Connect(ctx, id, in)
			})
		}
		return splitter.ToGRPCError(splitter.ErrNotOwned)
	}

	return grpcx.Connect(ctx, client.Connect, func(ctx context.Context, in <-chan *robotspb.ConnectMessage) (<-chan *robotspb.ConnectMessage, error) {
		resp, err := handler(ctx, in)
		if err != nil {
			return nil, splitter.ToGRPCError(err)
		}
		return resp, nil
	})
}
