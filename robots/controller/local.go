package controller

import (
	"context"
	"fmt"
	"time"

	"go.atoms.co/lib/chanx"
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
	robotspb "go.atoms.co/splitter-examples/robots/proto"
	splitter "go.atoms.co/splitter/pkg/model"
)

// localRobotoService handles gRPC requests from the internal port used by other consumers.
// These requests are not forwarded and are handled locally. If owning range is not found locally,
// the requests return an error.
type localRobotService struct {
	proxy splitterRangeProxy
}

func newLocalRobotService(proxy splitterRangeProxy) *localRobotService {
	return &localRobotService{
		proxy: proxy,
	}
}

func (s *localRobotService) Connect(server robotspb.RobotService_ConnectServer) error {
	return grpcx.Receive(server.Context(), server, func(ctx context.Context, in <-chan *robotspb.ConnectMessage) (<-chan *robotspb.ConnectMessage, error) {
		msg, ok := chanx.TryRead(in, 3*time.Second)
		if !ok {
			return nil, fmt.Errorf("could not read from channel: timeout")
		}
		register := msg.GetRegister()
		if register == nil {
			return nil, fmt.Errorf("expected registration message, got: %s", protox.CompactTextString(msg))
		}

		id := robotNameToID(register.GetName())

		log.Infof(ctx, "Connecting robot %s(%s) locally", register.GetName(), id)

		r, ok := s.proxy.Lookup(id)
		if !ok {
			return nil, splitter.ToGRPCError(splitter.ErrNotFound)
		}

		return r.Connect(ctx, id, in)
	})
}

func (s *localRobotService) Act(ctx context.Context, request *robotspb.ActionRequest) (*robotspb.ActionResponse, error) {
	id := robotNameToID(request.GetRobot())
	return splitter.HandleLocalEx(ctx, s.proxy, id, request, splitter.ToGRPCError, (*robotRange).Handle)
}
