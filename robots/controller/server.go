package controller

import (
	"context"
	"net"

	robotspb "go.atoms.co/splitter-examples/robots/proto"
	splitter "go.atoms.co/splitter/pkg/model"
	"google.golang.org/grpc"
)

type server struct {
	proxy splitterRangeProxy
}

func newServer() (*server, []splitter.DispatchFilter) {
	// dkf is used to translate IDs from an application domain to a Splitter domain
	dkf := func(k robotID) splitter.DomainKey { return splitter.DomainKey{Key: splitter.Key(k)} }
	factory := func(ctx context.Context, grant splitter.GrantID, shard splitter.Shard, ownership splitter.Ownership) *robotRange {
		return newRobotRange(ctx, shard)
	}
	processor := splitter.NewProcessor(domain.Domain, robotspb.NewRobotServiceClient, dkf, factory)
	return &server{
		proxy: processor,
	}, []splitter.DispatchFilter{processor}
}

func (s *server) serve(listener net.Listener) error {
	gs := grpc.NewServer()
	robotspb.RegisterRobotServiceServer(gs, newRobotService(NewProxy(s.proxy)))
	return gs.Serve(listener)
}

func (s *server) serveInternal(listener net.Listener) error {
	gs := grpc.NewServer()
	robotspb.RegisterRobotServiceServer(gs, newLocalRobotService(s.proxy))
	return gs.Serve(listener)
}
