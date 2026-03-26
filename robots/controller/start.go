package controller

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"go.atoms.co/lib/backoffx"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/service/logx"
	"go.atoms.co/lib/signalx"
	"go.atoms.co/splitter/lib/service/location"
	splitter "go.atoms.co/splitter/pkg/model"
	"google.golang.org/grpc"
)

func createStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a controller",
	}
	cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		logx.Init(context.Background())
	}

	splitterEndpoint := cmd.PersistentFlags().String("splitter-endpoint", "splitter:50051", "Splitter endpoint")
	instance := cmd.PersistentFlags().String("instance", "", "Instance IP to publish")
	port := cmd.PersistentFlags().Int("port", 50051, "Server port")
	internalPort := cmd.PersistentFlags().Int("internal-port", 50052, "Internal server port")

	cmd.Run = func(cmd *cobra.Command, args []string) {
		internalEndpoint := fmt.Sprintf("%v:%v", *instance, *internalPort)

		startController(*splitterEndpoint, internalEndpoint, *port, *internalPort)
	}

	return cmd
}

var (
	domain = splitter.MustParseQualifiedDomainNameStr("facilities/robots/controllers")
)

func startController(splitterEndpoint, internalEndpoint string, port, internalPort int) {
	ctx := context.Background()

	// Initialize

	self := splitter.NewInstance(location.NewInstance(location.NewFromEnv()), internalEndpoint)

	b := backoffx.NewUnlimited(backoffx.WithInitialInterval(time.Second))
	cc, err := backoffx.Retry1(b, func() (*grpc.ClientConn, error) {
		cc, err := grpcx.DialNonBlocking(ctx, splitterEndpoint, grpcx.WithInsecure())
		if err != nil {
			log.Errorf(ctx, "Failed to connect to Splitter at %v: %v", splitterEndpoint, err)
		}
		return cc, err
	})
	if err != nil {
		log.Exitf(ctx, "Failed to connect to Splitter at %v: %v", splitterEndpoint, err)
	}

	s, chain := newServer()

	dispatcher := splitter.NewDispatcher(ctx, splitter.NewConsumerClient(cc), self.Location(), self.Endpoint(), domain.Service, chain)
	quit := iox.WithQuit(dispatcher.Closed(), iox.NewAsyncCloser())

	// Start the server and await termination

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer quit.Close()

		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
		if err != nil {
			log.Errorf(ctx, "failed to open port %v: %v", port, err)
			return
		}
		if err := s.serve(listener); err != nil {
			log.Errorf(ctx, "Server exited: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer quit.Close()

		listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", internalPort))
		if err != nil {
			log.Errorf(ctx, "failed to open port %v: %v", port, err)
			return
		}
		if err := s.serveInternal(listener); err != nil {
			log.Errorf(ctx, "Server exited: %v", err)
		}
	}()

	go func() {
		defer quit.Close()

		sig := <-signalx.InterruptChan()
		log.Infof(ctx, "Received '%v' signal. Exiting", sig)
	}()

	<-quit.Closed()

	log.Infof(ctx, "Shutting down. Exiting in 20s.")

	dispatcher.Drain(20 * time.Second)
	<-dispatcher.Closed()

	time.AfterFunc(20*time.Second, func() {
		log.Exitf(ctx, "Exited forcefully")
	})

	wg.Wait()

	log.Infof(ctx, "Exited")
}
