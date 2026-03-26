package robot

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/spf13/cobra"
	"go.atoms.co/lib/encoding/protox"
	"go.atoms.co/lib/iox"
	"go.atoms.co/lib/log"
	"go.atoms.co/lib/net/grpcx"
	"go.atoms.co/lib/service/logx"
	"go.atoms.co/lib/signalx"
	robotspb "go.atoms.co/splitter-examples/robots/proto"
)

func createStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a robot",
	}
	cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		logx.Init(context.Background())
	}

	endpoint := cmd.PersistentFlags().String("endpoint", "controller:50051", "Controller endpoint")
	name := cmd.PersistentFlags().String("name", "", "Robot name")

	cmd.Run = func(cmd *cobra.Command, args []string) {
		if len(*name) == 0 {
			cmd.PrintErr("Error: empty robot name\n")
			return
		}
		startRobot(*endpoint, *name)
	}

	return cmd
}

func startRobot(endpoint, name string) {
	ctx, cancel := context.WithCancel(context.Background())

	log.Infof(ctx, "Starting robot %s", name)

	quit := iox.NewAsyncCloser()

	go func() {
		connect(ctx, quit, endpoint, name)
	}()

	go func() {
		defer quit.Close()

		sig := <-signalx.InterruptChan()
		log.Infof(ctx, "Received '%v' signal. Exiting", sig)
	}()

	<-quit.Closed()

	log.Infof(ctx, "Shutting down. Exiting in 3s.")

	time.AfterFunc(3*time.Second, func() {
		log.Exitf(ctx, "Exited forcefully")
	})

	cancel()

	log.Infof(ctx, "Exited")
}

func connect(ctx context.Context, quit iox.AsyncCloser, endpoint, name string) {
	for !quit.IsClosed() {
		connectOnce(ctx, endpoint, name)
	}
}

func connectOnce(ctx context.Context, endpoint, name string) {
	cc, err := grpcx.DialNonBlocking(ctx, endpoint, grpcx.WithInsecure())
	if err != nil {
		log.Errorf(ctx, "Unable to dial %s: %v", endpoint, err)
		time.Sleep(3 * time.Second)
		return
	}
	defer func() { _ = cc.Close() }()

	client, err := robotspb.NewRobotServiceClient(cc).Connect(ctx)
	if err != nil {
		log.Errorf(ctx, "Unable to call ConnectService at %s: %v", endpoint, err)
		time.Sleep(3 * time.Second)
		return
	}

	err = client.Send(&robotspb.ConnectMessage{
		Msg: &robotspb.ConnectMessage_Register{
			Register: &robotspb.Register{
				Name: name,
			},
		},
	})
	if err != nil {
		log.Errorf(ctx, "Failed to send a registration message: %v", err)
		return
	}

	recv(ctx, client, name)

	log.Infof(ctx, "Robot %s disconnected from the controller", name)
}

func recv(ctx context.Context, client robotspb.RobotService_ConnectClient, name string) {
	log.Infof(ctx, "Robot %s connected to the controller", name)

	for {
		m, err := client.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Errorf(ctx, "Failed to read a message: %v", err)
			}
			return
		}
		log.Infof(ctx, "Received a message: %v", protox.CompactTextString(m))

		switch {
		case m.GetMove() != nil:
			log.Infof(ctx, "Moving to position %s", m.GetMove().GetLocation())
		default:
			log.Errorf(ctx, "Unsupported message: %v", protox.CompactTextString(m))
		}
	}
}
