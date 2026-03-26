package robot

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"go.atoms.co/lib/net/grpcx"
	robotspb "go.atoms.co/splitter-examples/robots/proto"
)

func createActCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "act",
		Short: "Act on a robot",
	}
	cmd.AddCommand(createMoveCommand())

	return cmd
}

func createMoveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "move",
		Short: "Move a robot to a location",
	}

	name := cmd.Flags().String("name", "", "Robot name")
	location := cmd.Flags().String("location", "", "New location")
	endpoint := cmd.Flags().String("endpoint", "controller:50051", "Controller endpoint")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if len(*name) == 0 {
			return fmt.Errorf("empty robot name")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cc, err := grpcx.DialNonBlocking(ctx, *endpoint, grpcx.WithInsecure())
		if err != nil {
			cmd.PrintErrf("Could not dial %s: %v\n", *endpoint, err)
			return nil
		}
		defer func() { _ = cc.Close() }()

		_, err = robotspb.NewRobotServiceClient(cc).Act(ctx, &robotspb.ActionRequest{
			Robot: *name,
			Action: &robotspb.ActionRequest_Move{
				Move: &robotspb.Move{
					Location: *location,
				},
			},
		})
		if err != nil {
			cmd.PrintErrf("Could not move %s to %s: %v\n", *name, *location, err)
			return nil
		}
		cmd.Printf("Robot %s moved to %s\n", *name, *location)
		return nil
	}

	return cmd
}
