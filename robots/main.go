package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.atoms.co/splitter-examples/robots/controller"
	"go.atoms.co/splitter-examples/robots/robot"
)

func main() {
	cmd := &cobra.Command{
		Use:   "robots",
		Short: "Robots",
	}
	cmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	cmd.AddCommand(controller.CreateControllerCmd())
	cmd.AddCommand(robot.CreateRobotCmd())

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
