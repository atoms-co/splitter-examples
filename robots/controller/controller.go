package controller

import (
	"github.com/spf13/cobra"
)

func CreateControllerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "controller",
		Short: "Controller command",
	}

	cmd.AddCommand(createStartCommand())

	return cmd
}
