package robot

import (
	"github.com/spf13/cobra"
)

func CreateRobotCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "robot",
		Short: "Robot command",
	}
	cmd.AddCommand(createStartCommand())
	cmd.AddCommand(createActCommand())

	return cmd
}
