package cmds

import (
	"errors"

	"github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"

	"github.com/rybit/thinker/conf"
)

func RootCmd() *cobra.Command {
	readCmd := cobra.Command{
		Use:  "read <db> <table> [<index> <id>]",
		RunE: read,
	}
	readCmd.Flags().BoolP("follow", "f", false, "if we should follow changes")

	writeCmd := cobra.Command{
		Use:  "write <db> <table> <file>",
		RunE: write,
	}
	writeCmd.Flags().Int32P("times", "t", 1, "the number of times to write the file to the db")
	writeCmd.Flags().Int32P("delay", "d", 0, "the number of seconds to pause between writes")

	createCmd := cobra.Command{
		Use:  "create <db> <table>",
		RunE: create,
	}
	createCmd.Flags().StringSliceP("index", "i", nil, "an index to create")

	describeCmd := cobra.Command{
		Use:  "describe [db]",
		RunE: describe,
	}

	purgeCmd := cobra.Command{
		Use:  "purge <db> <table> [index id]",
		RunE: purge,
	}
	purgeCmd.Flags().BoolP("silent", "s", false, "if we should dump the lines found")

	root := cobra.Command{}
	root.PersistentFlags().StringP("config", "c", "", "a config file to use")
	root.PersistentFlags().StringP("host", "H", "localhost", "host to use for rethink")
	root.PersistentFlags().StringP("key", "k", "", "the auth key to use when connecting")
	root.PersistentFlags().IntP("port", "p", 28015, "port to use for rethink")
	root.PersistentFlags().BoolP("verbose", "v", false, "enable debug logging")
	root.AddCommand(&writeCmd, &readCmd, &createCmd, &describeCmd, &purgeCmd)

	return &root
}

func connect(cmd *cobra.Command, args []string) (*conf.Conf, *r.Session, error) {
	config := conf.LoadConfiguration(cmd)
	url, err := config.GetURL()
	if err != nil {
		return nil, nil, err
	}

	if len(args) < 2 {
		return nil, nil, errors.New("wrong number of params")
	}
	config.DB = args[0]
	config.Table = args[1]

	config.Logger().Debug("Connecting")
	conn, err := r.Connect(r.ConnectOpts{
		Addresses:     []string{url},
		Database:      config.DB,
		DiscoverHosts: true,
		AuthKey:       config.AuthKey,
	})
	if err != nil {
		return nil, nil, err
	}

	if config.Debug {
		r.SetVerbose(true)
		logrus.SetLevel(logrus.DebugLevel)
	}

	return config, conn, nil
}

func extractListOfStrings(resp *r.Cursor, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}

	listOfStrings := []string{}
	return listOfStrings, resp.All(&listOfStrings)
}

func containsString(val string, slice []string) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}
