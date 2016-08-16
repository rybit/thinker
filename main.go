package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	root := cobra.Command{
		Use:  "thinker <db> <table> <index> <id>",
		RunE: run,
	}

	root.PersistentFlags().BoolP("debug", "d", false, "enable debug logging")
	root.PersistentFlags().StringP("host", "H", "localhost", "host to use for rethink")
	root.PersistentFlags().StringP("key", "k", "", "the auth key to use when connecting")
	root.PersistentFlags().IntP("port", "p", 28015, "port to use for rethink")

	if c, err := root.ExecuteC(); err != nil {
		log.Fatalf("Failed to execute command %s - %s", c.Name(), err.Error())
	}
}

func run(cmd *cobra.Command, args []string) error {
	if len(args) != 4 {
		return errors.New("wrong number of params")
	}
	db := args[0]
	table := args[1]
	index := args[2]
	id := args[3]

	loadConfiguration(cmd)

	host := viper.GetString("host")
	port := viper.GetInt("port")
	authKey := viper.GetString("authKey")
	url := fmt.Sprintf("%s:%d", host, port)
	if url == "" {
		return errors.New("url is invalid")
	}

	l := logrus.WithFields(logrus.Fields{
		"host":  host,
		"port":  port,
		"table": table,
		"db":    db,
		"index": index,
	})

	l.Info("connecting to the database")
	conn, err := r.Connect(r.ConnectOpts{
		Addresses:     []string{url},
		Database:      db,
		DiscoverHosts: true,
		AuthKey:       authKey,
	})
	if err != nil {
		return err
	}

	l.Infof("Querying for '%s'", id)
	r.SetVerbose(true)
	resp, err := r.DB(db).Table(table).Filter(r.Row.Field(index).Eq(id)).Changes(r.ChangesOpts{IncludeInitial: true}).Run(conn)
	if err != nil {
		return err
	}
	defer resp.Close()

	face := map[string]interface{}{}
	i := 0
	l.Info("Listening for entries")
	for resp.Next(&face) {
		i++
		fmt.Printf("%d - %+v\n", i, face)
	}

	l.Info("Finished")
	return nil
}

func loadConfiguration(cmd *cobra.Command) {
	viper.BindPFlags(cmd.PersistentFlags())
	viper.BindPFlags(cmd.Flags())

	if viper.GetBool("debug") {
		logrus.SetLevel(logrus.DebugLevel)
	}

	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
}
