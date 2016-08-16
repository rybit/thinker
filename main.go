package main

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type conf struct {
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
	AuthKey string `mapstructure:"key"`
	Debug   bool   `mapstructure:"debug"`
	Follow  bool   `mapstructure:"follow"`
}

func main() {
	root := cobra.Command{
		Use:  "thinker <db> <table> <index> <id>",
		RunE: run,
	}

	root.PersistentFlags().StringP("config", "c", "", "a config file to use")
	root.PersistentFlags().StringP("host", "H", "localhost", "host to use for rethink")
	root.PersistentFlags().StringP("key", "k", "", "the auth key to use when connecting")
	root.PersistentFlags().IntP("port", "p", 28015, "port to use for rethink")
	root.PersistentFlags().BoolP("debug", "d", false, "enable debug logging")
	root.PersistentFlags().BoolP("follow", "f", false, "if we should follow changes")

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

	config := loadConfiguration(cmd)
	_ = config

	url, err := config.GetURL()

	l := logrus.WithFields(logrus.Fields{
		"host":  config.Host,
		"port":  config.Port,
		"url":   url,
		"table": table,
		"db":    db,
		"index": index,
	})

	l.Info("connecting to the database")
	conn, err := r.Connect(r.ConnectOpts{
		Addresses:     []string{url},
		Database:      db,
		DiscoverHosts: true,
		AuthKey:       config.AuthKey,
	})
	if err != nil {
		return err
	}

	l.Infof("Querying for '%s'", id)

	if config.Debug {
		r.SetVerbose(true)
	}

	tableRef := r.DB(db).Table(table)
	var resp *r.Cursor
	if config.Follow {
		l.Info("doing follow query")
		resp, err = tableRef.Filter(r.Row.Field(index).Eq(id)).Changes(r.ChangesOpts{IncludeInitial: true}).Run(conn)
	} else {
		l.Info("doing get all")
		resp, err = tableRef.GetAllByIndex(index, id).Run(conn)
	}
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

func loadConfiguration(cmd *cobra.Command) *conf {
	viper.BindPFlags(cmd.PersistentFlags())
	viper.BindPFlags(cmd.Flags())

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("thinker")
	viper.AutomaticEnv()

	if confFile := viper.GetString("config"); confFile != "" {
		viper.SetConfigFile(confFile)
	}

	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	config := new(conf)
	if err := viper.Unmarshal(config); err != nil {
		logrus.WithError(err).Fatal("Failed to load configuration")
	}

	if config.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	return config
}

func (c *conf) GetURL() (string, error) {
	var url string
	if c.Port > 0 {
		url = fmt.Sprintf("%s:%d", c.Host, c.Port)
	} else {
		url = c.Host
	}
	if url == "" {
		return "", errors.New("url is invalid")
	}
	return url, nil
}
