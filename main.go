package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type conf struct {
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
	AuthKey string `mapstructure:"key"`
	Debug   bool   `mapstructure:"verbose"`
	Follow  bool   `mapstructure:"follow"`

	db    string
	table string
}

func main() {
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

	root := cobra.Command{}
	root.PersistentFlags().StringP("config", "c", "", "a config file to use")
	root.PersistentFlags().StringP("host", "H", "localhost", "host to use for rethink")
	root.PersistentFlags().StringP("key", "k", "", "the auth key to use when connecting")
	root.PersistentFlags().IntP("port", "p", 28015, "port to use for rethink")
	root.PersistentFlags().BoolP("verbose", "v", false, "enable debug logging")
	root.AddCommand(&writeCmd, &readCmd)

	if c, err := root.ExecuteC(); err != nil {
		log.Fatalf("Failed to execute command %s - %s", c.Name(), err.Error())
	}
}

func connect(cmd *cobra.Command, args []string) (*conf, *r.Session, error) {
	config := loadConfiguration(cmd)
	url, err := config.GetURL()
	if err != nil {
		return nil, nil, err
	}

	if len(args) < 2 {
		return nil, nil, errors.New("wrong number of params")
	}
	config.db = args[0]
	config.table = args[1]

	config.logger().Debug("Connecting")
	conn, err := r.Connect(r.ConnectOpts{
		Addresses:     []string{url},
		Database:      config.db,
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

func write(cmd *cobra.Command, args []string) error {
	config, conn, err := connect(cmd, args)
	if err != nil {
		return err
	}
	if len(args) != 3 {
		return errors.New("require a file to be set")
	}

	file := args[2]
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	structured := make(map[string]interface{})
	err = json.Unmarshal(data, &structured)
	if err != nil {
		return nil
	}
	l := config.logger().WithField("file", file)

	delaySec := viper.GetInt("delay")
	times := viper.GetInt("times")
	for ; times > 0; times-- {
		_, err := config.tableTerm().Insert(&structured).RunWrite(conn)
		if err != nil {
			return err
		}

		l.Debug("wrote entry")
		if delaySec > 0 {
			l.Debugf("sleeping for %d seconds", delaySec)
			time.Sleep(time.Second * time.Duration(delaySec))
		}
	}

	return nil
}

func read(cmd *cobra.Command, args []string) error {
	config, conn, err := connect(cmd, args)
	if err != nil {
		return err
	}

	var index string
	var id string
	if len(args) == 4 {
		index = args[2]
		id = args[3]
	}

	l := config.logger().WithFields(logrus.Fields{
		"index": index,
		"id":    id,
	})
	term := config.tableTerm()
	if config.Follow {
		if index != "" && id != "" {
			term = term.Filter(r.Row.Field(index).Eq(id))
		}

		l.Debug("doing follow query")
		term = term.Changes(r.ChangesOpts{IncludeInitial: true})
	} else {
		l.Debug("doing get all")
		if index != "" && id != "" {
			term = term.GetAllByIndex(index, id)
		}
	}

	resp, err := term.Run(conn)
	if err != nil {
		return err
	}
	defer resp.Close()

	face := map[string]interface{}{}
	i := 0
	l.Debug("Listening for entries")
	for resp.Next(&face) {
		i++
		b, _ := json.MarshalIndent(&face, "", " ")
		fmt.Println(string(b))
		fmt.Println("-----------")
	}

	l.Debug("Finished")
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

func (c *conf) logger() *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"host":  c.Host,
		"port":  c.Port,
		"table": c.table,
		"db":    c.db,
	})
}

func (c *conf) tableTerm() r.Term {
	return r.DB(c.db).Table(c.table)
}
