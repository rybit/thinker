package conf

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Conf struct {
	Host    string `mapstructure:"host"`
	Port    int    `mapstructure:"port"`
	AuthKey string `mapstructure:"key"`
	Debug   bool   `mapstructure:"verbose"`
	Follow  bool   `mapstructure:"follow"`

	DB    string
	Table string
}

func (c *Conf) GetURL() (string, error) {
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

func (c *Conf) Logger() *logrus.Entry {
	return logrus.WithFields(logrus.Fields{
		"host":  c.Host,
		"port":  c.Port,
		"table": c.Table,
		"db":    c.DB,
	})
}

func (c *Conf) TableTerm() r.Term {
	return r.DB(c.DB).Table(c.Table)
}

func LoadConfiguration(cmd *cobra.Command) *Conf {
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

	config := new(Conf)
	if err := viper.Unmarshal(config); err != nil {
		logrus.WithError(err).Fatal("Failed to load configuration")
	}

	if config.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	return config
}
