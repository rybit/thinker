package cmds

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

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
	l := config.Logger().WithField("file", file)

	delaySec := viper.GetInt("delay")
	times := viper.GetInt("times")
	for ; times > 0; times-- {
		_, err := config.TableTerm().Insert(&structured).RunWrite(conn)
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
