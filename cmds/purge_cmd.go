package cmds

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func purge(cmd *cobra.Command, args []string) error {
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

	log := config.Logger().WithFields(logrus.Fields{
		"index": index,
		"id":    id,
	})

	term := config.TableTerm()
	if id != "" && index != "" {
		log.WithFields(logrus.Fields{
			"id":    id,
			"index": index,
		}).Debug("adding filter")
		term = term.GetAllByIndex(index, id)

	} else {
		log.Debug("Deleteing all the rows")
	}
	rsp, err := term.Delete(r.DeleteOpts{ReturnChanges: true}).RunWrite(conn)

	if err != nil {
		return err
	}

	if !viper.GetBool("silent") {
		fmt.Printf("Deleted %d rows\n", rsp.Deleted)
		for i, changeRsp := range rsp.Changes {
			fmt.Printf("Deleted: %d: %+v\n", i, changeRsp.OldValue)
		}
	}

	return nil
}
