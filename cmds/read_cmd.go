package cmds

import (
	"encoding/json"
	"fmt"

	"github.com/Sirupsen/logrus"
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"
)

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

	l := config.Logger().WithFields(logrus.Fields{
		"index": index,
		"id":    id,
	})
	term := config.TableTerm()
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
