package cmds

import (
	"encoding/json"
	"fmt"

	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"

	"github.com/rybit/thinker/conf"
)

type dbDesc struct {
	Tables map[string][]string
}

func describe(cmd *cobra.Command, args []string) error {
	config := conf.LoadConfiguration(cmd)
	url, err := config.GetURL()
	if err != nil {
		return err
	}

	conn, err := r.Connect(r.ConnectOpts{
		Addresses:     []string{url},
		DiscoverHosts: true,
		AuthKey:       config.AuthKey,
	})
	if err != nil {
		return err
	}

	var dbs []string
	if len(args) == 1 {
		dbs = []string{args[0]}
	} else {
		dbs, err = extractListOfStrings(r.DBList().Run(conn))
	}

	desc := map[string]map[string][]string{}

	for _, db := range dbs {
		listOfTables, err := extractListOfStrings(r.DB(db).TableList().Run(conn))
		if err != nil {
			return err
		}
		tables := map[string][]string{}
		for _, table := range listOfTables {
			listOfIndexes, err := extractListOfStrings(r.DB(db).Table(table).IndexList().Run(conn))
			if err != nil {
				return err
			}

			tables[table] = listOfIndexes
		}
		desc[db] = tables
	}

	bs, err := json.MarshalIndent(&desc, "", " ")
	if err != nil {
		return err
	}
	fmt.Println(string(bs))

	return nil
}
