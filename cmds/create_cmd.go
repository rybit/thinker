package cmds

import (
	r "github.com/dancannon/gorethink"
	"github.com/spf13/cobra"
)

func create(cmd *cobra.Command, args []string) error {
	config, conn, err := connect(cmd, args)
	if err != nil {
		return err
	}
	log := config.Logger()
	log.Debug("Getting list of databases")
	listOfDBs, err := extractListOfStrings(r.DBList().Run(conn))
	if err != nil {
		return err
	}

	if !containsString(config.DB, listOfDBs) {
		log.Debugf("The database %s didn't exist - creating it", config.DB)
		_, err = r.DBCreate(config.DB).RunWrite(conn)
		if err != nil {
			return err
		}
	}
	log.Debugf("There are dbs: %+v", listOfDBs)
	db := r.DB(config.DB)

	log.Debugf("Getting list of tables")
	listOfTables, err := extractListOfStrings(db.TableList().Run(conn))
	if err != nil {
		return err
	}

	if !containsString(config.Table, listOfTables) {
		log.WithField("tableName", config.Table).Debugf("The table doesn't exists - creating it")
		_, err = db.TableCreate(config.Table).RunWrite(conn)
		if err != nil {
			return err
		}
	}
	log.Debug("Created table")
	table := db.Table(config.Table)

	listOfIndexes, err := extractListOfStrings(table.IndexList().Run(conn))
	if err != nil {
		return err
	}
	indices, _ := cmd.Flags().GetStringSlice("index")
	if err != nil {
		return err
	}
	for _, i := range indices {
		if !containsString(i, listOfIndexes) {
			log.WithField("index", i).Debug("Creating index")
			_, err = table.IndexCreate(i).RunWrite(conn)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
