package main

import (
	"log"

	"github.com/rybit/thinker/cmds"
)

func main() {
	if c, err := cmds.RootCmd().ExecuteC(); err != nil {
		log.Fatalf("Failed to execute command %s - %s", c.Name(), err.Error())
	}
}
