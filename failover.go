package main

import (
	"fmt"
	"log"
	"os"
)

// Failover a healthy cluster pair
func (c *ConfigData) failover(demotePrimary bool, force bool) {
	var dec string

	if !force {
		fmt.Print("Proceeed with operation? [y/n]: ")
		fmt.Scan(&dec)
		if dec != "y" {
			log.Fatalln("Operation aborted")
			os.Exit(1)
		}
	}

	// if the primary is healthy, demote it before promoting the secondary
	if demotePrimary {
		err := c.demote()
		if err != nil {
			log.Fatalf("demote: %v", err)
		}
	}

	err := c.promote()
	if err != nil {
		log.Fatalf("promote: %v", err)
	}

	if demotePrimary {
		err = c.waitForSecondary(false)
		if err != nil {
			log.Fatalf("wait for secondary: %v", err)
		}

		err = c.getActivationToken(c.SecondaryCluster.Client)
		if err != nil {
			log.Fatalf("get activation token: %v", err)
		}

		// initialize the new secondary client
		err = c.initClient(c.PrimaryCluster.Addr)
		if err != nil {
			log.Fatalf("error initializing new secondary cluster client: %v", err)
		}
		err = c.updatePrimary(c.PrimaryCluster.Client, false)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}
}
