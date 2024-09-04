package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/hashicorp/vault-client-go"
)

// Demote a primary cluster
func (c *ConfigData) demote() error {
	log.Println("Demoting primary cluster...")
	_, err := c.PrimaryCluster.Client.Write(context.Background(), replicationPath+c.ClientConfig.Mode+"/primary/demote", nil)
	if err != nil {
		return fmt.Errorf("primary demotion operation failed: %w", err)
	}

	return nil
}

// Get a new secondary activation token
func (c *ConfigData) getActivationToken(client *vault.Client) error {
	var activationTokenPayload = map[string]interface{}{
		"id": "secondary-token",
	}
	resp, err := client.Write(context.Background(), replicationPath+c.ClientConfig.Mode+"/primary/secondary-token", activationTokenPayload)
	if err != nil {
		return fmt.Errorf("failed to generate new secondary activation token: %w", err)
	}
	c.SecondaryActivationToken = resp.WrapInfo.Token

	return nil
}

// Update a secondary cluster with a new primary address
func (c *ConfigData) updatePrimary(client *vault.Client, terminate bool) error {
	log.Println("Updating new secondary cluster with new primary address")
	var updatePayload map[string]interface{}

	switch c.ClientConfig.Mode {
	case "dr":
		updatePayload = map[string]interface{}{
			"dr_operation_token": c.ClientConfig.OpBatchToken,
			"token":              c.SecondaryActivationToken,
		}
	case "performance":
		updatePayload = map[string]interface{}{
			"token": c.SecondaryActivationToken,
		}
	}
	_, err := client.Write(context.Background(), replicationPath+c.ClientConfig.Mode+"/secondary/update-primary", updatePayload)
	if err != nil {
		log.Printf("%v\n", c)
		return fmt.Errorf("update-primary operation failed: %w", err)
	}
	log.Println("Successfully updated secondary cluster with new primary address")

	if terminate {
		log.Println("Operation completed successfully")
		os.Exit(0)
	}

	return nil
}
