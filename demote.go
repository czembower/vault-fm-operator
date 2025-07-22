package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

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

	// we terminate when we're only healing the config, in which case there is no persona reversal
	// in that heal-only case, we use the current primary cluster's API address
	// otherwise, we use the secondary cluster's API address
	var addr string
	if terminate {
		addr = c.PrimaryCluster.Addr
	} else {
		addr = c.SecondaryCluster.Addr
	}
	if c.ClientConfig.LoadBalanced {
		log.Println("Using load-balanced address for replication config primary_api_addr:", addr)
		updatePayload["primary_api_addr"] = addr
	}
	if c.ClientConfig.CaFilePath != "" {
		log.Println("Using custom CA file for replication config:", c.ClientConfig.CaFilePath)
		updatePayload["ca_file"] = c.ClientConfig.CaFilePath
	}

	var lastErr error
	maxRetries := 5
	for range make([]struct{}, maxRetries) {
		_, err := client.Write(context.Background(), replicationPath+c.ClientConfig.Mode+"/secondary/update-primary", updatePayload)
		if err == nil {
			log.Println("Successfully updated secondary cluster with new primary address")
			if terminate {
				log.Println("Operation completed successfully")
				os.Exit(0)
			} else {
				return nil
			}
		}
		log.Printf("update-primary operation failed: %v", err)
		time.Sleep(3 * time.Second)
		lastErr = err
	}

	return fmt.Errorf("update-primary operation failed after retries: %w", lastErr)
}
