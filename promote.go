package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/vault-client-go"
)

// Wait for replication mode to be set to "secondary" on newly-demoted cluster
func (c *ConfigData) waitForSecondary(override bool) error {
	var client *vault.Client
	if override {
		client = c.SecondaryCluster.Client
	} else {
		client = c.PrimaryCluster.Client
	}
	switch c.ClientConfig.Mode {
	case "dr":
		var tempStatus SecondaryDrConfig
		for {
			replicationStatus, err := client.System.ReadReplicationStatus(context.Background())
			if err != nil {
				log.Println("Waiting for cluster to be ready...")
				time.Sleep(timeout)
			} else {
				if repStatus, ok := replicationStatus.Data["dr"]; ok {
					data, _ := json.Marshal(repStatus)
					err = json.Unmarshal(data, &tempStatus)
					if err != nil {
						return fmt.Errorf("failed to unmarshal replication status: %w", err)
					}
					if tempStatus.Mode == "secondary" {
						log.Println("Demoted cluster is now confirmed to be in secondary mode")
						time.Sleep(timeout)
						break
					} else {
						time.Sleep(timeout)
					}
				}
			}
		}
	case "performance":
		var tempStatus SecondaryPrConfig
		for {
			replicationStatus, err := client.System.ReadReplicationStatus(context.Background())
			if err != nil {
				log.Println("Waiting for cluster to be ready...")
				time.Sleep(timeout)
			} else {
				if repStatus, ok := replicationStatus.Data["performance"]; ok {
					data, _ := json.Marshal(repStatus)
					err = json.Unmarshal(data, &tempStatus)
					if err != nil {
						return fmt.Errorf("failed to unmarshal replication status: %w", err)
					}
					if tempStatus.Mode == "secondary" {
						log.Println("Demoted cluster is now confirmed to be in secondary mode")
						time.Sleep(timeout)
						break
					} else {
						time.Sleep(timeout)
					}
				}
			}
		}
	}
	return nil
}

// Promote a secondary cluster
func (c *ConfigData) promote() error {
	promotePayload := map[string]interface{}{
		"primary_cluster_addr": c.SecondaryCluster.ClusterAddr,
		"force":                false,
	}

	if c.ClientConfig.Mode == "dr" {
		promotePayload["dr_operation_token"] = c.ClientConfig.OpBatchToken
	}

	log.Println("Promoting secondary cluster...")
	_, err := c.SecondaryCluster.Client.Write(context.Background(), replicationPath+c.ClientConfig.Mode+"/secondary/promote", promotePayload)
	if err != nil {
		return fmt.Errorf("secondary promotion operation failed: %w", err)
	}
	for {
		err = c.initClient(c.SecondaryCluster.Addr)
		if err != nil {
			log.Println("Waiting for cluster to be ready...")
			time.Sleep(timeout)
		} else {
			break
		}
	}

	resp, err := c.SecondaryCluster.Client.System.ReadHealthStatus(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get health status of new primary cluster: %w", err)
	}
	if resp.Data["cluster_name"] != c.SecondaryCluster.Name {
		return fmt.Errorf("expected cluster name %s does not match discovered cluster name %s", c.SecondaryCluster.Name, resp.Data["cluster_name"])
	} else {
		log.Println("Successfully re-authenticated with new primary cluster and confirmed cluster name")
	}

	return nil
}
