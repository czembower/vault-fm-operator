package main

import (
	"context"
	"fmt"
	"log"

	"github.com/hashicorp/vault-client-go"
)

// Initialize vault clients for primary and secondary clusters
func (c *ConfigData) initialize() {
	c.OpBatchTokenVerified = false
	c.OpBatchTokenValid = false
	err := c.getTopology(c.ClientConfig.VerifiedAddrs)
	if err != nil {
		log.Fatalf("error getting topology: %v", err)
	}

	for _, addr := range c.ClientConfig.VerifiedAddrs {
		err := c.initClient(addr)
		if err != nil {
			log.Printf("client initialization failed for %s: %v", addr, err)
		}
	}

	if c.PrimaryCluster.Client == nil && c.SecondaryCluster.Client == nil {
		log.Fatalln("could not initialize clients for primary and secondary clusters")
	}
}

// Build a vault client for a given address
// If a token is not provided, the client will use the operation batch token
func (c *ConfigData) buildClient(addr string, token string) (*vault.Client, error) {
	if token == "" {
		token = c.ClientConfig.OpBatchToken
	}
	tls := vault.TLSConfiguration{}
	tls.InsecureSkipVerify = c.ClientConfig.TlsSkipVerify
	client, err := vault.New(
		vault.WithAddress(addr),
		vault.WithRequestTimeout(timeout),
		vault.WithRetryConfiguration(vault.RetryConfiguration{}),
		vault.WithTLS(tls),
	)
	if err != nil {
		return nil, fmt.Errorf("error initializing client for %s: %w", addr, err)
	}

	client.SetToken(token)
	return client, nil
}

// Initialize a Vault client and verify the health status of the cluster
func (c *ConfigData) initClient(addr string) error {
	var repMode string
	switch addr {
	case c.PrimaryCluster.Addr:
		repMode = "primary"
	case c.SecondaryCluster.Addr:
		repMode = "secondary"
	default:
		return fmt.Errorf("could not determine replication mode for %s - aborting", addr)
	}

	client, err := c.buildClient(addr, "")
	if err != nil {
		return fmt.Errorf("build client: %w", err)
	}

	healthResp, err := client.System.ReadHealthStatus(context.Background())
	if err != nil {
		return fmt.Errorf("read health status: %w", err)
	}
	if healthResp.Data["initialized"] != true || healthResp.Data["sealed"] != false {
		return fmt.Errorf("cluster at %s is not healthy", addr)
	}

	leaderResp, err := client.System.LeaderStatus(context.Background())
	if err != nil {
		return err
	}

	if addr == c.PrimaryCluster.Addr {
		c.PrimaryCluster.Client = client
		c.PrimaryCluster.Healthy = true
		c.PrimaryCluster.Name = healthResp.Data["cluster_name"].(string)
		c.PrimaryCluster.ClusterAddr = leaderResp.Data.LeaderClusterAddress
	} else {
		c.SecondaryCluster.Client = client
		c.SecondaryCluster.Healthy = true
		c.SecondaryCluster.Name = healthResp.Data["cluster_name"].(string)
		c.SecondaryCluster.ClusterAddr = leaderResp.Data.LeaderClusterAddress
	}
	log.Printf("Initialized client for %s (%s %s)", addr, c.ClientConfig.Mode, repMode)

	return nil
}
