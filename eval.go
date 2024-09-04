package main

import (
	"log"
)

// Evaluate the current state of the primary and secondary clusters and
// determine if a promotion scenario is possible
func (c *ConfigData) evaluate() {
	switch c.ClientConfig.Mode {
	case "dr":
		if c.PrimaryDrConfig.ClusterID == c.SecondaryDrConfig.ClusterID && c.PrimaryDrConfig.ClusterID != "" {
			log.Println("Confirmed replication for cluster ID:", c.PrimaryDrConfig.ClusterID)
		} else {
			log.Println("Could not confirm replication relationship")
		}
	case "performance":
		if c.PrimaryPrConfig.ClusterID == c.SecondaryPrConfig.ClusterID && c.PrimaryPrConfig.ClusterID != "" {
			log.Println("Confirmed replication for cluster ID:", c.PrimaryPrConfig.ClusterID)
		} else {
			log.Println("Could not confirm replication relationship")
		}
	}

	switch {
	case !c.OpBatchTokenValid || !c.OpBatchTokenVerified:
		log.Println("Operation batch token is invalid or could not be verified")
		err := generateOpBatchToken(c)
		if err != nil {
			log.Fatalf("%v", err)
		}
	case c.PrimaryCluster.Healthy && c.SecondaryCluster.Healthy && c.PrimaryCluster.Leader && c.SecondaryCluster.Follower && c.SecondaryCluster.Connected && c.OpBatchTokenValid:
		log.Println("Secondary promotion with primary demotion (failover) can be safely initiated")
		c.failover(true, false)
	case !c.OpBatchTokenValid && !c.PrimaryCluster.Healthy && c.ClientConfig.Mode == "dr":
		log.Fatalln("Operation batch token is invalid and primary cluster is not healthy - proceeding with DR operation token generation using secondary cluster recovery method")
		// c.generateOpBatchToken("recovery")
	case c.PrimaryCluster.Healthy && c.SecondaryCluster.Healthy && c.PrimaryCluster.Leader && !c.SecondaryCluster.Follower:
		log.Fatalln("The configured secondary cluster is not in a follower state - this could indicate a split-brain scenario")
	case c.PrimaryCluster.Healthy && c.SecondaryCluster.Healthy && !c.PrimaryCluster.Leader && c.SecondaryCluster.Follower:
		log.Fatalln("The configured primary cluster is not in a leader state - this could indicate a split-brain scenario")
	case c.PrimaryCluster.Healthy && c.SecondaryCluster.Healthy && !c.PrimaryCluster.Leader && !c.SecondaryCluster.Follower:
		log.Fatalln("Both configured primary and secondary clusters are not in an expected replication state")
	case !c.PrimaryCluster.Healthy && c.SecondaryCluster.Healthy && c.SecondaryCluster.Follower && !c.SecondaryCluster.Connected:
		log.Println("Primary cluster unhealthy and secondary is not connected to the primary - proceeding with secondary promotion")
		log.Println("WARNING: ensure old primary is quarantined and demoted before re-establishing client connectivity")
		c.failover(false, true)
	case c.PrimaryCluster.Healthy && c.SecondaryCluster.Healthy && c.PrimaryCluster.Leader && c.SecondaryCluster.Follower && !c.SecondaryCluster.Connected:
		log.Println("Clusters are healthy but secondary is not connected to the primary - an attempt will be made to re-establish healthy replication")
		client := c.getHttpClient()
		err := c.revokeSecondary(c.PrimaryCluster.Addr, client)
		if err != nil {
			log.Fatalf("revoke secondary: %v", err)
		}
		err = c.getActivationToken(c.PrimaryCluster.Client)
		if err != nil {
			log.Fatalf("get activation token: %v", err)
		}
		err = c.updatePrimary(c.SecondaryCluster.Client, true)
		if err != nil {
			log.Fatalf("%v", err)
		}
	case !c.PrimaryCluster.Healthy && c.SecondaryCluster.Healthy && c.SecondaryCluster.Follower && c.SecondaryCluster.Connected:
		log.Println("Primary cluster unhealthy but secondary is connected to the primary - proceeding with secondary promotion")
		log.Println("WARNING: ensure old primary is quarantined and demoted before re-establishing client connectivity")
		c.failover(false, true)
	default:
		log.Println("Could not determine a valid promotion scenario - manual intervention is required")
	}
}
