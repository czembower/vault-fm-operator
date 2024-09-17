package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/hashicorp/vault-client-go"
)

const (
	// Set this to the KV engine path where the operation token should be stored
	// This path should be a subpath of the KV engine mount specified above.
	tokenKvPath = "failover-handler"
	// Set this to the name of the policy that should be created for the operation token
	handlerPolicyName = "failover-handler"

	timeout          = 3 * time.Second
	replicationPath  = "/sys/replication/"
	vaultTokenHeader = "X-Vault-Token"
)

type ConfigData struct {
	ClientConfig             ClientConfig      `json:"clientConfig,omitempty"`
	PrimaryCluster           ClusterData       `json:"primaryClusterData,omitempty"`
	SecondaryCluster         ClusterData       `json:"secondaryClusterData,omitempty"`
	PrimaryDrConfig          PrimaryDrConfig   `json:"primaryDrConfig,omitempty"`
	SecondaryDrConfig        SecondaryDrConfig `json:"secondaryDrConfig,omitempty"`
	PrimaryPrConfig          PrimaryPrConfig   `json:"primaryPrConfig,omitempty"`
	SecondaryPrConfig        SecondaryPrConfig `json:"secondaryPrConfig,omitempty"`
	OpBatchTokenValid        bool              `json:"opBatchTokenValid,omitempty"`
	OpBatchTokenVerified     bool              `json:"opBatchTokenVerified,omitempty"`
	SecondaryActivationToken string            `json:"secondaryActivationToken,omitempty"`
	HighestWal               float64           `json:"highestWal,omitempty"`
	TokenKvMount             string            `json:"tokenKvPath,omitempty"`
}

type ClusterData struct {
	Addr        string        `json:"addr,omitempty"`
	Name        string        `json:"clusterName,omitempty"`
	Healthy     bool          `json:"healthy,omitempty"`
	Leader      bool          `json:"isLeader,omitempty"`
	Follower    bool          `json:"isFollower,omitempty"`
	Client      *vault.Client `json:"client,omitempty"`
	ClusterAddr string        `json:"clusterAddr,omitempty"`
	Connected   bool          `json:"connected,omitempty"`
}

type ClientConfig struct {
	Mode            string   `json:"mode,omitempty"`
	ConfiguredAddrs string   `json:"configuredAddr,omitempty"`
	OpBatchToken    string   `json:"opBatchToken,omitempty"`
	TlsSkipVerify   bool     `json:"tlsSkipVerify,omitempty"`
	VerifiedAddrs   []string `json:"verifiedAddrs,omitempty"`
}

type DrConfigBase struct {
	ClusterID                string `json:"cluster_id"`
	CorruptedMerkleTree      bool   `json:"corrupted_merkle_tree"`
	MerkleRoot               string `json:"merkle_root"`
	LastCorruptionCheckEpoch string `json:"last_corruption_check_epoch"`
	LastReindexEpoch         string `json:"last_reindex_epoch"`
	LastWal                  int    `json:"last_wal"`
	State                    string `json:"state"`
	Mode                     string `json:"mode"`
}

type PrimaryDrConfig struct {
	DrConfigBase
	PrimaryClusterAddr string   `json:"primary_cluster_addr"`
	KnownSecondaries   []string `json:"known_secondaries"`
	Secondaries        []struct {
		APIAddress                    string    `json:"api_address"`
		ClockSkewMs                   string    `json:"clock_skew_ms"`
		ClusterAddress                string    `json:"cluster_address"`
		ConnectionStatus              string    `json:"connection_status"`
		LastHeartbeat                 time.Time `json:"last_heartbeat"`
		LastHeartbeatDurationMs       string    `json:"last_heartbeat_duration_ms"`
		NodeID                        string    `json:"node_id"`
		ReplicationPrimaryCanaryAgeMs string    `json:"replication_primary_canary_age_ms"`
	} `json:"secondaries"`
	SsctGenerationCounter int `json:"ssct_generation_counter"`
	LastDrWal             int `json:"last_dr_wal"`
}

type SecondaryDrConfig struct {
	DrConfigBase
	KnownPrimaryClusterAddrs []string `json:"known_primary_cluster_addrs"`
	Primaries                []struct {
		APIAddress                    string    `json:"api_address"`
		ClockSkewMs                   string    `json:"clock_skew_ms"`
		ClusterAddress                string    `json:"cluster_address"`
		ConnectionStatus              string    `json:"connection_status"`
		LastHeartbeat                 time.Time `json:"last_heartbeat"`
		LastHeartbeatDurationMs       string    `json:"last_heartbeat_duration_ms"`
		ReplicationPrimaryCanaryAgeMs string    `json:"replication_primary_canary_age_ms"`
	} `json:"primaries"`
	LastRemoteWal int       `json:"last_remote_wal"`
	LastStart     time.Time `json:"last_start"`
	SecondaryID   string    `json:"secondary_id"`
}

type PrConfigBase struct {
	ClusterID                string `json:"cluster_id"`
	CorruptedMerkleTree      bool   `json:"corrupted_merkle_tree"`
	MerkleRoot               string `json:"merkle_root"`
	LastCorruptionCheckEpoch string `json:"last_corruption_check_epoch"`
	LastReindexEpoch         string `json:"last_reindex_epoch"`
	LastWal                  int    `json:"last_wal"`
	Mode                     string `json:"mode"`
	State                    string `json:"state"`
}

type PrimaryPrConfig struct {
	PrConfigBase
	PrimaryClusterAddr string   `json:"primary_cluster_addr"`
	KnownSecondaries   []string `json:"known_secondaries"`
	Secondaries        []struct {
		APIAddress                    string    `json:"api_address"`
		ClockSkewMs                   string    `json:"clock_skew_ms"`
		ClusterAddress                string    `json:"cluster_address"`
		ConnectionStatus              string    `json:"connection_status"`
		LastHeartbeat                 time.Time `json:"last_heartbeat"`
		LastHeartbeatDurationMs       string    `json:"last_heartbeat_duration_ms"`
		NodeID                        string    `json:"node_id"`
		ReplicationPrimaryCanaryAgeMs string    `json:"replication_primary_canary_age_ms"`
	} `json:"secondaries"`
	SsctGenerationCounter int `json:"ssct_generation_counter"`
	LastPerformanceWal    int `json:"last_performance_wal"`
}

type SecondaryPrConfig struct {
	PrConfigBase
	KnownPrimaryClusterAddrs []string `json:"known_primary_cluster_addrs"`
	Primaries                []struct {
		APIAddress                    string    `json:"api_address"`
		ClockSkewMs                   string    `json:"clock_skew_ms"`
		ClusterAddress                string    `json:"cluster_address"`
		ConnectionStatus              string    `json:"connection_status"`
		LastHeartbeat                 time.Time `json:"last_heartbeat"`
		LastHeartbeatDurationMs       string    `json:"last_heartbeat_duration_ms"`
		ReplicationPrimaryCanaryAgeMs string    `json:"replication_primary_canary_age_ms"`
	} `json:"primaries"`
	LastRemoteWal int       `json:"last_remote_wal"`
	LastStart     time.Time `json:"last_start"`
	SecondaryID   string    `json:"secondary_id"`
}

func main() {
	c := ConfigData{}
	flag.StringVar(&c.ClientConfig.ConfiguredAddrs, "addresses", "https://localhost:8200,https://localhost:8300", "Comma-separated list of two Vault addresses in a replication relationship")
	flag.StringVar(&c.ClientConfig.OpBatchToken, "opBatchToken", "", "Operation batch token with a policy that allows for the manipulation of replication configurations on either cluster")
	flag.BoolVar(&c.ClientConfig.TlsSkipVerify, "tlsSkipVerify", false, "Skip TLS verification of the Vault server's certificate")
	flag.StringVar(&c.ClientConfig.Mode, "mode", "", "Replication mode to evaluate ('dr' or 'performance')")
	flag.StringVar(&c.TokenKvMount, "tokenKvMount", "kv", "KV engine mount point where the generated operation token should be stored")
	flag.Parse()

	for _, arg := range os.Args {
		if arg == "--help" || arg == "--h" {
			flag.Usage()
			os.Exit(0)
		}
	}

	flag.VisitAll(func(f *flag.Flag) {
		if f.Value.String() == "" && f.Name != "opBatchToken" {
			log.Fatalf("Missing required flag: %s\n", f.Name)
		}
	})

	c.ClientConfig.verifyAddrs()
	c.initialize()
	c.evaluate()
	log.Println("Operation completed successfully")
}
