package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"golang.org/x/term"
)

const handlerPolicy = `path "sys/replication/+/secondary/promote" {
  capabilities = ["update"]
}

path "sys/replication/+/secondary/update-primary" {
  capabilities = ["update"]
}

path "sys/replication/+/primary/demote" {
  capabilities = ["update"]
}

path "sys/replication/+/primary/secondary-token" {
  capabilities = ["update", "sudo"]
}

path "sys/replication/+/primary/revoke-secondary" {
  capabilities = ["update"]
}

path "auth/token/lookup-self" {
	capabilities = ["read"]
}
`

// Create a policy for the handler token
func createHandlerPolicy(client *vault.Client) error {
	log.Println("creating policy", handlerPolicyName)
	_, err := client.System.PoliciesWriteAclPolicy(context.Background(), handlerPolicyName, schema.PoliciesWriteAclPolicyRequest{
		Policy: handlerPolicy,
	})
	if err != nil {
		return fmt.Errorf("error creating policy: %w", err)
	}
	return nil
}

// Verify the KV engine specific by c.TokenKvMount is present and return the version
func verifyKvEngine(client *vault.Client, tokenKvMount string) (string, error) {
	engines, err := client.System.MountsListSecretsEngines(context.Background())
	if err != nil {
		return "", fmt.Errorf("error listing secrets engines")
	}

	confimedPath := false
	var kvVersion string
	for path, config := range engines.Data {
		if config.(map[string]interface{})["type"] == "kv" {
			if path == tokenKvMount+"/" {
				confimedPath = true
				if v, ok := config.(map[string]interface{})["options"].(map[string]interface{})["version"]; !ok {
					kvVersion = "1"
				} else if v == "2" {
					kvVersion = "2"
				} else {
					fmt.Println("KV engine version not found, assuming v1")
				}
				break
			}
		}
	}
	if !confimedPath {
		return "", fmt.Errorf("no KV engine found")
	}
	log.Printf("KV engine (v%s) found at %s", kvVersion, tokenKvMount)
	return kvVersion, nil
}

// Store the new operations token in the KV engine
func storeToken(kvVersion string, client *vault.Client, batchToken string, tokenKvMount string) error {
	if kvVersion == "2" {
		_, err := client.Secrets.KvV2Write(context.Background(), tokenKvPath, schema.KvV2WriteRequest{
			Data: map[string]interface{}{
				"token": batchToken,
			},
		}, vault.WithMountPath(tokenKvMount))
		if err != nil {
			return fmt.Errorf("error storing token at %s: %w", tokenKvPath, err)
		}
	} else {
		_, err := client.Secrets.KvV1Write(context.Background(), tokenKvPath, map[string]interface{}{
			"token": batchToken,
		})
		if err != nil {
			return fmt.Errorf("error storing token at %s: %w", tokenKvPath, err)
		}
	}
	log.Printf("Token stored at %s/%s", tokenKvMount, tokenKvPath)

	return nil
}

// Create a token with the handler policy
func createToken(client *vault.Client, creatorName string) (string, error) {
	var ttl string
	fmt.Print("Token TTL: ")
	fmt.Scan(&ttl)

	tokenResp, err := client.Auth.TokenCreate(context.Background(), schema.TokenCreateRequest{
		Type:            "batch",
		Policies:        []string{handlerPolicyName},
		NoDefaultPolicy: true,
		NoParent:        true,
		Ttl:             ttl,
		DisplayName:     handlerPolicyName,
		Meta: map[string]interface{}{
			"created_by": creatorName,
		},
	})
	if err != nil {
		return "", fmt.Errorf("error creating batch operations token: %w", err)
	}

	if tokenResp.Warnings != nil {
		log.Printf("token creation warnings: %v", tokenResp.Warnings)
	}

	lookup, err := client.Auth.TokenLookUp(context.Background(), schema.TokenLookUpRequest{
		Token: string(tokenResp.Auth.ClientToken),
	})
	if err != nil {
		return "", fmt.Errorf("error querying for token: %w", err)
	}

	log.Println("Token type:", lookup.Data["type"])
	log.Println("Token policies:", lookup.Data["policies"])
	log.Println("Token display name:", lookup.Data["display_name"])
	log.Println("Token TTL:", lookup.Data["ttl"])
	log.Println("Renewable:", lookup.Data["renewable"])
	log.Println("Created by:", lookup.Data["meta"].(map[string]interface{})["created_by"])

	return tokenResp.Auth.ClientToken, nil
}

// Verify the handler policy exists and is correct
func verifyPolicy(client *vault.Client) error {
	createPolicy := false

	resp, err := client.System.PoliciesReadAclPolicy(context.Background(), handlerPolicyName)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			createPolicy = true
		} else {
			return fmt.Errorf("error querying for policy: %w", err)
		}
	} else {
		switch resp.Data.Policy {
		case "":
			log.Printf("%s policy not found, attempting to create", handlerPolicyName)
			createPolicy = true
		case handlerPolicy:
			log.Printf("%s policy already exists", handlerPolicyName)
		default:
			log.Printf("%s policy does not match expected policy, attempting to update", handlerPolicyName)
			createPolicy = true
		}
	}

	if createPolicy {
		err = createHandlerPolicy(client)
		if err != nil {
			return fmt.Errorf("createPolicy: %w", err)
		}
	}
	return nil
}

// Generate an operations batch token
func generateOpBatchToken(c *ConfigData) error {
	var dec string
	fmt.Print("Proceeed with batch token generation? [y/n]: ")
	fmt.Scan(&dec)

	if dec != "y" {
		log.Fatalln("Operation aborted")
		os.Exit(1)
	}

	if !c.PrimaryCluster.Healthy {
		return fmt.Errorf("primary cluster is not healthy - cannot generate operation batch token")
	}

	log.Println("A token with suitable policy is required to proceed")
	fmt.Print("Vault token: ")
	token, err := term.ReadPassword(0)
	if err != nil {
		return fmt.Errorf("error reading token: %v", err)
	}

	client, err := c.buildClient(c.PrimaryCluster.Addr, string(token))
	if err != nil {
		return fmt.Errorf("build client: %v", err)
	}
	lookup, err := client.Auth.TokenLookUp(context.Background(), schema.TokenLookUpRequest{
		Token: string(token),
	})
	if err != nil {
		return fmt.Errorf("error querying for token: %w", err)
	}
	creatorName := lookup.Data["display_name"].(string)
	fmt.Println()

	err = verifyPolicy(client)
	if err != nil {
		return fmt.Errorf("verifyPolicy: %w", err)
	}

	kvVersion, err := verifyKvEngine(client, c.TokenKvMount)
	if err != nil {
		return fmt.Errorf("verifyKvEngine: %w", err)
	}
	batchToken, err := createToken(client, creatorName)
	if err != nil {
		return fmt.Errorf("createToken: %w", err)
	}
	err = storeToken(kvVersion, client, batchToken, c.TokenKvMount)
	if err != nil {
		return fmt.Errorf("storeToken: %w", err)
	}

	fmt.Println("Retrieve the new token from the KV engine, then run this with the `opBatchToken` flag set to the new token")

	return nil
}
