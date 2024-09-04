package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// Revoke the secondary token on the primary cluster
func (c *ConfigData) revokeSecondary(revokeAddr string, client *http.Client) error {
	log.Println("Revoking secondary token for cluster", revokeAddr)

	m, b := map[string]interface{}{"id": "secondary-token"}, new(bytes.Buffer)
	json.NewEncoder(b).Encode(m)

	req, err := http.NewRequest("POST", revokeAddr+"/v1"+replicationPath+c.ClientConfig.Mode+"/primary/revoke-secondary", b)
	req.Header.Set(vaultTokenHeader, c.ClientConfig.OpBatchToken)
	if err != nil {
		return err
	}

	_, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("attempt to revoke secondary failed: %w", err)
	}

	return nil
}

// Create a new http client with the appropriate transport settings
func (c *ConfigData) getHttpClient() *http.Client {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: c.ClientConfig.TlsSkipVerify},
	}
	client := &http.Client{Transport: tr, Timeout: timeout}
	return client
}

// Resolve a primary conflict by demoting the primary with the lowest WAL
// Note that this may not always be the correct resolution, as the primary with
// the lowest WAL may not universally be the best choice for demotion
func (c *ConfigData) resolvePrimaryConflict(haveHighestWal bool, addr string, client *http.Client) error {
	var demoteAddr string
	var revokeAddr string

	if !haveHighestWal {
		demoteAddr = addr
		revokeAddr = c.PrimaryCluster.Addr
		c.SecondaryCluster.Addr = addr
	} else {
		demoteAddr = c.PrimaryCluster.Addr
		revokeAddr = addr
		c.PrimaryCluster.Addr = addr
		c.SecondaryCluster.Addr = c.PrimaryCluster.Addr
	}

	req, err := http.NewRequest("POST", demoteAddr+"/v1"+replicationPath+c.ClientConfig.Mode+"/primary/demote", nil)
	req.Header.Set(vaultTokenHeader, c.ClientConfig.OpBatchToken)
	if err != nil {
		return err
	}
	_, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("multiple primary resolution attempt failed: %w", err)
	}
	log.Printf("Demoted cluster %s and with lowest WAL, attempting to heal replication connection", demoteAddr)
	c.SecondaryCluster.Addr = demoteAddr
	c.initClient(c.PrimaryCluster.Addr)
	c.initClient(c.SecondaryCluster.Addr)
	c.waitForSecondary(true)
	c.revokeSecondary(revokeAddr, client)
	err = c.getActivationToken(c.PrimaryCluster.Client)
	if err != nil {
		return fmt.Errorf("get activation token: %w", err)
	}
	err = c.updatePrimary(c.SecondaryCluster.Client, true)
	if err != nil {
		return err
	}

	return nil
}

// Resolve a secondary conflict by promoting the secondary with the highest WAL
func (c *ConfigData) resolveSecondaryConflict(haveHighestWal bool, addr string, client *http.Client) error {
	// find secondary with highest WAL and promote it
	// get secondary activation token
	// update-primary on secondary

	if !haveHighestWal {
		c.PrimaryCluster.Addr = c.SecondaryCluster.Addr
		c.SecondaryCluster.Addr = addr
	} else {
		c.PrimaryCluster.Addr = addr
	}

	req, err := http.NewRequest("POST", c.PrimaryCluster.Addr+"/v1"+replicationPath+c.ClientConfig.Mode+"/secondary/promote", nil)
	req.Header.Set(vaultTokenHeader, c.ClientConfig.OpBatchToken)
	if err != nil {
		return err
	}
	_, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("multiple secondary resolution attempt failed: %w", err)
	}
	log.Printf("Promoted cluster %s and with highest WAL, attempting to heal replication connection", c.PrimaryCluster.Addr)
	c.initClient(c.PrimaryCluster.Addr)
	c.initClient(c.SecondaryCluster.Addr)
	err = c.getActivationToken(c.PrimaryCluster.Client)
	if err != nil {
		return fmt.Errorf("get activation token: %w", err)
	}
	err = c.updatePrimary(c.SecondaryCluster.Client, true)
	if err != nil {
		return err
	}

	return nil
}

// Assign the primary and secondary cluster addresses based on the discovered topology
func (c *ConfigData) getTopology(verifiedAddrs []string) error {
	for _, addr := range verifiedAddrs {
		client := c.getHttpClient()

		req, err := http.NewRequest("GET", addr+"/v1/auth/token/lookup-self", nil)
		if err != nil {
			return fmt.Errorf("error lookup-self: %w", err)
		}

		// use the operation batch token to lookup-self
		// we should only do this if we haven't already verified the token, otherwise we could overwrite a verified status
		if !c.OpBatchTokenVerified {
			req.Header.Set(vaultTokenHeader, c.ClientConfig.OpBatchToken)
			resp, err := client.Do(req)
			if err != nil || resp.StatusCode != 200 {
				c.OpBatchTokenValid = false
			} else if resp.StatusCode == 200 {
				c.OpBatchTokenValid = true
				c.OpBatchTokenVerified = true
			}
		}

		resp, err := client.Get(addr + "/v1" + replicationPath + c.ClientConfig.Mode + "/status")
		if err != nil || resp.StatusCode != 200 {
			return fmt.Errorf("topology discovery failed: %w", err)
		}
		defer resp.Body.Close()

		var data map[string]interface{}
		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(&data)
		if err != nil {
			log.Printf("error decoding topology json: %v", err)
		}

		var repMode string
		if v, ok := data["data"].(map[string]interface{})["mode"]; ok {
			repMode = v.(string)
		} else {
			return fmt.Errorf("could not determine replication mode for %s", addr)
		}

		var lastWal float64
		if v, ok := data["data"].(map[string]interface{})["last_wal"]; ok {
			lastWal = v.(float64)
		} else {
			lastWal = 0
		}

		var haveHighestWal bool
		if lastWal > c.HighestWal {
			c.HighestWal = lastWal
			haveHighestWal = true
		}

		switch c.ClientConfig.Mode {
		case "dr":
			if repMode != "disabled" {
				switch repMode {
				case "primary":
					if c.PrimaryCluster.Addr != "" {
						log.Println("Multiple primary clusters detected - attempting to resolve conflict")
						err = c.resolvePrimaryConflict(haveHighestWal, addr, client)
						if err != nil {
							return err
						}
					}
					c.PrimaryCluster.Addr = addr
					data, _ := json.Marshal(data["data"])
					err = json.Unmarshal(data, &c.PrimaryDrConfig)
					if err != nil {
						return err
					}
					if c.PrimaryDrConfig.Mode == "primary" && c.PrimaryDrConfig.State == "running" {
						c.PrimaryCluster.Leader = true
					}
				case "secondary":
					if c.SecondaryCluster.Addr != "" {
						log.Println("Multiple secondary clusters detected - attempting to resolve conflict")
						err = c.resolveSecondaryConflict(haveHighestWal, addr, client)
						if err != nil {
							return err
						}
					}
					c.SecondaryCluster.Addr = addr
					data, _ := json.Marshal(data["data"])
					err = json.Unmarshal(data, &c.SecondaryDrConfig)
					if err != nil {
						return err
					}
					if c.SecondaryDrConfig.Mode == "secondary" {
						c.SecondaryCluster.Follower = true
						if c.SecondaryDrConfig.State == "stream-wals" {
							for primary := range c.SecondaryDrConfig.Primaries {
								if c.SecondaryDrConfig.Primaries[primary].ConnectionStatus == "connected" {
									c.SecondaryCluster.Connected = true
									break
								}
							}
						} else {
							c.SecondaryCluster.Connected = false
						}
					}
				}
			}
		case "performance":
			if repMode != "disabled" {
				switch repMode {
				case "primary":
					if c.PrimaryCluster.Addr != "" {
						log.Println("Multiple primary clusters detected - attempting to resolve conflict")
						err = c.resolvePrimaryConflict(haveHighestWal, addr, client)
						if err != nil {
							return err
						}
					}
					c.PrimaryCluster.Addr = addr
					data, _ := json.Marshal(data["data"])
					err = json.Unmarshal(data, &c.PrimaryPrConfig)
					if err != nil {
						return err
					}
					if c.PrimaryPrConfig.Mode == "primary" && c.PrimaryPrConfig.State == "running" {
						c.PrimaryCluster.Leader = true
					}
				case "secondary":
					if c.SecondaryCluster.Addr != "" {
						log.Println("Multiple secondary clusters detected - attempting to resolve conflict")
						err = c.resolveSecondaryConflict(haveHighestWal, addr, client)
						if err != nil {
							return err
						}
					}
					c.SecondaryCluster.Addr = addr
					data, _ := json.Marshal(data["data"])
					err = json.Unmarshal(data, &c.SecondaryPrConfig)
					if err != nil {
						return err
					}
					if c.SecondaryPrConfig.Mode == "secondary" {
						c.SecondaryCluster.Follower = true
						if c.SecondaryPrConfig.State == "stream-wals" {
							for primary := range c.SecondaryPrConfig.Primaries {
								if c.SecondaryPrConfig.Primaries[primary].ConnectionStatus == "connected" {
									c.SecondaryCluster.Connected = true
									break
								}
							}
						} else {
							c.SecondaryCluster.Connected = false
						}
					}
				}
			}
		}
	}

	log.Println("Topology discovery complete")
	return nil
}
