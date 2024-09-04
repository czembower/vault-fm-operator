package main

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// Verify that the provided addresses are valid and reachable
func (c *ClientConfig) verifyAddrs() {
	addrs := strings.Split(c.ConfiguredAddrs, ",")
	if len(addrs) != 2 {
		log.Fatalf("Invalid number of addresses specified. Please provide exactly two addresses separated by a comma in `--addresses` flag\n")
	}
	for _, addr := range addrs {
		_, err := url.ParseRequestURI(addr)
		if err != nil {
			log.Fatalf("invalid address: %s\n", addr)
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		client := http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: c.TlsSkipVerify},
				DialContext: (&net.Dialer{
					Timeout: timeout,
				}).DialContext,
			},
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/v1/sys/health", nil)
		if err != nil {
			log.Fatalf("cannot create request: %s\n", err)
		}
		resp, err := client.Do(req)
		if resp != nil {
			defer resp.Body.Close()
			c.VerifiedAddrs = append(c.VerifiedAddrs, addr)
			log.Println("Verified address:", addr)
		}
		if e, ok := err.(net.Error); ok && e.Timeout() {
			log.Println("WARN: request timeout for", addr)
		} else if err != nil {
			log.Printf("WARN: request errored for %s: %v\n", addr, err)
		}
	}
}
