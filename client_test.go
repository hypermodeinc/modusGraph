/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	mg "github.com/hypermodeinc/modusgraph"
	"github.com/stretchr/testify/require"
)

func TestClientPool(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ClientPoolWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ClientPoolWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test as MODUSGRAPH_TEST_ADDR is not set")
			}

			// Create a client with pool size 10
			client, err := mg.NewClient(tc.uri, mg.WithPoolSize(10))
			require.NoError(t, err)
			defer client.Close()

			// Test concurrent client pool usage
			const numWorkers = 20
			var wg sync.WaitGroup
			var mu sync.Mutex
			var clientCount int

			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					// Get a client from the pool
					client, cleanup, err := client.DgraphClient()
					require.NoError(t, err)
					require.NotNil(t, client)
					txn := client.NewReadOnlyTxn()
					ctx := context.Background()
					_, err = txn.Query(ctx, "query { q(func: uid(1)) { uid } }")
					require.NoError(t, err)
					err = txn.Discard(ctx)
					require.NoError(t, err)

					// Verify we got a valid Dgraph client
					if client != nil {
						mu.Lock()
						clientCount++
						mu.Unlock()
					}

					// Clean up the client
					cleanup()
				}()
			}

			// Wait for all workers to complete
			wg.Wait()

			// Verify we got clients from the pool
			require.GreaterOrEqual(t, clientCount, 1)

			// Get a client before close
			beforeClient, cleanupBefore, err := client.DgraphClient()
			require.NoError(t, err)
			require.NotNil(t, beforeClient)

			// Close the client pool
			client.Close()
			time.Sleep(100 * time.Millisecond) // Give some time for cleanup

			// Verify we can still get a new client after close (pool will create a new one)
			afterClient, cleanupAfter, err := client.DgraphClient()
			require.NoError(t, err)
			require.NotNil(t, afterClient)

			// Verify the client is actually new
			require.NotEqual(t, fmt.Sprintf("%p", beforeClient), fmt.Sprintf("%p", afterClient))

			// Clean up the client
			cleanupAfter()

			// Also clean up the before client if it wasn't already closed
			cleanupBefore()
		})
	}

	// Shutdown at the end of the test to ensure the next test can start fresh
	mg.Shutdown()
}

func TestClientPoolStress(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ClientPoolStressWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ClientPoolStressWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test as MODUSGRAPH_TEST_ADDR is not set")
			}

			// Create a client with pool size 10
			client, err := mg.NewClient(tc.uri, mg.WithPoolSize(10))
			require.NoError(t, err)
			defer func() {
				client.Close()
			}()

			// Test concurrent client pool usage with high load
			const numWorkers = 20
			const iterations = 10
			var wg sync.WaitGroup
			var successCount int
			var errorCount int
			var mu sync.Mutex

			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < iterations; j++ {
						dgraphClient, cleanup, err := client.DgraphClient()
						if err != nil {
							mu.Lock()
							errorCount++
							mu.Unlock()
							continue
						}

						if dgraphClient != nil {
							// Test the client works
							txn := dgraphClient.NewReadOnlyTxn()
							ctx := context.Background()
							_, err = txn.Query(ctx, "query { q(func: uid(1)) { uid } }")
							if err != nil {
								err = txn.Discard(ctx)
								if err != nil {
									mu.Lock()
									errorCount++
									mu.Unlock()
								}
								cleanup()
								continue
							}
							err = txn.Discard(ctx)
							if err != nil {
								mu.Lock()
								errorCount++
								mu.Unlock()
								cleanup()
								continue
							}

							mu.Lock()
							successCount++
							mu.Unlock()
						}

						// Clean up the client
						cleanup()
					}
				}()
			}

			wg.Wait()

			require.Greater(t, successCount, 0)
		})

		mg.Shutdown()
	}
}

func TestClientPoolMisuse(t *testing.T) {
	testCases := []struct {
		name string
		uri  string
		skip bool
	}{
		{
			name: "ClientPoolMisuseWithFileURI",
			uri:  "file://" + GetTempDir(t),
		},
		{
			name: "ClientPoolMisuseWithDgraphURI",
			uri:  "dgraph://" + os.Getenv("MODUSGRAPH_TEST_ADDR"),
			skip: os.Getenv("MODUSGRAPH_TEST_ADDR") == "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test as MODUSGRAPH_TEST_ADDR is not set")
			}

			// Create a client with pool size 10
			client, err := mg.NewClient(tc.uri, mg.WithPoolSize(10))
			require.NoError(t, err)
			client.Close()
			client.Close()
		})
	}

	// Shutdown at the end of the test to ensure the next test can start fresh
	mg.Shutdown()
}
