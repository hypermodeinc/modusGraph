/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph_test

import (
	"context"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/go-logr/stdr"
	mg "github.com/hypermodeinc/modusgraph"
	"github.com/stretchr/testify/require"
)

// CreateTestClient creates a new ModusGraph client for testing purposes with a configured logger.
// It returns the client and a cleanup function that should be deferred by the caller.
func CreateTestClient(t *testing.T, uri string) (mg.Client, func()) {

	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	logger := stdr.NewWithOptions(stdLogger, stdr.Options{LogCaller: stdr.All}).WithName("mg")
	verbosity := os.Getenv("MODUSGRAPH_TEST_LOG_LEVEL")
	if verbosity == "" {
		stdr.SetVerbosity(0)
	} else {
		level, err := strconv.Atoi(verbosity)
		if err != nil {
			stdr.SetVerbosity(0)
		} else {
			stdr.SetVerbosity(level)
		}
	}

	client, err := mg.NewClient(uri, mg.WithAutoSchema(true), mg.WithLogger(logger))
	require.NoError(t, err)

	cleanup := func() {
		err := client.DropAll(context.Background())
		if err != nil {
			t.Error(err)
		}
		client.Close()

		// Reset the singleton state so the next test can create a new engine
		mg.ResetSingleton()
	}

	return client, cleanup
}

// SetupTestEnv configures the environment variables for tests.
// This is particularly useful when debugging tests in an IDE.
func SetupTestEnv(logLevel int) {
	// Only set these if they're not already set in the environment
	if os.Getenv("MODUSGRAPH_TEST_ADDR") == "" {
		os.Setenv("MODUSGRAPH_TEST_ADDR", "localhost:9080")
	}
	if os.Getenv("MODUSGRAPH_TEST_LOG_LEVEL") == "" {
		// Uncomment to enable verbose logging during debugging
		os.Setenv("MODUSGRAPH_TEST_LOG_LEVEL", strconv.Itoa(logLevel))
	}
}
