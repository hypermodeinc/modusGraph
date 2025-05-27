/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph_test

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

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

		// Properly shutdown the engine and reset the singleton state
		mg.Shutdown()
	}

	return client, cleanup
}

// GetTempDir returns a temporary directory for testing purposes.
// It creates a unique directory for each test and registers a cleanup function to remove it.
// On Windows, it uses the standard temp directory and creates a unique directory for each test.
// On other platforms, it uses the standard toolchain TempDir function.
func GetTempDir(t *testing.T) string {
	if runtime.GOOS == "windows" {
		baseDir := os.TempDir()
		testName := t.Name()
		testName = strings.ReplaceAll(testName, "/", "_")
		testName = strings.ReplaceAll(testName, "\\", "_")
		testName = strings.ReplaceAll(testName, ":", "_")

		tempDir := filepath.Join(baseDir, "modusgraph_test_"+testName)

		err := os.MkdirAll(tempDir, 0755)
		if err != nil {
			t.Logf("Failed to create temp directory %s: %v, falling back to standard temp dir", tempDir, err)
			return os.TempDir()
		}

		t.Cleanup(func() {
			runtime.GC()
			time.Sleep(200 * time.Millisecond)

			if err := os.RemoveAll(tempDir); err != nil {
				t.Logf("Warning: failed to remove temp directory %s: %v", tempDir, err)
			}
		})

		return tempDir
	}
	return t.TempDir()
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
