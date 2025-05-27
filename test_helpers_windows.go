//go:build windows
// +build windows

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// CreateTestDir creates a test directory that works around the Windows file locking issue
// by creating a temporary directory structure that won't be automatically cleaned up by Go's
// testing framework. Instead, we'll just mark it with a timestamp and let it be cleaned up
// by Windows eventually or by an external cleanup process.
func CreateTestDir(t *testing.T) string {
	// Get the system temp directory
	tempDir := os.TempDir()

	// Create a uniquely named directory that we won't try to delete
	// Format: TestWin_TestName_UnixTimestamp
	dirName := fmt.Sprintf("TestWin_%s_%d", t.Name(), os.Now().UnixNano())
	testDir := filepath.Join(tempDir, dirName)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Log where this directory is for manual cleanup if needed
	t.Logf("Created Windows-specific test directory: %s", testDir)

	return testDir
}
