//go:build windows
// +build windows

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import (
	"fmt"
	"runtime"
	"time"
)

// windowsCleanup performs Windows-specific cleanup operations to ensure
// file handles are properly released before the temporary directory is removed.
// This is necessary because Windows handles file locks differently than Unix systems.
func windowsCleanup() {
	fmt.Println("⚠️ Performing Windows-specific cleanup")

	// Force multiple garbage collections to help release file handles
	for i := 0; i < 3; i++ {
		runtime.GC()
		// Small pause between GCs to allow finalizers to run
		time.Sleep(50 * time.Millisecond)
	}

	// Give the OS some extra time to actually release the file handles
	// This is particularly important for WAL files from Badger DB
	time.Sleep(200 * time.Millisecond)
}
