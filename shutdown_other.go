//go:build !windows
// +build !windows

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

// windowsCleanup is a no-op on non-Windows platforms
func windowsCleanup() {
	// No special cleanup needed on Unix-like systems
}
