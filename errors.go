/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusgraph

import dg "github.com/dolan-in/dgman/v2"

// UniqueError represents an error that occurs when attempting to insert or update
// a node that would violate a unique constraint.
type UniqueError = dg.UniqueError
