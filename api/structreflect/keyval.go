/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package structreflect

type keyValue struct {
	key   string
	value any
}

func (kv *keyValue) Key() string {
	return kv.key
}

func (kv *keyValue) Value() any {
	return kv.value
}
