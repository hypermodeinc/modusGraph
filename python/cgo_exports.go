/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package main

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/json"
	"sync"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/hypermodeinc/modusdb"
)

// We need this for c-shared buildmode
func main() {}

// Handle registry to safely store Go pointers
var (
	handleRegistry        = make(map[uint64]interface{})
	handleCounter  uint64 = 1
	handleMutex    sync.Mutex
)

// storeHandle safely stores a Go pointer and returns a handle
func storeHandle(v interface{}) uint64 {
	handleMutex.Lock()
	defer handleMutex.Unlock()
	handle := handleCounter
	handleCounter++
	handleRegistry[handle] = v
	return handle
}

// loadHandle safely retrieves a Go pointer from a handle
func loadHandle(handle uint64) interface{} {
	handleMutex.Lock()
	defer handleMutex.Unlock()
	return handleRegistry[handle]
}

// removeHandle removes a handle from the registry
func removeHandle(handle uint64) {
	handleMutex.Lock()
	defer handleMutex.Unlock()
	delete(handleRegistry, handle)
}

//export NewEngineC
func NewEngineC(dataDir *C.char, errPtr **C.char) C.uint64_t {
	engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig(C.GoString(dataDir)))
	if err != nil {
		*errPtr = C.CString(err.Error())
		return 0
	}
	return C.uint64_t(storeHandle(engine))
}

//export CreateNamespaceC
func CreateNamespaceC(engineHandle C.uint64_t, errPtr **C.char) C.uint64_t {
	engine := loadHandle(uint64(engineHandle)).(*modusdb.Engine)
	ns, err := engine.CreateNamespace()
	if err != nil {
		*errPtr = C.CString(err.Error())
		return 0
	}
	return C.uint64_t(storeHandle(ns))
}

//export GetNamespaceC
func GetNamespaceC(engineHandle C.uint64_t, nsID C.uint64_t, errPtr **C.char) C.uint64_t {
	engine := loadHandle(uint64(engineHandle)).(*modusdb.Engine)
	ns, err := engine.GetNamespace(uint64(nsID))
	if err != nil {
		*errPtr = C.CString(err.Error())
		return 0
	}
	return C.uint64_t(storeHandle(ns))
}

//export DropAllC
func DropAllC(engineHandle C.uint64_t, errPtr **C.char) {
	engine := loadHandle(uint64(engineHandle)).(*modusdb.Engine)
	err := engine.DropAll(context.Background())
	if err != nil {
		*errPtr = C.CString(err.Error())
	}
}

//export LoadC
func LoadC(engineHandle C.uint64_t, schemaPath *C.char, dataPath *C.char, errPtr **C.char) {
	engine := loadHandle(uint64(engineHandle)).(*modusdb.Engine)
	err := engine.Load(context.Background(), C.GoString(schemaPath), C.GoString(dataPath))
	if err != nil {
		*errPtr = C.CString(err.Error())
	}
}

//export LoadDataC
func LoadDataC(engineHandle C.uint64_t, dataDir *C.char, errPtr **C.char) {
	engine := loadHandle(uint64(engineHandle)).(*modusdb.Engine)
	err := engine.LoadData(context.Background(), C.GoString(dataDir))
	if err != nil {
		*errPtr = C.CString(err.Error())
	}
}

//export CloseC
func CloseC(engineHandle C.uint64_t) {
	engine := loadHandle(uint64(engineHandle)).(*modusdb.Engine)
	engine.Close()
	removeHandle(uint64(engineHandle))
}

//export GetNamespaceIDC
func GetNamespaceIDC(nsHandle C.uint64_t) C.uint64_t {
	ns := loadHandle(uint64(nsHandle)).(*modusdb.Namespace)
	return C.uint64_t(ns.ID())
}

//export DropDataC
func DropDataC(nsHandle C.uint64_t, errPtr **C.char) {
	ns := loadHandle(uint64(nsHandle)).(*modusdb.Namespace)
	err := ns.DropData(context.Background())
	if err != nil {
		*errPtr = C.CString(err.Error())
	}
}

//export AlterSchemaC
func AlterSchemaC(nsHandle C.uint64_t, schema *C.char, errPtr **C.char) {
	ns := loadHandle(uint64(nsHandle)).(*modusdb.Namespace)
	err := ns.AlterSchema(context.Background(), C.GoString(schema))
	if err != nil {
		*errPtr = C.CString(err.Error())
	}
}

//export MutateC
func MutateC(nsHandle C.uint64_t, mutations *C.char, resultPtr **C.char, errPtr **C.char) {
	ns := loadHandle(uint64(nsHandle)).(*modusdb.Namespace)

	// Parse mutations JSON string to []*api.Mutation
	var muts []*api.Mutation
	err := json.Unmarshal([]byte(C.GoString(mutations)), &muts)
	if err != nil {
		*errPtr = C.CString(err.Error())
		return
	}

	// Perform mutation
	result, err := ns.Mutate(context.Background(), muts)
	if err != nil {
		*errPtr = C.CString(err.Error())
		return
	}

	// Convert result to JSON
	jsonResult, err := json.Marshal(result)
	if err != nil {
		*errPtr = C.CString(err.Error())
		return
	}

	*resultPtr = C.CString(string(jsonResult))
}

//export QueryC
func QueryC(nsHandle C.uint64_t, query *C.char, resultPtr **C.char, errPtr **C.char) {
	ns := loadHandle(uint64(nsHandle)).(*modusdb.Namespace)

	response, err := ns.Query(context.Background(), C.GoString(query))
	if err != nil {
		*errPtr = C.CString(err.Error())
		return
	}

	// Convert response to JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		*errPtr = C.CString(err.Error())
		return
	}

	*resultPtr = C.CString(string(jsonResponse))
}
