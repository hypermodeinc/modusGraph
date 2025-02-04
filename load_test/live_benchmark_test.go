/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package load_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/hypermodeinc/modusdb"
	"github.com/stretchr/testify/require"
)

func BenchmarkDatabaseOperations(b *testing.B) {
	setupProfiler := func(b *testing.B) *os.File {
		f, err := os.Create("cpu_profile.prof")
		if err != nil {
			b.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			b.Fatal("could not start CPU profiling: ", err)
		}
		return f
	}

	reportMemStats := func(b *testing.B, initialAlloc uint64) {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		b.ReportMetric(float64(ms.Alloc-initialAlloc)/float64(b.N), "bytes/op")
		b.ReportMetric(float64(ms.NumGC), "total-gc-cycles")
	}

	b.Run("DropAndLoad", func(b *testing.B) {
		f := setupProfiler(b)
		defer f.Close()
		defer pprof.StopCPUProfile()

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		initialAlloc := ms.Alloc

		engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig(b.TempDir()))
		require.NoError(b, err)
		defer engine.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dataFolder := b.TempDir()
			schemaFile := filepath.Join(dataFolder, "data.schema")
			dataFile := filepath.Join(dataFolder, "data.rdf")
			require.NoError(b, os.WriteFile(schemaFile, []byte(DbSchema), 0600))
			require.NoError(b, os.WriteFile(dataFile, []byte(SmallData), 0600))
			require.NoError(b, engine.Load(context.Background(), schemaFile, dataFile))
		}
		reportMemStats(b, initialAlloc)
	})

	b.Run("Query", func(b *testing.B) {
		f := setupProfiler(b)
		defer f.Close()
		defer pprof.StopCPUProfile()

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		initialAlloc := ms.Alloc

		// Setup database with data once
		engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig(b.TempDir()))
		require.NoError(b, err)
		defer engine.Close()

		dataFolder := b.TempDir()
		schemaFile := filepath.Join(dataFolder, "data.schema")
		dataFile := filepath.Join(dataFolder, "data.rdf")
		require.NoError(b, os.WriteFile(schemaFile, []byte(DbSchema), 0600))
		require.NoError(b, os.WriteFile(dataFile, []byte(SmallData), 0600))
		require.NoError(b, engine.Load(context.Background(), schemaFile, dataFile))

		const query = `{
            caro(func: allofterms(name@en, "Marc Caro")) {
                name@en
                director.film {
                    name@en
                }
            }
        }`
		const expected = `{
            "caro": [
                {
                    "name@en": "Marc Caro",
                    "director.film": [
                        {
                            "name@en": "Delicatessen"
                        },
                        {
                            "name@en": "The City of Lost Children"
                        }
                    ]
                }
            ]
        }`

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := engine.GetDefaultNamespace().Query(context.Background(), query)
			require.NoError(b, err)
			require.JSONEq(b, expected, string(resp.Json))
		}
		reportMemStats(b, initialAlloc)
	})
}
