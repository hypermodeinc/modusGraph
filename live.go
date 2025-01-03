/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/dgo/v240/protos/api"
	"github.com/dgraph-io/dgraph/v24/chunker"
	"github.com/dgraph-io/dgraph/v24/filestore"
	"github.com/dgraph-io/dgraph/v24/x"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	maxRoutines       = 4
	batchSize         = 1000
	numBatchesInBuf   = 100
	progressFrequency = 5 * time.Second
)

type liveLoader struct {
	n *Namespace

	blankNodes map[string]string
	mutex      sync.RWMutex
}

func (n *Namespace) Load(ctx context.Context, schemaPath, dataPath string) error {
	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("error reading schema file [%v]: %w", schemaPath, err)
	}
	if err := n.AlterSchema(ctx, string(schemaData)); err != nil {
		return fmt.Errorf("error altering schema: %w", err)
	}

	if err := n.LoadData(ctx, dataPath); err != nil {
		return fmt.Errorf("error loading data: %w", err)
	}
	return nil
}

// TODO: Add support for CSV file
func (n *Namespace) LoadData(inCtx context.Context, dataDir string) error {
	fs := filestore.NewFileStore(dataDir)
	files := fs.FindDataFiles(dataDir, []string{".rdf", ".rdf.gz", ".json", ".json.gz"})
	if len(files) == 0 {
		return errors.Errorf("no data files found in [%v]", dataDir)
	}
	log.Printf("found %d data file(s) to process", len(files))

	// Here, we build a context tree so that we can wait for the goroutines towards the
	// end. This also ensures that we can cancel the context tree if there is an error.
	rootG, rootCtx := errgroup.WithContext(inCtx)
	procG, procCtx := errgroup.WithContext(rootCtx)
	procG.SetLimit(maxRoutines)

	// start a goroutine to do the mutations
	start := time.Now()
	nqudsProcessed := 0
	nqch := make(chan *api.Mutation, 10000)
	rootG.Go(func() error {
		ticker := time.NewTicker(progressFrequency)
		defer ticker.Stop()

		last := nqudsProcessed
		for {
			select {
			case <-rootCtx.Done():
				return rootCtx.Err()

			case <-ticker.C:
				elapsed := time.Since(start).Round(time.Second)
				rate := float64(nqudsProcessed-last) / progressFrequency.Seconds()
				log.Printf("Elapsed: %v, N-Quads: %d, N-Quads/s: %5.0f", x.FixedDuration(elapsed), nqudsProcessed, rate)
				last = nqudsProcessed

			case nqs, ok := <-nqch:
				if !ok {
					return nil
				}
				uids, err := n.Mutate(rootCtx, []*api.Mutation{nqs})
				if err != nil {
					return fmt.Errorf("error applying mutations: %w", err)
				}
				x.AssertTruef(len(uids) == 0, "no UIDs should be returned for live loader")
				nqudsProcessed += len(nqs.Set)
			}
		}
	})

	ll := &liveLoader{n: n, blankNodes: make(map[string]string)}
	for _, datafile := range files {
		procG.Go(func() error {
			return ll.processFile(procCtx, fs, datafile, nqch)
		})
	}

	// Wait until all the files are processed
	if errProcG := procG.Wait(); errProcG != nil {
		rootG.Go(func() error {
			return errProcG
		})
	}

	// close the channel and wait for the mutations to finish
	close(nqch)
	return rootG.Wait()
}

func (l *liveLoader) processFile(inCtx context.Context, fs filestore.FileStore,
	filename string, nqch chan *api.Mutation) error {

	log.Printf("processing data file [%v]", filename)

	rd, cleanup := fs.ChunkReader(filename, nil)
	defer cleanup()

	loadType := chunker.DataFormat(filename, "")
	if loadType == chunker.UnknownFormat {
		if isJson, err := chunker.IsJSONData(rd); err == nil {
			if isJson {
				loadType = chunker.JsonFormat
			} else {
				return errors.Errorf("unable to figure out data format for [%v]", filename)
			}
		}
	}

	g, ctx := errgroup.WithContext(inCtx)
	ck := chunker.NewChunker(loadType, batchSize)
	nqbuf := ck.NQuads()

	g.Go(func() error {
		buffer := make([]*api.NQuad, 0, numBatchesInBuf*batchSize)

		drain := func() {
			for len(buffer) > 0 {
				sz := batchSize
				if len(buffer) < batchSize {
					sz = len(buffer)
				}
				nqch <- &api.Mutation{Set: buffer[:sz]}
				buffer = buffer[sz:]
			}
		}

		loop := true
		for loop {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case nqs, ok := <-nqbuf.Ch():
				if !ok {
					loop = false
					break
				}
				if len(nqs) == 0 {
					continue
				}

				var err error
				for _, nq := range nqs {
					nq.Subject, err = l.uid(nq.Namespace, nq.Subject)
					if err != nil {
						return fmt.Errorf("error getting UID for subject: %w", err)
					}
					if len(nq.ObjectId) > 0 {
						nq.ObjectId, err = l.uid(nq.Namespace, nq.ObjectId)
						if err != nil {
							return fmt.Errorf("error getting UID for object: %w", err)
						}
					}
				}

				buffer = append(buffer, nqs...)
				if len(buffer) < numBatchesInBuf*batchSize {
					continue
				}
				drain()
			}
		}
		drain()
		return nil
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			chunkBuf, errChunk := ck.Chunk(rd)
			if errChunk != nil && errChunk != io.EOF {
				return fmt.Errorf("error chunking data: %w", errChunk)
			}
			if err := ck.Parse(chunkBuf); err != nil {
				return fmt.Errorf("error parsing chunk: %w", err)
			}
			// We do this here in case of io.EOF, so that we can flush the last batch.
			if errChunk != nil {
				break
			}
		}

		nqbuf.Flush()
		return nil
	})

	return g.Wait()
}

func (l *liveLoader) uid(ns uint64, val string) (string, error) {
	key := x.NamespaceAttr(ns, val)

	l.mutex.RLock()
	uid, ok := l.blankNodes[key]
	l.mutex.RUnlock()
	if ok {
		return uid, nil
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	uid, ok = l.blankNodes[key]
	if ok {
		return uid, nil
	}

	asUID, err := l.n.db.LeaseUIDs(1)
	if err != nil {
		return "", fmt.Errorf("error allocating UID: %w", err)
	}

	uid = fmt.Sprintf("%#x", asUID.StartId)
	l.blankNodes[key] = uid
	return uid, nil
}
