/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/cavaliergopher/grab/v3"
	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	"github.com/hypermodeinc/modusgraph"
)

const (
	baseURL          = "https://github.com/hypermodeinc/dgraph-benchmarks/blob/main/data"
	oneMillionSchema = baseURL + "/1million.schema?raw=true"
	oneMillionRDF    = baseURL + "/1million.rdf.gz?raw=true"
)

func main() {
	// Parse command line arguments
	dirFlag := flag.String("dir", "", "Directory where modusGraph will initialize and store the 1million dataset")
	verbosityFlag := flag.Int("verbosity", 1, "Verbosity level (0-2)")

	// Parse command line arguments
	flag.Parse()

	// Validate required flags
	if *dirFlag == "" {
		fmt.Println("Error: --dir parameter is required")
		flag.Usage()
		os.Exit(1)
	}

	// Create and clean the directory path
	dirPath := filepath.Clean(*dirFlag)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		log.Printf("Error creating directory %s: %v", dirPath, err)
		os.Exit(1)
	}

	// Initialize standard logger with stdr
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	logger := stdr.NewWithOptions(stdLogger, stdr.Options{LogCaller: stdr.All}).WithName("mg")

	// Set verbosity level based on flag
	stdr.SetVerbosity(*verbosityFlag)

	logger.Info("Starting 1million dataset loader")
	start := time.Now()

	// Initialize modusGraph engine
	logger.Info("Initializing modusGraph engine", "directory", dirPath)
	conf := modusgraph.NewDefaultConfig(dirPath).WithLogger(logger)
	engine, err := modusgraph.NewEngine(conf)
	if err != nil {
		logger.Error(err, "Failed to initialize modusGraph engine")
		os.Exit(1)
	}
	defer engine.Close()

	logger.Info("modusGraph engine initialized successfully")

	// Download the schema and data files
	logger.Info("Downloading 1million schema and data files")
	tmpDir := filepath.Join(dirPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		logger.Error(err, "Failed to create temporary directory", "path", tmpDir)
		os.Exit(1)
	}

	// Download files with progress tracking
	schemaFile, err := downloadFile(logger, tmpDir, oneMillionSchema, "schema")
	if err != nil {
		logger.Error(err, "Failed to download schema file")
		os.Exit(1)
	}

	dataFile, err := downloadFile(logger, tmpDir, oneMillionRDF, "data")
	if err != nil {
		logger.Error(err, "Failed to download data file")
		os.Exit(1)
	}

	// Drop all existing data
	logger.Info("Dropping any existing data")
	if err := engine.DropAll(context.Background()); err != nil {
		logger.Error(err, "Failed to drop existing data")
		os.Exit(1)
	}

	// Load the schema and data
	logger.Info("Loading 1million dataset into modusGraph")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if err := engine.Load(ctx, schemaFile, dataFile); err != nil {
		logger.Error(err, "Failed to load data")
		os.Exit(1)
	}

	elapsed := time.Since(start).Round(time.Second)
	logger.Info("Successfully loaded 1million dataset", "elapsed", elapsed, "directory", dirPath)
}

func downloadFile(logger logr.Logger, dir, url, fileType string) (string, error) {
	logger.Info("Starting download", "fileType", fileType, "url", url)

	// Create a new client
	client := grab.NewClient()
	req, err := grab.NewRequest(dir, url)
	if err != nil {
		return "", fmt.Errorf("failed to create download request: %w", err)
	}

	// Start download
	resp := client.Do(req)

	// Start UI loop
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()

	lastProgress := 0.0
	for {
		select {
		case <-t.C:
			progress := 100 * resp.Progress()
			// Only log if progress has changed significantly
			if progress-lastProgress >= 10 || progress >= 99.9 && lastProgress < 99.9 {
				logger.V(1).Info("Download progress", "fileType", fileType, "progress", fmt.Sprintf("%.1f%%", progress))
				lastProgress = progress
			}
			// Still show on console for interactive feedback
			logger.Info(fmt.Sprintf("\r%s: %.1f%% complete", fileType, progress), "fileType", fileType, "progress", fmt.Sprintf("%.1f%%", progress))

		case <-resp.Done:
			// Download is complete
			size := formatBytes(resp.Size())
			logger.Info("Download complete", "fileType", fileType, "file", resp.Filename, "size", size)
			if err := resp.Err(); err != nil {
				return "", fmt.Errorf("download failed: %w", err)
			}
			return resp.Filename, nil
		}
	}
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
