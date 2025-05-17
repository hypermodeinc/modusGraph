/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/stdr"
	"github.com/hypermodeinc/modusgraph"
)

func main() {
	// Define flags
	dirFlag := flag.String("dir", "", "Directory where the modusGraph database is stored")
	prettyFlag := flag.Bool("pretty", true, "Pretty-print the JSON output")
	timeoutFlag := flag.Duration("timeout", 30*time.Second, "Query timeout duration")
	flag.Parse()

	// Initialize the stdr logger with the verbosity from -v
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	logger := stdr.NewWithOptions(stdLogger, stdr.Options{LogCaller: stdr.All}).WithName("mg")
	vFlag := flag.Lookup("v")
	if vFlag != nil {
		val, err := strconv.Atoi(vFlag.Value.String())
		if err != nil {
			log.Fatalf("Error: Invalid verbosity level: %s", vFlag.Value.String())
		}
		stdr.SetVerbosity(val)
	}

	// Validate required flags
	if *dirFlag == "" {
		log.Println("Error: --dir parameter is required")
		flag.Usage()
		os.Exit(1)
	}

	// Create clean directory path
	dirPath := filepath.Clean(*dirFlag)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		log.Fatalf("Error: Directory %s does not exist", dirPath)
	}

	// Initialize modusGraph client with the directory where data is stored
	logger.V(1).Info("Initializing modusGraph client", "directory", dirPath)
	client, err := modusgraph.NewClient(fmt.Sprintf("file://%s", dirPath),
		modusgraph.WithLogger(logger))
	if err != nil {
		logger.Error(err, "Failed to initialize modusGraph client")
		os.Exit(1)
	}
	defer client.Close()

	// Read query from stdin
	reader := bufio.NewReader(os.Stdin)
	query := ""
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			logger.Error(err, "Error reading from stdin")
			os.Exit(1)
		}

		query += line

		if err == io.EOF {
			break
		}
	}

	query = strings.TrimSpace(query)
	if query == "" {
		logger.Error(nil, "Empty query provided")
		os.Exit(1)
	}

	logger.V(1).Info("Executing query", "query", query)

	// Set up context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
	defer cancel()

	start := time.Now()

	// Execute the query
	resp, err := client.QueryRaw(ctx, query, nil)
	if err != nil {
		logger.Error(err, "Query execution failed")
		os.Exit(1)
	}

	elapsed := time.Since(start)
	elapsedMs := float64(elapsed.Nanoseconds()) / 1e6
	logger.V(1).Info("Query completed", "elapsed_ms", elapsedMs)

	// Format and print the response
	if *prettyFlag {
		var data any
		if err := json.Unmarshal(resp, &data); err != nil {
			logger.Error(err, "Failed to parse JSON response")
			os.Exit(1)
		}

		prettyJSON, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			logger.Error(err, "Failed to format JSON response")
			os.Exit(1)
		}

		fmt.Println(string(prettyJSON))
	} else {
		fmt.Println(string(resp))
	}
}
