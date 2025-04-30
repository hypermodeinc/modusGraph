package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	mg "github.com/hypermodeinc/modusgraph"
)

type Thread struct {
	Name        string `json:"name,omitempty" dgraph:"index=exact"`
	WorkspaceID string `json:"workspaceID,omitempty" dgraph:"index=exact"`
	CreatedBy   string `json:"createdBy,omitempty" dgraph:"index=exact"`

	UID   string   `json:"uid,omitempty"`
	DType []string `json:"dgraph.type,omitempty"`
}

func main() {
	// Define command line flags
	dirFlag := flag.String("dir", "", "Directory where modusGraph will initialize")
	addrFlag := flag.String("addr", "", "Hostname/port where modusGraph will access for I/O")

	// Command flags
	cmdFlag := flag.String("cmd", "create", "Command to execute: create, update, delete, get, list")
	uidFlag := flag.String("uid", "", "UID of the Thread (required for update, delete, and get)")
	nameFlag := flag.String("name", "", "Name of the Thread (for create and update)")
	workspaceFlag := flag.String("workspace", "", "Workspace ID (for create, update, and filter for list)")
	authorFlag := flag.String("author", "", "Created by (for create and update)")

	// Parse command line arguments
	flag.Parse()

	// Validate required flags - either dirFlag or addrFlag must be provided
	if *dirFlag == "" && *addrFlag == "" {
		fmt.Println("Error: either --dir or --addr parameter is required")
		flag.Usage()
		os.Exit(1)
	}

	// Validate command
	command := strings.ToLower(*cmdFlag)
	validCommands := map[string]bool{
		"create": true,
		"update": true,
		"delete": true,
		"get":    true,
		"list":   true,
	}

	if !validCommands[command] {
		fmt.Printf("Error: invalid command '%s'. Valid commands are: create, update, delete, get, list\n", command)
		flag.Usage()
		os.Exit(1)
	}

	// Validate UID for commands that require it
	if (command == "update" || command == "delete" || command == "get") && *uidFlag == "" {
		fmt.Printf("Error: --uid parameter is required for %s command\n", command)
		flag.Usage()
		os.Exit(1)
	}

	// Determine which parameter to use as the first argument to NewClient
	var endpoint string
	if *dirFlag != "" {
		// Using directory mode
		dirPath := filepath.Clean(*dirFlag)
		endpoint = fmt.Sprintf("file://%s", dirPath)
		fmt.Printf("Initializing modusGraph with directory: %s\n", endpoint)
	} else {
		// Using Dgraph cluster mode
		endpoint = fmt.Sprintf("dgraph://%s", *addrFlag)
		fmt.Printf("Initializing modusGraph with address: %s\n", endpoint)
	}

	// Initialize standard logger with stdr
	stdLogger := log.New(os.Stdout, "", log.LstdFlags)
	logger := stdr.NewWithOptions(stdLogger, stdr.Options{LogCaller: stdr.All}).WithName("mg")

	// Set verbosity level
	stdr.SetVerbosity(1)

	logger.Info("Logger initialized")

	// Initialize modusGraph client with logger
	client, err := mg.NewClient(endpoint,
		// Auto schema will update the schema each time a mutation event is received
		mg.WithAutoSchema(true),
		// Logger will log events to the console
		mg.WithLogger(logger))
	if err != nil {
		logger.Error(err, "Failed to initialize modusGraph client")
		os.Exit(1)
	}
	defer client.Close()

	logger.Info("modusGraph client initialized successfully")

	// Execute the requested command
	var cmdErr error
	switch command {
	case "create":
		cmdErr = createThread(client, logger, *nameFlag, *workspaceFlag, *authorFlag)
	case "update":
		cmdErr = updateThread(client, logger, *uidFlag, *nameFlag, *workspaceFlag, *authorFlag)
	case "delete":
		cmdErr = deleteThread(client, logger, *uidFlag)
	case "get":
		cmdErr = getThread(client, logger, *uidFlag)
	case "list":
		cmdErr = listThreads(client, logger, *workspaceFlag)
	}

	if cmdErr != nil {
		fmt.Printf("Command '%s' failed: %v\n", command, cmdErr)
		os.Exit(1)
	}

	logger.Info("Command completed successfully", "command", command)
}

// createThread creates a new Thread in the database. Note that this function does
// not check for existing threads with the same name and workspace ID.
func createThread(client mg.Client, logger logr.Logger, name, workspaceID, createdBy string) error {
	thread := Thread{
		Name:        name,
		WorkspaceID: workspaceID,
		CreatedBy:   createdBy,
	}

	ctx := context.Background()
	err := client.Insert(ctx, &thread)
	if err != nil {
		logger.Error(err, "Failed to create Thread")
		return err
	}

	logger.Info("Thread created successfully", "UID", thread.UID)
	fmt.Printf("Thread created successfully\nUID: %s\nName: %s\nWorkspaceID: %s\nCreatedBy: %s\n",
		thread.UID, thread.Name, thread.WorkspaceID, thread.CreatedBy)
	return nil
}

// updateThread updates an existing Thread in the database
func updateThread(client mg.Client, logger logr.Logger, uid, name, workspaceID, createdBy string) error {
	// First get the existing Thread
	ctx := context.Background()
	var thread Thread
	err := client.Get(ctx, &thread, uid)
	if err != nil {
		logger.Error(err, "Failed to get Thread for update", "UID", uid)
		return err
	}

	// Update fields if provided
	if name != "" {
		thread.Name = name
	}
	if workspaceID != "" {
		thread.WorkspaceID = workspaceID
	}
	if createdBy != "" {
		thread.CreatedBy = createdBy
	}

	// Save the updated Thread
	err = client.Update(ctx, &thread)
	if err != nil {
		logger.Error(err, "Failed to update Thread", "UID", uid)
		return err
	}

	logger.Info("Thread updated successfully", "UID", thread.UID)
	fmt.Printf("Thread updated successfully\nUID: %s\nName: %s\nWorkspaceID: %s\nCreatedBy: %s\n",
		thread.UID, thread.Name, thread.WorkspaceID, thread.CreatedBy)
	return nil
}

// truncateString truncates a string to the specified length and adds ellipsis if needed
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	// Truncate and add ellipsis
	return s[:maxLen-3] + "..."
}

// deleteThread deletes a Thread from the database
func deleteThread(client mg.Client, logger logr.Logger, uid string) error {
	ctx := context.Background()
	var thread Thread
	// First get the Thread to confirm it exists and to show what's being deleted
	err := client.Get(ctx, &thread, uid)
	if err != nil {
		logger.Error(err, "Failed to get Thread for deletion", "UID", uid)
		return err
	}

	// Now delete it
	err = client.Delete(ctx, []string{uid})
	if err != nil {
		logger.Error(err, "Failed to delete Thread", "UID", uid)
		return err
	}

	logger.Info("Thread deleted successfully", "UID", uid)
	fmt.Printf("Thread deleted successfully\nUID: %s\nName: %s\n", uid, thread.Name)
	return nil
}

// getThread retrieves a Thread by UID
func getThread(client mg.Client, logger logr.Logger, uid string) error {
	ctx := context.Background()
	var thread Thread
	err := client.Get(ctx, &thread, uid)
	if err != nil {
		logger.Error(err, "Failed to get Thread", "UID", uid)
		return err
	}

	logger.Info("Thread retrieved successfully", "UID", thread.UID)

	// Display thread in a tabular format
	fmt.Println("\nThread Details:")
	fmt.Printf("%-15s | %s\n", "Field", "Value")
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("%-15s | %s\n", "UID", thread.UID)
	fmt.Printf("%-15s | %s\n", "Name", thread.Name)
	fmt.Printf("%-15s | %s\n", "Workspace ID", thread.WorkspaceID)
	fmt.Printf("%-15s | %s\n", "Created By", thread.CreatedBy)
	fmt.Println()
	return nil
}

// listThreads retrieves all Threads
func listThreads(client mg.Client, logger logr.Logger, workspaceID string) error {
	ctx := context.Background()
	var threads []Thread

	// We'll apply filters in the query builder

	// Execute the query using the fluent API pattern
	queryBuilder := client.Query(ctx, Thread{})

	// Apply filter if workspaceID is provided
	if workspaceID != "" {
		queryBuilder = queryBuilder.Filter(fmt.Sprintf(`eq(workspaceID, %q)`, workspaceID))
	} else {
		queryBuilder = queryBuilder.Filter(`has(name)`)
	}

	// Execute the query and retrieve the nodes
	logger.V(2).Info("Executing query", "query", queryBuilder.String())
	err := queryBuilder.Nodes(&threads)
	if err != nil {
		logger.Error(err, "Failed to list Threads")
		return err
	}

	logger.Info("Threads listed successfully", "Count", len(threads))

	// Display threads in a tabular format
	if len(threads) > 0 {
		// Define column headers and widths
		fmt.Println("\nThreads:")
		fmt.Printf("%-10s | %-30s | %-30s | %-20s\n", "UID", "Name", "Workspace ID", "Created By")
		fmt.Println(strings.Repeat("-", 97))

		// Print each thread as a row
		for _, thread := range threads {
			// Truncate values if they're too long for display
			uid := truncateString(thread.UID, 8)
			name := truncateString(thread.Name, 28)
			workspace := truncateString(thread.WorkspaceID, 28)
			createdBy := truncateString(thread.CreatedBy, 18)

			fmt.Printf("%-10s | %-30s | %-30s | %-20s\n", uid, name, workspace, createdBy)
		}
		fmt.Println()
	}

	return nil
}
