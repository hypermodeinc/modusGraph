# modusGraph Basic CLI Example

This command-line application demonstrates basic operations with modusGraph, a graph database
library. The example implements CRUD operations (Create, Read, Update, Delete) for a simple `Thread`
entity type.

## Requirements

- Go 1.24 or higher
- Access to either:
  - A local filesystem (for file-based storage)
  - A Dgraph cluster (for distributed storage)

## Installation

```bash
# Navigate to the examples/basic directory
cd examples/basic

# Run directly
go run main.go [options]

# Or build and then run
go build -o modusgraph-cli
./modusgraph-cli [options]
```

## Usage

```sh
Usage of ./main:
  --dir string       Directory where modusGraph will initialize, note the directory must exist and you must have write access
  --addr string      Hostname/port where modusGraph will access for I/O (if not using the dir flag)
  --cmd string       Command to execute: create, update, delete, get, list (default "create")
  --author string    Created by (for create and update)
  --name string      Name of the Thread (for create and update)
  --uid string       UID of the Thread (required for update, delete, and get)
  --workspace string Workspace ID (for create, update, and filter for list)
```

**Note**: You must provide either `--dir` (for file-based storage) or `--addr` (for Dgraph cluster)
parameter.

## Commands

### Create a Thread

```bash
go run main.go --dir /tmp/modusgraph-data --cmd create --name "New Thread" --workspace "workspace-123" --author "user-456"
```

**Note**: Due to the intricacies of how Dgraph handles unique fields and upserts in its core
package, unique field checks and upsert operations are not supported (yet) when using the local
(file-based) mode. These operations work properly when using a full Dgraph cluster (--addr option),
but the simplified file-based mode does not support the constraint enforcement mechanisms required
for uniqueness guarantees. The workaround here would be to check for the Thread name and workspace
ID before creating a new Thread.

### Update a Thread

```bash
go run main.go --dir /tmp/modusgraph-data --cmd update --uid "0x123" --name "Updated Thread" --workspace "workspace-123" --author "user-456"
```

### Get a Thread by UID

```bash
go run main.go --dir /tmp/modusgraph-data --cmd get --uid "0x123"
```

### Delete a Thread

```bash
go run main.go --dir /tmp/modusgraph-data --cmd delete --uid "0x123"
```

### List All Threads

```bash
go run main.go --dir /tmp/modusgraph-data --cmd list
```

### List Threads by Workspace

```bash
go run main.go --dir /tmp/modusgraph-data --cmd list --workspace "workspace-123"
```

## Using with Dgraph

To use with a Dgraph cluster instead of file-based storage:

```bash
go run main.go --addr localhost:9080 --cmd list
```

## Output Format

The application displays data in a tabular format:

- For single Thread retrieval (`get`), fields are displayed in a vertical layout
- For multiple Thread retrieval (`list`), records are displayed in a horizontal table

## Logging

The application uses structured logging with different verbosity levels. To see more detailed logs
including query execution, you can modify the `stdr.SetVerbosity(1)` line in the code to a higher
level.
