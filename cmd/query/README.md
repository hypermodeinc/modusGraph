# modusGraph Query CLI

This command-line tool allows you to run arbitrary DQL (Dgraph Query Language) queries against a
modusGraph database, either in local file-based mode or (optionally) against a remote
Dgraph-compatible endpoint.

## Requirements

- Go 1.24 or higher
- Access to a directory containing a modusGraph database (created by modusGraph)

## Installation

```bash
# Navigate to the cmd/query directory
cd cmd/query

# Run directly
go run main.go --dir /path/to/modusgraph [options]

# Or build and then run
go build -o modusgraph-query
./modusgraph-query --dir /path/to/modusgraph [options]
```

## Usage

The tool reads a DQL query from standard input and prints the JSON response to standard output.

```sh
Usage of ./main:
  --dir string     Directory where the modusGraph database is stored (required)
  --pretty         Pretty-print the JSON output (default true)
  --timeout        Query timeout duration (default 30s)
  -v int           Verbosity level for logging (e.g., -v=1, -v=2)
```

### Example: Querying the Graph

```bash
echo '{ q(func: has(name@en), first: 10) { id: uid name@en } }' | go run main.go --dir /tmp/modusgraph
```

### Example: With Verbose Logging

```bash
echo '{ q(func: has(name@en), first: 10) { id: uid name@en } }' | go run main.go --dir /tmp/modusgraph -v 1
```

### Example: Build and Run

```bash
go build -o modusgraph-query
cat query.dql | ./modusgraph-query --dir /tmp/modusgraph
```

## Notes

- The `--dir` flag is required and must point to a directory initialized by modusGraph.
- The query must be provided via standard input.
- Use the `-v` flag to control logging verbosity (higher values show more log output).
- Use the `--pretty=false` flag to disable pretty-printing of the JSON response.
- The tool logs query timing and errors to standard error.

## Example Output

```json
{
  "q": [
    { "id": "0x2", "name@en": "Ivan Sen" },
    { "id": "0x3", "name@en": "Peter Lord" }
  ]
}
```

---

For more advanced usage and integration, see the main [modusGraph documentation](../../README.md).
