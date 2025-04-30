# modusGraph 1Million Dataset Loader

This command-line application demonstrates how to load the 1million dataset into modusGraph. The
1million dataset consists of approximately one million RDF triples representing relationships
between various entities and is commonly used for benchmarking graph database performance.

## Requirements

- Go 1.24 or higher
- Approximately 500MB of disk space for the downloaded dataset
- Internet connection (to download the dataset files)

## Usage

```sh
# Navigate to the examples/load directory
cd examples/load

# Run directly
go run main.go --dir /path/to/data/directory

# Or build and then run
go build -o modusgraph-loader
./modusgraph-loader --dir /path/to/data/directory
```

### Command Line Options

```sh
Usage of ./modusgraph-loader:
  --dir string         Directory where modusGraph will initialize and store the 1million dataset (required)
  --verbosity int     Verbosity level (0-2) (default 1)
```

## How It Works

1. The application creates the specified directory if it doesn't exist
2. It initializes a modusGraph engine in that directory
3. Downloads the 1million schema and RDF data files from the Dgraph benchmarks repository
4. Drops any existing data in the modusGraph instance
5. Loads the schema and RDF data into the database
6. Provides progress and timing information

## Performance Considerations

- Loading the 1million dataset may take several minutes depending on your hardware
- The application sets a 30-minute timeout for the loading process
- Memory usage will peak during the loading process

## Using the Loaded Dataset

After loading is complete, you can use the database in other applications by initializing modusGraph
with the same directory:

```go
// Initialize modusGraph client with the same directory
client, err := mg.NewClient("file:///path/to/data/directory")
if err != nil {
    // handle error
}
defer client.Close()

// Now you can run queries against the 1million dataset
```

## Dataset Details

The 1million dataset represents:

- Films, directors, and actors
- Relationships between these entities
- Various properties like names, dates, and film details

This is a great dataset for learning and testing graph query capabilities.
