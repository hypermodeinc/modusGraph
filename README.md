<div align="center">

[![modus](https://github.com/user-attachments/assets/1a6020bd-d041-4dd0-b4a9-ce01dc015b65)](https://github.com/hypermodeinc/modusgraph)

[![GitHub License](https://img.shields.io/github/license/hypermodeinc/modusdb)](https://github.com/hypermodeinc/modusgraph?tab=Apache-2.0-1-ov-file#readme)
[![chat](https://img.shields.io/discord/1267579648657850441)](https://discord.gg/NJQ4bJpffF)
[![GitHub Repo stars](https://img.shields.io/github/stars/hypermodeinc/modusdb)](https://github.com/hypermodeinc/modusgraph/stargazers)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/hypermodeinc/modusdb)](https://github.com/hypermodeinc/modusgraph/commits/main/)

</div>

<p align="center">
   <a href="https://docs.hypermode.com/">Docs</a>
   <span> · </span>
   <a href="https://discord.gg/4z4GshR7fq">Discord</a>
<p>

**modusGraph is a high-performance, transactional database system.** It's designed to be type-first,
schema-agnostic, and portable. ModusGraph provides object-oriented APIs that make it simple to build
new apps, paired with support for advanced use cases through the Dgraph Query Language (DQL). A
dynamic schema allows for natural relations to be expressed in your data with performance that
scales with your use case.

modusGraph is available as a Go package for running in-process, providing low-latency reads, writes,
and vector searches. We’ve made trade-offs to prioritize speed and simplicity. When runnning
in-process, modusGraph internalizes Dgraph's server components, and data is written to a local
file-based database. modusGraph also supports remote Dgraph servers, allowing you deploy your apps
to any Dgraph cluster simply by changing the connection string.

The [modus framework](https://github.com/hypermodeinc/modus) is optimized for apps that require
sub-second response times. ModusGraph augments polyglot functions with simple to use data and vector
storage. When paired together, you can build a complete AI semantic search or retrieval-augmented
generation (RAG) feature with a single framework.

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "time"

    mg "github.com/hypermodeinc/modusgraph"
)

type TestEntity struct {
    Name        string    `json:"name,omitempty" dgraph:"index=exact"`
    Description string    `json:"description,omitempty" dgraph:"index=term"`
    CreatedAt   time.Time `json:"createdAt,omitempty"`

    // UID is a required field for nodes
    UID string `json:"uid,omitempty"`
    // DType is a required field for nodes, will get populated with the struct name
    DType []string `json:"dgraph.type,omitempty"`
}

func main() {
    // Use a file URI to connect to a in-process modusGraph instance, ensure that the directory exists
    uri := "file:///tmp/modusgraph"
    // Assigning a Dgraph URI will connect to a remote Dgraph server
    // uri := "dgraph://localhost:9080"

    client, err := mg.NewClient(uri, mg.WithAutoSchema(true))
    if err != nil {
        panic(err)
    }
    defer client.Close()

    entity := TestEntity{
        Name:        "Test Entity",
        Description: "This is a test entity",
        CreatedAt:   time.Now(),
    }

    ctx := context.Background()
    err = client.Insert(ctx, &entity)

    if err != nil {
        panic(err)
    }
    fmt.Println("Insert successful, entity UID:", entity.UID)

    // Query the entity
    var result TestEntity
    err = client.Get(ctx, &result, entity.UID)
    if err != nil {
        panic(err)
    }
    fmt.Println("Query successful, entity:", result.UID)
}
```

## Creating a Client

The `NewClient` function takes a URI and optional configuration options.

```go
client, err := mg.NewClient(uri)
if err != nil {
    panic(err)
}
defer client.Close()
```

### URI Options

modusGraph supports two URI schemes for managing graph databases:

#### `file://` - Local File-Based Database

Connects to a database stored locally on the filesystem. This mode doesn't require a separate
database server and is perfect for development, testing, or embedded applications. The directory
must exist before connecting.

File-based databases do not support concurrent access from separate processes. Further, there can
only be one file-based client per process.

```go
// Connect to a local file-based database
client, err := mg.NewClient("file:///path/to/data")
```

#### `dgraph://` - Remote Dgraph Server

Connects to a Dgraph cluster. For more details on the Dgraph URI format, see the
[Dgraph Dgo documentation](https://github.com/hypermodeinc/dgo#connection-strings).

```go
// Connect to a remote Dgraph server
client, err := mg.NewClient("dgraph://hostname:9080")
```

You can have multiple remote clients per process provided the URIs are distinct.

### Configuration Options

modusGraph provides several configuration options that can be passed to the `NewClient` function:

#### WithAutoSchema(bool)

Enables or disables automatic schema management. When enabled, modusGraph will automatically create
and update the graph database schema based on the struct tags of objects you insert.

```go
// Enable automatic schema management
client, err := mg.NewClient(uri, mg.WithAutoSchema(true))
```

#### WithPoolSize(int)

Sets the size of the connection pool for better performance under load. The default is 10
connections.

```go
// Set pool size to 20 connections
client, err := mg.NewClient(uri, mg.WithPoolSize(20))
```

#### WithMaxEdgeTraversal(int)

Sets the maximum number of edges to traverse when querying. The default is 10 edges.

```go
// Set max edge traversal to 20 edges
client, err := mg.NewClient(uri, mg.WithMaxEdgeTraversal(20))
```

#### WithLogger(logr.Logger)

Configures structured logging with custom verbosity levels. By default, logging is disabled.

```go
// Set up a logger
logger := logr.New(logr.Discard())
client, err := mg.NewClient(uri, mg.WithLogger(logger))
```

You can combine multiple options:

```go
// Using multiple configuration options
client, err := mg.NewClient(uri,
    mg.WithAutoSchema(true),
    mg.WithPoolSize(20),
    mg.WithLogger(logger))
```

## Defining Your Graph with Structs

modusGraph uses Go structs to define your graph database schema. By adding `json` and `dgraph` tags
to your struct fields, you tell modusGraph how to store and index your data in the graph database.

### Basic Structure

Every struct that represents a node in your graph should include:

```go
type MyNode struct {
    // Your fields here with appropriate tags
    Name string `json:"name,omitempty" dgraph:"index=exact"`
    Description string `json:"description,omitempty" dgraph:"index=term"`
    CreatedAt time.Time `json:"createdAt,omitempty" dgraph:"index=day"`

    // These fields are required for Dgraph integration
    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}
```

### `dgraph` Field Tags

modusGraph uses struct tags to define how each field should be handled in the graph database:

| Directive   | Option   | Description                                     | Example                                                                              |
| ----------- | -------- | ----------------------------------------------- | ------------------------------------------------------------------------------------ |
| **index**   | exact    | Creates an exact-match index for string fields  | Name string &#96;json:"name" dgraph:"index=exact"&#96;                               |
|             | hash     | Creates a hash index (same as exact)            | Code string &#96;json:"code" dgraph:"index=hash"&#96;                                |
|             | term     | Creates a term index for text search            | Description string &#96;json:"description" dgraph:"index=term"&#96;                  |
|             | fulltext | Creates a full-text search index                | Content string &#96;json:"content" dgraph:"index=fulltext"&#96;                      |
|             | int      | Creates an index for integer fields             | Age int &#96;json:"age" dgraph:"index=int"&#96;                                      |
|             | geo      | Creates a geolocation index                     | Location &#96;json:"location" dgraph:"index=geo"&#96;                                |
|             | day      | Creates a day-based index for datetime fields   | Created time.Time &#96;json:"created" dgraph:"index=day"&#96;                        |
|             | year     | Creates a year-based index for datetime fields  | Birthday time.Time &#96;json:"birthday" dgraph:"index=year"&#96;                     |
|             | month    | Creates a month-based index for datetime fields | Hired time.Time &#96;json:"hired" dgraph:"index=month"&#96;                          |
|             | hour     | Creates an hour-based index for datetime fields | Login time.Time &#96;json:"login" dgraph:"index=hour"&#96;                           |
|             | hnsw     | Creates a vector similarity index               | Vector \*dg.VectorFloat32 &#96;json:"vector" dgraph:"index=hnsw(metric:cosine)"&#96; |
| **type**    | geo      | Specifies a geolocation field                   | Location &#96;json:"location" dgraph:"type=geo"&#96;                                 |
|             | datetime | Specifies a datetime field                      | CreatedAt time.Time &#96;json:"createdAt" dgraph:"type=datetime"&#96;                |
|             | int      | Specifies an integer field                      | Count int &#96;json:"count" dgraph:"type=int"&#96;                                   |
|             | float    | Specifies a floating-point field                | Price float64 &#96;json:"price" dgraph:"type=float"&#96;                             |
|             | bool     | Specifies a boolean field                       | Active bool &#96;json:"active" dgraph:"type=bool"&#96;                               |
|             | password | Specifies a password field (stored securely)    | Password string &#96;json:"password" dgraph:"type=password"&#96;                     |
| **count**   |          | Creates a count index                           | Visits int &#96;json:"visits" dgraph:"count"&#96;                                    |
| **unique**  |          | Enforces uniqueness for the field               | Email string &#96;json:"email" dgraph:"index=hash unique"&#96;                       |
| **upsert**  |          | Allows a field to be used in upsert operations  | UserID string &#96;json:"userID" dgraph:"index=hash upsert"&#96;                     |
| **reverse** |          | Creates a bidirectional edge                    | Friends []\*Person &#96;json:"friends" dgraph:"reverse"&#96;                         |
| **lang**    |          | Enables multi-language support for the field    | Description string &#96;json:"description" dgraph:"lang"&#96;                        |

### Relationships

Relationships between nodes are defined using struct pointers or slices of struct pointers:

```go
type Person struct {
    Name     string    `json:"name,omitempty" dgraph:"index=exact upsert"`
    Friends  []*Person `json:"friends,omitempty"`
    Manager  *Person   `json:"manager,omitempty"`

    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}
```

### Reverse Edges

Reverse edges allow efficient bidirectional traversal. When you query in the reverse direction, use
the tilde prefix in your JSON tag:

```go
type Student struct {
    Name       string   `json:"name,omitempty" dgraph:"index=exact"`
    Takes_Class []*Class `json:"takes_class,omitempty" dgraph:"reverse"`

    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}

type Class struct {
    Name     string     `json:"name,omitempty" dgraph:"index=exact"`
    Students []*Student `json:"~takes_class,omitempty"` // Reverse edge

    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}
```

Advanced querying is required to properly bind reverse edges in query results. See the
`TestReverseEdgeQuery` test in [query_test.go](./query_test.go) for an example.

## Basic Operations

modusGraph provides a simple API for common database operations.

Note that in local-mode, unique fields are limited to the top-level object. Fields marked as unique
in embedded or lists of embedded objects that have `unique` tags will not be checked for uniqueness
when the top-level object is inserted.

### Inserting Data

To insert a new node into the database:

```go
ctx := context.Background()

// Create a new object
user := User{
    Name:  "John Doe",
    Email: "john@example.com",
    Role:  "Admin",
}

// Insert it into the database
err := client.Insert(ctx, &user)
if err != nil {
    log.Fatalf("Failed to create user: %v", err)
}

// The UID field will be populated after insertion
fmt.Println("Created user with UID:", user.UID)
```

### Upserting Data

modusGraph provides a simple API for upserting data into the database.

Note that in local-mode, upserts are only supported on the top-level object. Fields in embedded or
lists of embedded objects that have `upsert` tags will be ignored when the top-level object is
upserted.

```go
ctx := context.Background()

user := User{
    Name:  "John Doe", // this field has the `upsert` tag
    Email: "john@example.com",
    Role:  "Admin",
}

// Upsert the user into the database
// If "John Doe" does not exist, it will be created
// If "John Doe" exists, it will be updated
err := client.Upsert(ctx, &user)
if err != nil {
    log.Fatalf("Failed to upsert user: %v", err)
}

```

### Updating Data

To update an existing node, first retrieve it, modify it, then save it back.

Note that in local-mode, unique update checks are only supported on the top-level object. Fields in
embedded or lists of embedded objects that have `unique` tags will not be checked for uniqueness
when the top-level object is updated.

```go
ctx := context.Background()

// Get the existing object by UID
var user User
err := client.Get(ctx, &user, "0x1234")
if err != nil {
    log.Fatalf("Failed to get user: %v", err)
}

// Modify fields
user.Name = "Jane Doe"
user.Role = "Manager"

// Save the changes
err = client.Update(ctx, &user)
if err != nil {
    log.Fatalf("Failed to update user: %v", err)
}
```

### Deleting Data

To delete one or more nodes from the database:

```go
ctx := context.Background()

// Delete by UID
err := client.Delete(ctx, []string{"0x1234", "0x5678"})
if err != nil {
    log.Fatalf("Failed to delete node: %v", err)
}
```

### Querying Data

modusGraph provides a basic query API for retrieving data:

```go
ctx := context.Background()

// Basic query to get all users
var users []User
err := client.Query(ctx, User{}).Nodes(&users)
if err != nil {
    log.Fatalf("Failed to query users: %v", err)
}

// Query with filters
var adminUsers []User
err = client.Query(ctx, User{}).
    Filter(`eq(role, "Admin")`).
    Nodes(&adminUsers)
if err != nil {
    log.Fatalf("Failed to query admin users: %v", err)
}

// Query with pagination
var pagedUsers []User
err = client.Query(ctx, User{}).
    Filter(`has(name)`).
    Offset(10).
    Limit(5).
    Nodes(&pagedUsers)
if err != nil {
    log.Fatalf("Failed to query paged users: %v", err)
}

// Query with ordering
var sortedUsers []User
err = client.Query(ctx, User{}).
    Order("name").
    Nodes(&sortedUsers)
if err != nil {
    log.Fatalf("Failed to query sorted users: %v", err)
}
```

### Advanced Querying

modusGraph is built on top of the [dgman](https://github.com/dolan-in/dgman) package, which provides
access to Dgraph's more powerful and complete query capabilities. For advanced use cases, you can
access the underlying Dgraph client directly and construct more sophisticated queries:

```go
// Define a struct with vector field for similarity search
type Product struct {
    Name        string            `json:"name,omitempty" dgraph:"index=term"`
    Description string            `json:"description,omitempty"`
    Vector      *dg.VectorFloat32 `json:"vector,omitempty" dgraph:"index=hnsw(metric:cosine)"`

    UID   string   `json:"uid,omitempty"`
    DType []string `json:"dgraph.type,omitempty"`
}

// Get similar products using vector similarity search
func getSimilarProducts(client mg.Client, embeddings []float32) (*Product, error) {
    ctx := context.Background()

    // Convert vector to string format for query
    vectorStr := fmt.Sprintf("%v", embeddings)
    vectorStr = strings.Trim(strings.ReplaceAll(vectorStr, " ", ", "), "[]")

    // Create result variable
    var result Product

    // Get access to the underlying Dgraph client
    dgo, cleanup, err := client.DgraphClient()
    if err != nil {
        return nil, err
    }
    defer cleanup()

    // Construct query using similar_to function with a parameter for the vector
    query := dg.NewQuery().Model(&result).RootFunc("similar_to(vector, 1, $vec)")

    // Execute query with variables
    tx := dg.NewReadOnlyTxn(dgo)
    err = tx.Query(query).
        Vars("similar_to($vec: string)", map[string]string{"$vec": vectorStr}).
        Scan()

    if err != nil {
        return nil, err
    }

    return &result, nil
}
```

This example demonstrates vector similarity search for finding semantically similar items - a
powerful feature in Dgraph. You can also access other advanced capabilities like full-text search
with language-specific analyzers, geolocation queries, and more. The ability to access the raw
Dgraph client gives you the full power of Dgraph's query language while still benefiting from
modusGraph's simplified client interface and schema management.

## Schema Management

modusGraph provides robust schema management features that simplify working with Dgraph's schema
system.

### AutoSchema

The AutoSchema feature automatically generates and updates the database schema based on your Go
struct definitions. When enabled, modusGraph will analyze the struct tags of objects you insert and
ensure the appropriate schema exists in the database.

Enable AutoSchema when creating a client:

```go
// Enable automatic schema management
client, err := mg.NewClient(uri, mg.WithAutoSchema(true))
if err != nil {
    log.Fatalf("Failed to create client: %v", err)
}

// Now you can insert objects without manually creating the schema first
user := User{
    Name:  "John Doe",
    Email: "john@example.com",
}

// The schema will be automatically created or updated as needed
err = client.Insert(ctx, &user)
```

With AutoSchema enabled, modusGraph will:

1. Analyze the struct tags of objects being inserted
2. Generate the appropriate Dgraph schema based on these tags
3. Apply any necessary schema updates to the database
4. Handle type definitions for node types based on struct names

This is particularly useful during development when your schema is evolving frequently.

### Schema Operations

For more control over schema management, modusGraph provides several methods in the Client
interface:

#### UpdateSchema

Manually update the schema based on one or more struct types:

```go
// Update schema based on User and Post structs
err := client.UpdateSchema(ctx, User{}, Post{})
if err != nil {
    log.Fatalf("Failed to update schema: %v", err)
}
```

This is useful when you want to ensure the schema is created before inserting data, or when you need
to update the schema for new struct types.

#### GetSchema

Retrieve the current schema definition from the database:

```go
// Get the current schema
schema, err := client.GetSchema(ctx)
if err != nil {
    log.Fatalf("Failed to get schema: %v", err)
}

fmt.Println("Current schema:")
fmt.Println(schema)
```

The returned schema is in Dgraph Schema Definition Language format.

#### DropAll and DropData

Reset the database completely or just clear the data:

```go
// Remove all data but keep the schema
err := client.DropData(ctx)
if err != nil {
    log.Fatalf("Failed to drop data: %v", err)
}

// Or remove both schema and data
err = client.DropAll(ctx)
if err != nil {
    log.Fatalf("Failed to drop all: %v", err)
}
```

These operations are useful for testing or when you need to reset your database state.

## Limitations

modusGraph has a few limitations to be aware of:

- **Unique constraints in file-based mode**: Due to the intricacies of how Dgraph handles unique
  fields in its core package, when using file-based mode, unique field checks are only supported at
  the top level object that is being in/upserted or updated. Embedded or lists of embedded objects
  that have unique tags will NOT be checked for uniqueness when the top-level object is in/upserted
  or updated.

- **Upsert operations**: Upsert operations are only supported on the top-level object. Fields in
  embedded or lists of embedded objects that have upsert tags will be ignored when the top-level
  object is upserted.

- **Schema evolution**: While modusGraph supports schema inference through tags, evolving an
  existing schema with new fields requires careful consideration to avoid data inconsistencies.

## CLI Commands and Examples

modusGraph provides several command-line tools and example applications to help you interact with
and explore the package. These are organized in the `cmd` and `examples` folders:

### Commands (`cmd` folder)

- **`cmd/query`**: A flexible CLI tool for running arbitrary DQL (Dgraph Query Language) queries
  against a modusGraph database.
  - Reads a query from standard input and prints JSON results.
  - Supports file-based modusGraph storage.
  - Flags: `--dir`, `--pretty`, `--timeout`, `-v` (verbosity).
  - See [`cmd/query/README.md`](./cmd/query/README.md) for usage and examples.

### Examples (`examples` folder)

- **`examples/basic`**: Demonstrates CRUD operations for a simple `Thread` entity.

  - Flags: `--dir`, `--addr`, `--cmd`, `--author`, `--name`, `--uid`, `--workspace`.
  - Supports create, update, delete, get, and list commands.
  - See [`examples/basic/README.md`](./examples/basic/README.md) for details.

- **`examples/load`**: Shows how to load the standard 1million RDF dataset into modusGraph for
  benchmarking.

  - Downloads, initializes, and loads the dataset into a specified directory.
  - Flags: `--dir`, `--verbosity`.
  - See [`examples/load/README.md`](./examples/load/README.md) for instructions.

You can use these tools as starting points for your own applications or as references for
integrating modusGraph into your workflow.

## Open Source

The modus framework, including modusGraph, is developed by [Hypermode](https://hypermode.com/) as an
open-source project, integral but independent from Hypermode.

We welcome external contributions. See the [CONTRIBUTING.md](./CONTRIBUTING.md) file if you would
like to get involved.

Modus and its components are © Hypermode Inc., and licensed under the terms of the Apache License,
Version 2.0. See the [LICENSE](./LICENSE) file for a complete copy of the license. If you have any
questions about modus licensing, or need an alternate license or other arrangement, please contact
us at <hello@hypermode.com>.

## Windows Users

modusGraph (and its dependencies) are designed to work on POSIX-compliant operating systems, and are
not guaranteed to work on Windows.

Tests at the top level folder (`go test .`) on Windows are maintained to pass on Windows, but other
tests in subfolders may not work as expected.

Temporary folders created during tests may not be cleaned up properly on Windows. Users should
periodically clean up these folders. The temporary folders are created in the Windows temp
directory, `C:\Users\<username>\AppData\Local\Temp\modusgraph_test*`.

## Acknowledgements

modusGraph builds heavily upon packages from the open source projects of
[Dgraph](https://github.com/hypermodeinc/dgraph) (graph query processing and transaction
management), [Badger](https://github.com/dgraph-io/badger) (data storage), and
[Ristretto](https://github.com/dgraph-io/ristretto) (cache). modusGraph also relies on the
[dgman](https://github.com/dolan-in/dgman) repository for much of its functionality. We expect the
architecture and implementations of modusGraph and Dgraph to expand in differentiation over time as
the projects optimize for different core use cases, while maintaining Dgraph Query Language (DQL)
compatibility.
