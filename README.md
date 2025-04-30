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

## Limitations

modusGraph has a few limitations to be aware of:

- **Unique constraints in file-based mode**: Due to the intricacies of how Dgraph handles unique
  fields and upserts in its core package, unique field checks and upsert operations are not
  supported (yet) when using the local (file-based) mode. These operations work properly when using
  a full Dgraph cluster, but the simplified file-based mode does not support the constraint
  enforcement mechanisms required for uniqueness guarantees.

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

## Acknowledgements

modusGraph builds heavily upon packages from the open source projects of
[Dgraph](https://github.com/hypermodeinc/dgraph) (graph query processing and transaction
management), [Badger](https://github.com/dgraph-io/badger) (data storage), and
[Ristretto](https://github.com/dgraph-io/ristretto) (cache). modusGraph also relies on the
[dgman](https://github.com/dolan-in/dgman) repository for much of its functionality. We expect the
architecture and implementations of modusGraph and Dgraph to expand in differentiation over time as
the projects optimize for different core use cases, while maintaining Dgraph Query Language (DQL)
compatibility.
