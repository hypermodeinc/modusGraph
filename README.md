<div align="center">

[![modus](https://github.com/user-attachments/assets/1a6020bd-d041-4dd0-b4a9-ce01dc015b65)](https://github.com/hypermodeinc/modusdb)

[![GitHub License](https://img.shields.io/github/license/hypermodeinc/modusdb)](https://github.com/hypermodeinc/modusdb?tab=Apache-2.0-1-ov-file#readme)
[![chat](https://img.shields.io/discord/1267579648657850441)](https://discord.gg/NJQ4bJpffF)
[![GitHub Repo stars](https://img.shields.io/github/stars/hypermodeinc/modusdb)](https://github.com/hypermodeinc/modusdb/stargazers)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/hypermodeinc/modusdb)](https://github.com/hypermodeinc/modusdb/commits/main/)

</div>

<p align="center">
   <a href="https://docs.hypermode.com/">Docs</a>
   <span> · </span>
   <a href="https://discord.gg/4z4GshR7fq">Discord</a>
<p>

**ModusDB is a high-performance, transactional database system.** It's designed to be type-first,
schema-agnostic, and portable. ModusDB provides object-oriented APIs that makes it simple to build
new apps, paired with support for advanced use cases through the Dgraph Query Language (DQL). A
dynamic schema allows for natural relations to be expressed in your data with performance that
scales with your use case.

ModusDB is available as a Go package for running in-process, providing low-latency reads, writes,
and vector searches. We’ve made trade-offs to prioritize speed and simplicity.

The [modus framework](https://github.com/hypermodeinc/modus) is optimized for apps that require
sub-second response times. ModusDB augments polyglot functions with simple to use data and vector
storage. When paired together, you can build a complete AI semantic search or retrieval-augmented
generation (RAG) feature with a single framework.

## Quickstart

```go
package main

import (
  "github.com/hypermodeinc/modusdb"
)

type User struct {
  Gid  uint64 `json:"gid,omitempty"`
  Id   string `json:"id,omitempty" db:"constraint=unique"`
  Name string `json:"name,omitempty"`
  Age  int    `json:"age,omitempty"`
}

func main() {
  engine, err := modusdb.NewEngine(modusdb.NewDefaultConfig("/local/modusdb"))
  if err != nil {
    panic(err)
  }
  defer engine.Close()

  gid, user, err := modusdb.Upsert(ns, User{
    Id:   "123",
    Name: "A",
    Age:  10,
  })
  if err != nil {
    panic(err)
  }
  fmt.Println(user)

  _, queriedUser, err := modusdb.Get[User](ns, gid)
  if err != nil {
    panic(err)
  }
  fmt.Println(queriedUser)

  _, _, err = modusdb.Delete[User](ns, gid)
  if err != nil {
    panic(err)
  }
}
```

## Open Source

The modus framework, including modusDB, is developed by [Hypermode](https://hypermode.com/) as an
open-source project, integral but independent from Hypermode.

We welcome external contributions. See the [CONTRIBUTING.md](./CONTRIBUTING.md) file if you would
like to get involved.

Modus and its components are © Hypermode Inc., and licensed under the terms of the Apache License,
Version 2.0. See the [LICENSE](./LICENSE) file for a complete copy of the license. If you have any
questions about modus licensing, or need an alternate license or other arrangement, please contact
us at <hello@hypermode.com>.

## Acknowledgements

ModusDB builds heavily upon packages from the open source projects of
[Dgraph](https://github.com/hypermodeinc/dgraph) (graph query processing and transaction
management), [Badger](https://github.com/dgraph-io/badger) (data storage), and
[Ristretto](https://github.com/dgraph-io/ristretto) (cache). We expect the architecture and
implementations of modusDB and Dgraph to expand in differentiation over time as the projects
optimize for different core use cases, while maintaining Dgraph Query Language (DQL) compatibility.
