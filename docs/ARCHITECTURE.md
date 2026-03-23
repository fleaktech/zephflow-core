# Architecture

This document describes the high-level architecture of ZephFlow Core.

## Overview

ZephFlow lets teams build real-time and batch data pipelines by composing
reusable operators, without writing boilerplate glue code.

Processing logic is modeled as a DAG (directed acyclic graph) of commands.
A pipeline has source nodes that ingest data, intermediate nodes that transform
or filter it, and sink nodes that write results to external systems. The core
data type flowing through every node is `RecordFleakData`, a recursive,
JSON-compatible value model.

The project ships three ways to run a pipeline: a fluent Java SDK, a Spring Boot
HTTP service, and a CLI runner.

## Module Map

The codebase is a multi-module Gradle project. Dependency flows roughly
top-to-bottom in this list.

### `api/`

Pure interfaces and data model — no implementation logic.

- `OperatorCommand` — abstract base for every pipeline node. Lifecycle:
  `parseAndValidateArg → initialize → process/writeToSink → terminate`.
  Subclasses: `ScalarCommand` (transform/filter), `ScalarSinkCommand` (write to
  external system), `SourceCommand` (ingest data).
- `CommandFactory` — creates an `OperatorCommand` for a given node id and
  `JobContext`. Declares the command's `CommandType` (SOURCE, SINK,
  INTERMEDIATE_COMMAND).
- `CommandConfig` — marker interface for strongly-typed config DTOs parsed from
  the DAG definition.
- `FleakData` — recursive value type hierarchy: `RecordFleakData` (map),
  `ArrayFleakData` (list), and primitive wrappers for strings, numbers, and
  booleans. Includes custom Jackson serializers that skip intermediate
  Map/List allocation.
- `JobContext` — pipeline-level state: metric tags, log level, optional S3 DLQ
  config, credentials.
- `MetricClientProvider` — strategy interface for metrics backends (InfluxDB,
  Splunk, noop).
- `ExecutionContext` — per-command resource holder created during initialization.

### `runner/`

DAG compilation and execution engine. Depends on `lib/` (and transitively `api/`).

- `Dag<T>` — generic graph container with adjacency-list storage, O(1)
  node/edge lookups, and validation (connectivity, acyclicity, source/sink
  terminal constraints).
- `DagCompiler` — takes a `commandFactoryMap` and an
  `AdjacencyListDagDefinition`, validates structure, instantiates commands via
  factories, returns `Dag<OperatorCommand>`.
- `DagExecutor` — top-level orchestrator. Uses `ServiceLoader` to discover
  `CommandProvider` SPIs, builds the factory map, compiles the DAG, splits
  sources from the rest, and drives execution.
- `NoSourceDagRunner` — executes a compiled DAG for a given batch of events
  (sources already resolved). Routes events node-by-node, captures per-node
  errors in `ErrorOutput`, optionally writes failures to an S3 dead-letter
  queue, and returns a `DagResult`.
- `DagRunCounters` — tracks input/output/error counts and latency per node.

### `lib/`

All concrete command implementations. Depends on `api/` only.

Commands are organized by type — sources (Kafka, Kinesis, Splunk, file, stdin),
sinks (Kafka, S3, ClickHouse, Delta Lake, Databricks, Kinesis, stdout), and
intermediate commands (eval, assertion/filter, parser, SQL). Browse `lib/` for
the full set.

Subpackages of note:

- `lib/antlr/` — two ANTLR grammars: `EvalExpression.g4` (boolean/arithmetic
  expressions with path selection `$.field[0]`, case expressions, dict
  construction) and `ExpressionSQLParser.g4` (SELECT/WHERE/JOIN/GROUP BY over
  in-memory data).
- `lib/parser/` — extraction rules: Grok, Syslog (3164/5424), CEF, JSON, XML,
  CSV, key-value pairs, delimited text.
- `lib/serdes/` — serializers and deserializers per `EncodingType`
  (JSON_OBJECT_LINE, CSV, XML, etc.) with transparent compression (gzip,
  brotli, deflate).
- `lib/pathselect/` — `PathExpression` and `ValueExtractor` for navigating
  `FleakData` via JSON-path-like syntax.

### `sdk/`

Fluent API for building and running pipelines programmatically. Depends on
`runner/`.

- `ZephFlow` — immutable builder; each chained method returns a new instance.
  `buildDag()` walks the builder tree and emits an `AdjacencyListDagDefinition`.
  `execute()` runs a full pipeline with source; `process()` runs a sourceless
  pipeline on pre-fetched events.

### `httpstarter/`

Spring Boot REST service. Depends on `runner/`.

- `WorkflowController` — register/retrieve DAG definitions.
- `ExecutionController` — execute a registered workflow against a batch of
  events.
- Compiles DAGs on registration and caches compiled runners for reuse.

### `clistarter/`

CLI entry point. Depends on `runner/`.

- `Main` — parses args via `JobCliParser`, selects a `MetricClientProvider`,
  creates a `DagExecutor`, and calls `executeDag()`.
- Shadow-jar packaging for single-artifact deployment.

## Naming Conventions

The codebase follows a consistent `[Domain][Abstraction]` pattern. Use these
to navigate via symbol search:

- `*CommandFactory` — creates a command for a given node (e.g., `EvalCommandFactory`)
- `*CommandProvider` — SPI entry point that registers factories (e.g., `LibCommandProvider`)
- `*Config` — strongly-typed config DTO (e.g., `GrokExtractionConfig`)
- `*ConfigParser` — parses raw map into a config DTO (e.g., `EvalConfigParser`)
- `*ConfigValidator` — validates a parsed config DTO

## Architectural Invariants

**SPI-based command discovery.** New commands are registered by implementing
`CommandProvider` (in `runner.spi`) and declaring it in
`META-INF/services`. `DagExecutor.loadCommands()` aggregates all providers at
startup and rejects duplicate command names. While `runner` depends on `lib`
directly at compile time, SPI provides the extension point for external command
providers to plug in additional commands without modifying core modules.

**DAG validity.** `Dag.validate()` enforces: the graph is connected, acyclic,
every source node has no incoming edges, and every sink node has no outgoing
edges. Validation runs at compile time inside `DagCompiler`, before any command
is initialized.

**Config parsing pipeline.** Each command owns a `ConfigParser` (Map → typed
DTO) and a `ConfigValidator` (DTO → validated DTO). Both run at compile time
via `OperatorCommand.parseAndValidateArg()`, so misconfiguration is caught
before execution begins.

**FleakData is the universal data type.** Every event entering or leaving a
command is a `RecordFleakData`. Commands must not traffic in raw Maps or JSON
strings across node boundaries.

**Commands are stateless across batches.** A command must not carry mutable
state from one batch invocation to the next. All per-batch state lives in the
runner; all long-lived resources live in `ExecutionContext`.

**No cross-command implementation dependencies.** A command's implementation
must never import or call another command directly. Commands compose only
through DAG edges at runtime.

## Layer Boundaries

```
       httpstarter / clistarter            (entry points)
                    │
                    ▼
                   sdk                     (fluent builder, optional)
                    │
                    ▼
                  runner                   (DAG compile + execute)
                    │
                    ▼
                   lib                     (command implementations)
                    │
                    ▼
                   api                     (interfaces, data model)
```

`api` has zero internal dependencies. `lib` depends on `api`. `runner` depends
on `lib` (and transitively on `api`). `sdk` depends on `runner`. The starters
depend on `runner`.

## Cross-Cutting Concerns

**Metrics.** `DagRunCounters` instruments every node with input/output/error
counters and a latency stopwatch, tagged by env, service, node id, command
name, and calling user. The `MetricClientProvider` strategy routes metrics to
InfluxDB, Splunk, or noop.

**Error handling / Dead Letter Queue.** Per-event exceptions inside a command
are captured as `ErrorOutput` and aggregated in `DagResult.errorByStep`.
If `JobContext` carries an S3 DLQ config, `NoSourceDagRunner` writes failed
events to S3 rather than silently dropping them.

**Serialization.** `lib/serdes/` handles encoding/decoding at pipeline edges.
`EncodingType` selects the strategy. Compression is layered transparently.
`FleakData`'s custom Jackson serializers avoid double-conversion overhead.

**Thread safety.** Commands and `ExecutionContext` implementations must be safe
to share across threads. Runner state is per-batch, so a single runner instance
can be reused across concurrent invocations.
