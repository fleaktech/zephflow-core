# Fleak ZephFlow

> A lightweight, high-performance framework for stateless data processing

ZephFlow is a streamlined framework designed to make stateless data processing simple, reliable, and efficient. It
provides an intuitive way to create and execute stateless data processing pipelines.

## Features

✨ **Simple by Design** - Intuitive APIs and clean abstractions to reduce complexity  
🚀 **High Performance** - Optimized for low-latency processing  
🔒 **Reliable** - Robust error handling with dead-letter queue support  
🧩 **Composable** - Build complex workflows from simple, reusable components  
🌐 **Flexible Deployment** - Run embedded in applications or as a standalone process  
🔍 **Data Validation** - Built-in assertion capabilities for data quality control

## Documentation

For detailed documentation about ZephFlow, please visit our [doc site](https://docs.fleak.ai/zephflow).

## Slack Community
Join the Slack Workspace [here](https://join.slack.com/t/fleak-hq/shared_invite/zt-361k9cnhf-9~mmjpOH1IbZfRxeXplfKA).

## Prerequisites
Java 21

## Parallel tests

The `lib` tests can use the shared JVM queue adapted from `dynamicapi`:

```sh
./gradlew :lib:test :lib:jacocoTestReport -PtestQueue=true -DtestForks=4
```

This requires Python 3 (standard library only), Java 21 and the same Docker services as
the existing integration tests. Each JVM takes another test class as soon as it finishes
the previous one; methods within a JVM run sequentially. `testForks` sets the number of
JVMs and defaults to the existing half-CPU setting. The current test heap and JVM arguments
are preserved.

The queue is opt-in. Without `-PtestQueue=true`, Gradle uses its normal test runner.
Filtered `--tests` runs, JUnit engine filters and `--test-dry-run` use the normal runner. Golden tests
and the separate 64 MiB low-memory tests keep their existing tasks and settings.

JUnit XML, HTML and JaCoCo reports keep their usual locations under `lib/build`.
Per-worker logs, execution timelines and run summaries are under `lib/build/test-queue/runs`.
Successful runs save class durations in `lib/build/test-queue/durations.json` for scheduling
later runs; `clean` removes this local history. Classes without recorded durations run first,
followed by known classes from longest to shortest.

The CI/CD workflow enables the queue with 4 JVMs, each using a 512 MiB maximum heap, on the
existing `ubuntu-latest` runner. Before the build, it copies the measured starting priorities
from `scripts/test-queue/durations.json` into the local history file. This gives fresh runners
a duration-based order on their first run. Successful runs update the copy under `build/`;
they do not modify the versioned starting priorities. The durations affect scheduling only,
not which tests run. Local runs remain opt-in.

Run the queue's regression tests without containers or external services:

```sh
./gradlew :lib:testQueueInfrastructure -PtestQueue=true
```

## Current Version
v0.4.4

Import Zephflow from Gradle
```groovy
implementation 'io.fleak.zephflow:runner:0.4.4'
```

Import Zephflow from Maven
```xml

<dependency>
  <groupId>io.fleak.zephflow</groupId>
  <artifactId>runner</artifactId>
  <version>0.4.4</version>
</dependency>
```


---

© 2025 Fleak Tech Inc.
