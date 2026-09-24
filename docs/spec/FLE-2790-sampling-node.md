# FLE-2790 — Sampling node (keep 1-in-N, random pick, static rate)

## Why

Some streams are too loud to keep in full (health checks, `200` access logs, debug output), but
dropping them loses "how much traffic was there?". New node `sample`: a list of filter rules, each
with a constant N. Per rule, matched events are collected in batches of N; one **picked at random**
is kept and tagged with N (so a consumer can multiply back up), the other N−1 are dropped. Events no
rule matches pass through unchanged. Random rather than "first of every N" makes the kept event an
unbiased sample of its batch: a periodic pattern in the input (e.g. every 3rd event from the same
host) can't line up with a fixed position and skew what survives.

## Config

| Param | Required? | Default         | What it does |
|---|---|-----------------|---|
| `rules` | yes | —               | Non-empty ordered list of `{condition?, sampleRate}`. |
| `sampleRateField` | no | `"__sampled__"` | Top-level field that receives N (as an integer) on kept events; override to avoid clashing with an existing field. The value is a literal key, not a path: `"a.b"` writes a top-level key named `a.b` (read downstream as `$["a.b"]`). |

There are no other parameters.

**Conditions.** A zephflow eval expression (e.g. `$.status == 200`), evaluated with the existing
eval engine exactly as `filter` (`AssertionCommand`) does. It matches **only if the result is
boolean `true`**; any other result (including `null`, `1`, `"true"`) doesn't match. A condition
that **throws** doesn't match: the walk continues to the next rule.

**Validation (parse time).** `rules` non-empty. `sampleRate` a JSON integer, 1 ≤ N ≤ 2³¹−1, checked
on the raw value's type, so `3.0` and `"3"` are rejected (throttle's parser coerces them; see
FLE-2842). `condition`, if present, must compile. `sampleRateField`, if set, a non-blank string (empty or whitespace-only is rejected). A
rule without `condition` before other rules is allowed; the rules after it are unreachable.

## Behaviour

- **First match wins.** Walk `rules` top to bottom; the first whose `condition` matches (no
  `condition` = matches everything) takes the event. This keeps the outcome predictable and lets
  specific rules sit above a catch-all. No match → pass through immediately, untagged (no N was
  applied), touching no batch.
- **Per-rule batch of N.** Each rule has its own batch. A matched event joins it and produces no
  output by itself. When the batch reaches N, one of the N (each with probability 1/N) is emitted,
  the rest dropped, and the batch is emptied.
  - The kept event is emitted when the **N-th** event arrives, at that event's position within the
    `process()` call — so it can come after later pass-through events and other rules' output;
    output order ≠ input order.
  - N = 1: every matched event is emitted immediately.
- **Pick algorithm (fixed, so seeded output is defined by the spec): single-slot reservoir.** Per
  rule, hold one candidate and a count k:
  - 1st event of a batch: becomes the candidate, **no draw**;
  - k-th event, 2 ≤ k ≤ N: draw `rng.nextInt(k)`; `0` → it replaces the candidate;
  - k = N: after that event's draw (none when N = 1), emit the candidate, clear it, reset k to 0.

  Memory is one event per rule, independent of N and traffic. **One** random generator per node
  instance is shared by all rules, drawn in event-processing order.
- **Injectable randomness.** The command implements a new interface
  `RandomAware { void setRandom(RandomGenerator rng); }`, same pattern as `ClockAware`. The
  generator is a command field (not in the execution context) whose field initializer is
  `new java.util.Random()`; `parseAndValidateArg`/`initialize` never reassign it, so an injected
  generator survives. `GoldenRunner` injects `new Random(GOLDEN_SEED)` into
  `RandomAware` commands before `parseAndValidateArg`. No user-facing seed.
- **Tagging.** Every kept event gets `sampleRateField` = N, **including N = 1** (so "sampled at 1:1"
  differs from "never sampled"), overwriting any existing field of that name. The output is a copy
  of the input with the tag merged in (`copyAndMerge`, like throttle); input is never mutated. The
  held candidate is kept by reference, relying on the runner's no-mutation contract for events
  shared across fan-out branches.
- **Runner integration.** Implements `KeyedStatefulCommand` (no key, but it holds per-rule state
  across events), **not** `WindowFlushable` (see partial batches below). The marker makes
  `NoSourceDagRunner` run it under the pipeline lock and `DagRunnerService` reject it on the sync
  (request/response) path, like `throttle`: that backend reuses the runner across requests and has
  no flush scheduler, so batch state would leak between requests and a caller could get another
  request's event. Widen `DagRunnerService`'s rejection message ("windowed or throttle") to cover
  it.
- **Metrics.** Input and output counters plus a dropped counter (like throttle), each tagged from the
  event it counts. Every step k ≥ 2 discards exactly one event for good (the new one or the replaced
  candidate); the dropped counter is incremented then, with that event's tags (N−1 per full batch).
  A partial batch's candidate is never counted as dropped. A condition that throws increments no
  error counter.

## Out of scope / known limits

- **Dynamic N.** No adaptive rate, group key, or windows.
- **Partial batches are never emitted** — a design choice, although the runner has an end-of-stream
  hook (`WindowFlushable.flush(…, finalFlush=true)`): a batch of k < N tagged N overstates volume,
  and tagged k adds a second, varying rate to the rule. No time-based flush either: on a quiet
  stream the batch just waits, so a large N on low volume delays output a long time.
- **Delivery.** An open batch is in memory only; end-of-stream, shutdown, crash or restart loses it
  (≤ N−1 matched events per rule, of which ≤ 1 would have been emitted). A kept event is
  **at-most-once** if something downstream fails on it: with a DLQ, the source writes the raw source
  record containing the N-th event, not the kept one, and replay puts its events in a new batch, so
  the kept event is not recovered. Any record DLQ'd for a failure at any node is replayed whole: its
  pass-through events are emitted again and its matched events join a new batch a second time.
- **Volume estimate `kept × N`** undercounts by ≤ N−1 per rule per process lifetime (again on every
  restart or rescale) plus N per kept event lost downstream, and overcounts by the matched events of
  replayed DLQ records.
- **Per process, not global.** A rule's stream split across processes gets one batch per process;
  the overall ratio is approximate. No cross-process coordination.
- **`RawDataSampler`** (source-side raw-preview sampler) is neither changed nor replaced.

## Done = these tests pass

1. The golden fixtures in `lib/src/test/resources/golden/sample/` define the expected behaviour.
   Each fixture's README describes what it covers. They assume `GOLDEN_SEED = 42`. Do not edit
   them; the implementation must make them pass:

   ```
   ./gradlew :lib:goldenTest -Dgolden.filter=sample/
   ```

2. Every other test in the repo must still pass, with no test disabled or weakened.
   `./gradlew build` covers this: it runs every module's unit tests, spotless, and all golden
   fixtures (`check` depends on `:lib:goldenTest`).

3. Unit tests (in `lib`) and runner tests (in `runner`) cover what golden can't observe. Each of
   these must exist and pass:
   - **Validation:** each parse-time rejection listed under Config fails with an exception whose
     message names the offending parameter: missing or empty `rules`; missing or null `sampleRate`;
     `sampleRate` of `0`, 2³¹, `3.0` and `"3"`; a `condition` that doesn't compile (the compile
     error is wrapped so the message names `condition`); an empty, whitespace-only or
     non-string `sampleRateField`. Valid configs are accepted, including `sampleRate` of `1` and
     2³¹−1 and a rule without `condition`.
   - **Seed injection:** inject a scripted `RandomGenerator` stub through `RandomAware.setRandom`
     before `parseAndValidateArg`/`initialize` and assert the exact output of at least two batches
     with N ≥ 3. Two stubs are needed: one whose `nextInt` always returns `0` (each batch keeps its
     last event) and one whose `nextInt` always returns `1` (each batch keeps its first; draws only
     happen for k ≥ 2). This proves the injected generator is the one used.
   - **Multi-event order:** one `process()` call carries at least two rules with N ≥ 2, interleaved
     pass-through events, and a stub that keeps an earlier event of the batch. Assert the full
     output list: each kept event sits at the position of its batch's N-th event, and
     pass-through events keep their input positions.
   - **Marker and sync path:** the command is a `KeyedStatefulCommand` and not a `WindowFlushable`.
     `DagRunnerService.createForApiBackend` rejects a DAG containing `sample`. The test asserts
     the widened rejection text names `sample` in its list of command kinds, not only
     `Found: sample`.
   - **Metrics:** events in a batch carry distinct map-valued `__tag__` (e.g. `{"id": "e1"}`), and
     every `process()` call uses the same `callingUser`. Assert each counter's increments per full
     tag set (event tags plus the calling-user tag):
     - input and output are counted for a pass-through event, and it adds no drop;
     - the output increment carries the kept event's tags;
     - a replaced candidate's drop carries the candidate's tags, not the arriving event's;
     - a full batch plus a partial one gives exactly N−1 drops, with the partial batch's
       candidate uncounted.
     - a throwing condition increments no error counter.
   - **Input not mutated:** deep-copy every input event before `process()` and `assertEquals` each
     copy afterwards, for kept, dropped and pass-through events, including a kept event that
     already carried the tag field. At least one batch must span two `process()` calls, so a
     candidate held across calls is checked.
   - **Literal key:** with `sampleRateField: "a.b"`, assert the full kept output record: it has a
     top-level key `a.b`, and an existing nested `a: {b: …}` is left unchanged.
