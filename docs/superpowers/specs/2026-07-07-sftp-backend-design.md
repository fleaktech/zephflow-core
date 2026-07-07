# SFTP Backend for the fssource Abstract File System

**Date:** 2026-07-07
**Status:** Approved

## Goal

Add an SFTP backend to the abstract file system in
`lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/`, alongside the
existing local, S3, GCS, and Azure Blob backends. Read-only, matching the
existing backends: list + stat + read. No post-processing (delete/move) of
ingested files.

## Library

**sshj (`com.hierynomus:sshj`)**, Apache 2.0. Its high-level API maps directly
onto the `FileLister`/`FileReader` contracts: `SSHClient.authPassword()` /
`authPublickey()`, `SFTPClient.ls()` / `stat()`, and remote-file input streams
that support an initial byte offset. Pulls in Bouncy Castle (needed by any
library for modern key formats).

Rejected alternatives:

- **Apache MINA SSHD** — more framework ceremony than a read-only client needs.
- **JSch (mwiede fork)** — dated API, awkward offset reads and error handling.

Exact sshj API signatures must be verified against the real library during
implementation, not assumed.

## Architecture

New package `lib/.../commands/fssource/backend/sftp/`, mirroring the Azure
backend layout:

| File | Responsibility |
|---|---|
| `SftpBackend` | `scheme() = "sftp"`; `capabilities() = {DELETE, MOVE, RANGE_READ}` (SFTP supports all three; nothing consumes capabilities today); creates lister/reader |
| `SftpBackendConfig` | record implementing `FsBackendConfig` |
| `SftpLister` | recursive directory walk under root; `stat` |
| `SftpReader` | open a remote file at a byte offset |

Registration: `FsBackendRegistry.register(new SftpBackend())` in the
`FsSourceCommandFactory` static block. Config construction: new `case "sftp"`
in `FsSourceCommand.buildBackendConfig()`.

### Addressing

`root` is a URN: `sftp://host[:port]/absolute/path` (default port 22). File
URNs have the same shape (`sftp://host:22/upload/data/file.json`), so
`FileKey.of()` parses the `sftp` scheme naturally, consistent with `s3://`,
`gs://`, and `file://`.

### Connection lifecycle

- Host and port are parsed from `root` into `SftpBackendConfig` inside
  `buildBackendConfig()` (which sees the full command config).
- `SftpLister` and `SftpReader` each own one `SSHClient` connection:
  established lazily on first use, closed by `FsSourceExecutionContext.close()`
  (which already calls `lister.close()` / `reader.close()`).
- Each operation checks connectivity and reconnects once if the connection
  dropped; per-file failures beyond that are logged-and-skipped by
  `FsSourceCommand`'s existing loop.

## Config & credentials

`backendConfig` map keys, resolved in the new `case "sftp"`:

| Key | Meaning |
|---|---|
| `credentialId` | → `UsernamePasswordCredential` in JobContext: password auth |
| `privateKeyCredentialId` | → `RSAPrivateKeyCredential` in JobContext: key auth (PKCS8 `key` + `user` as username) |
| `hostKeyFingerprint` | optional, OpenSSH `SHA256:...` format |

Rules:

- Exactly one of `credentialId` / `privateKeyCredentialId` is required;
  otherwise throw `IllegalArgumentException` with a message matching the
  azblob error style.
- `hostKeyFingerprint` set → sshj fingerprint verifier; mismatch fails the
  connection. Absent → accept any host key (promiscuous verifier).
- A new `MiscUtils.lookupRSAPrivateKeyCredential(jobContext, credentialId)`
  helper follows the existing per-type lookup pattern
  (`loadOtherConfig(..., RSAPrivateKeyCredential.class)`).
- No key-passphrase support: `RSAPrivateKeyCredential` is documented PKCS8 and
  lives in the credential store; encrypted-key support is deferred until
  needed.

`SftpBackendConfig` record:

```java
record SftpBackendConfig(
    String host,
    int port,
    String username,
    String password,          // null for key auth
    String privateKeyPkcs8,   // null for password auth
    String hostKeyFingerprint // nullable
) implements FsBackendConfig {}
```

## Lister / Reader semantics

**`SftpLister.list(req)`**

- Parse the path from the root URN; walk directories recursively via
  `SFTPClient.ls()`.
- Regular files only. Do not descend into symlinked directories (cycle
  safety, matching `Files.walk` semantics in the local backend).
- Apply `fileNameRegex` against the bare filename, like other backends.
- Collect eagerly into a list, return `list.stream()` — `FsSourceCommand`
  drains the stream immediately, so laziness buys nothing.
- `FileEntry`: size and mtime from SFTP attributes (seconds precision — fine
  for watermark logic); `displayPath` = URN.

**`SftpLister.stat(key)`** — `SFTPClient.stat(path)` → `FileEntry`.

**`SftpReader.open(key, offset)`** — open the remote file, return an
`InputStream` positioned at `offset`. The wrapper stream's `close()` also
closes the underlying remote-file handle and its resources.

## Error handling

- Connect/auth/host-key failures surface on first use; wrap `IOException` in
  `UncheckedIOException` (matching `LocalFsLister`) so the job fails fast with
  sshj's descriptive message.
- Host-key mismatch against a pinned fingerprint → connection refused by the
  fingerprint verifier.
- Per-file read/deserialize errors → existing log-and-skip in
  `FsSourceCommand.execute()`.

## Testing

Real integration tests (TestContainers), following the Azurite precedent for
Azure:

**`SftpBackendIntegrationTest`** — `@Tag("integration")`, `atmoz/sftp` image:

- recursive listing across nested directories
- `fileNameRegex` filtering
- `stat`
- full-file read; read from a non-zero offset
- password auth; key auth (mount an authorized public key into the container)
- host-key pinning: correct fingerprint connects, wrong fingerprint fails

**Unit tests:**

- URN parsing: host/port/path extraction, default port 22, invalid URNs
- `buildBackendConfig` for sftp: password path, key path, error when both or
  neither credential key is present

## Build changes

- Add sshj to the gradle version catalog (`gradle/libs.versions.toml`) and
  `lib/build.gradle` (`implementation`).
- No new test-framework dependencies: TestContainers `GenericContainer` (as
  used for Azurite) suffices for `atmoz/sftp`.

## Out of scope

- Post-processing of ingested files (delete/move after read) — the
  `Capability` enum exists but nothing consumes it; a cross-backend feature
  for later.
- SFTP sink support.
- Encrypted private keys (passphrase support).
- Connection pooling beyond the two per-run connections.
