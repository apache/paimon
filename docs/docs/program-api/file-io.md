---
title: "FileIO API"
sidebar_position: 4
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# FileIO API

`FileIO` is Paimon's interface for file I/O on local file systems, distributed file systems, and
object stores. This page is for code that calls `FileIO` directly and for developers implementing a
new `FileIO` implementation. Most users only need
[Filesystems](../maintenance/filesystems), which describes the dependencies and options for the
built-in implementations.

The method sections below describe behavior common to the supported implementations. The final
section describes how Paimon currently calls the API. Current usage does not narrow the public
interface: a call remains valid when its method contract allows it, even if Paimon does not make
that call today.

These contracts describe observable results, not a required sequence of storage requests. An
implementation can use conditional writes, metadata returned by listings, known file lengths, or
batch operations. It does not need a preliminary `exists(...)` or `getFileStatus(...)` call when an
operation can produce the required result directly.

## Read and Write Files

`newInputStream(...)` opens a new `SeekableInputStream`. Each stream has its own position, which
starts at `0` and is returned by `getPos()`. The stream supports forward and backward seeks from `0`
through the file length. Reading at the end of the file returns `-1` and does not change the
position. Opening a missing path or a directory can fail either when the stream is opened or on the
first read.

`newOutputStream(path, overwrite)` opens a `PositionOutputStream`. Its `getPos()` value is the
number of bytes written. After the stream closes successfully, the target contains exactly those
bytes. The `overwrite` argument controls how an existing target is handled:

- `false` preserves the existing file and reports an `IOException`. The error can occur while
  opening the stream, writing data, or closing it.
- `true` replaces the existing content.

`flush()` writes buffered data to the underlying stream, but only a successful `close()` guarantees
that the data is persistent and visible. Writing a file below a missing directory also makes its
parent paths visible as directories through `exists(...)` and `getFileStatus(...)`. Object store
implementations do not need to create a physical directory marker for every parent.

A missing directory successfully created by `mkdirs(...)` before its descendants are written is
explicit: deleting or moving its last child does not delete that directory. A parent that becomes
visible only because a descendant was written is implicit. It must remain visible while any
descendant exists, but after the last descendant is deleted or moved away it may either remain as
an empty directory or become missing. This difference does not require callers to inspect physical
directory markers.

## File Status and Listing

`getFileStatus(...)` returns a `FileStatus` for an existing path. `getPath()` returns the path,
`isDir()` distinguishes a directory from a file, `getLen()` returns a file's length, and
`getModificationTime()` returns the number of milliseconds since the Unix epoch. Modification-time
precision depends on the storage system. `getAccessTime()` and `getOwner()` may be unavailable; their
default values are `0` and `null`. A missing path throws `FileNotFoundException`.

The listing methods are defined for existing directories:

- `listStatus(...)` returns the direct files and directories. It returns an empty array for an empty
  directory.
- `listFiles(...)` and `listFilesIterative(...)` return files only. With recursive listing enabled,
  they also return files in nested directories. The iterator may load entries as it is consumed.
- `listDirectories(...)` returns direct directories only.

Listing order is not guaranteed. `getFileSize(...)` and `isDir(...)` return the corresponding value
from `getFileStatus(...)`, without requiring a separate `exists(...)` call.

## File and Directory Operations

- `exists(...)` returns `true` for an existing file or directory and `false` for a missing path.
- `mkdirs(...)` creates the requested directory and any missing parents. It returns `true` when the
  directory already exists. A file at the target path or in its parent path causes an
  `IOException`.
- `delete(path, recursive)` returns `true` after deleting an existing file or empty directory.
  Deleting a non-empty directory with `recursive=false` throws `IOException` and leaves the
  directory unchanged. With `recursive=true`, it deletes the complete directory tree. The return
  value for a missing path is implementation-specific.
- `rename(...)` moves a file or directory to the exact destination path. Call it with an existing
  source, a different destination that does not exist, and an existing destination parent in the
  same underlying file system. On success, it returns `true`, removes the source path, and preserves
  the file content or complete directory tree. Moving the last descendant out of an explicit source
  parent leaves that parent as an empty directory; an implicit source parent may become missing.
- `copyFile(...)` copies the source bytes to the exact destination. An existing destination is
  replaced only when `overwrite=true`. When a source directory contains files only,
  `copyFiles(...)` applies the same behavior to each direct file.

`checkOrMkdirs(...)` accepts an existing directory or creates a missing one, but throws
`IllegalArgumentException` for an existing file. `deleteQuietly(...)`, `deleteFilesQuietly(...)`,
and `deleteDirectoryQuietly(...)` suppress `IOException`; directory deletion is recursive, while
file deletion is not. These quiet helpers are best-effort and do not return a success result.

## UTF-8 File Helpers

`writeFile(...)` writes UTF-8 text using the requested overwrite mode. `overwriteFileUtf8(...)` and
`overwriteHintFile(...)` replace the current content. All three methods close the output stream.
Use `overwriteHintFile(...)` only for hint files whose temporary absence during an overwrite is
acceptable.

`readFileUtf8(...)` reads UTF-8 text and closes the input stream. It reads the file line by line and
does not preserve line separators. `readOverwrittenFileUtf8(...)` returns an empty `Optional` for a
missing file and retries the remote-file-change errors recognized by the implementation.

`tryToWriteAtomic(...)` returns `true` when it publishes content to a missing target. If the target
already exists, it returns `false`, keeps the existing content, and removes temporary data. Its
default implementation writes to a temporary file and then renames it, while storage implementations
may use native conditional writes. Atomicity between clients therefore depends on the storage
system.

## Two-Phase Writes

`newTwoPhaseOutputStream(...)` writes data to a staging path. `closeForCommit()` returns a
serializable committer. The staged data is not visible at the target before `commit(...)` is called;
a successful `commit(...)` publishes it. If the commit throws an exception, the target state is not
guaranteed. The `overwrite` argument has the same meaning as it does for `newOutputStream(...)`.

`discard(...)` removes only the data staged by that writer. After a successful commit, `clean(...)`
can remove resources that the committer no longer needs, but it must not remove another writer's
data.

The default implementation stages a temporary file and commits it with `rename(...)`. A storage
system can override the method, for example to use multipart upload.

## Loading and Configuration

`FileIO.get(...)` selects an implementation from a path and `CatalogContext`. When
`resolving-file-io.enabled` is enabled, it returns a configured `ResolvingFileIO`, which selects an
underlying `FileIO` for each path. Otherwise, a path without a URI scheme returns `LocalFileIO`
directly. Its `configure(...)` method is a no-op.

For a path with a scheme, `FileIO.get(...)` first checks the configured preferred loader. If that
loader is absent or inaccessible, it looks for a discovered loader with the same scheme. Before
using the preferred or discovered loader, it checks `requiredOptions()`: each returned group lists
aliases for one required option, and at least one alias from every group must occur in the catalog
options, matched case-insensitively. A loader with a missing required option is skipped. Selection
then checks the configured fallback loader and finally Hadoop. The final loader creates a new
`FileIO`, which `FileIO.get(...)` configures before returning it.

Applications can obtain the storage for an existing table through `Table.fileIO()`. Code that only
has a path and a `CatalogContext` can call `FileIO.get(...)`. `discoverLoaders()` and
`checkAccess(...)` support this selection and are not normally called directly.

Call `configure(...)` on factory- or loader-created instances that have not yet received a
`CatalogContext`. A `FileIO` returned by `Table.fileIO()` is already configured and must not be
configured again. Some table-bound implementations, including `RESTTokenFileIO`, reject
reconfiguration.

`setRuntimeContext(...)` supplies optional job-level file system settings. Paimon's Flink
integration calls it only when `filesystem.job-level-settings.enabled` is enabled. It is not an
automatic callback after deserialization, and its default implementation does nothing. `close()`
releases resources owned by an implementation; its default also does nothing.

`FileIO` implementations are serializable and thread-safe. `isObjectStore()` is an implementation
hint and does not define the behavior of other methods.

## Optional Operations

`archive(...)`, `restoreArchive(...)`, `unarchive(...)`, and `createBlobPresignedUrl(...)` are
optional. Their default implementations throw `UnsupportedOperationException`.

## Paimon FileIO Usage

This section records the call shapes and results used by Paimon's production code. It helps new
callers choose the same preconditions and recovery rules. It does not replace the method contracts
above or remove behavior from methods that have no current caller.

### Status and Listing

Paimon lists paths expected to be directories. They usually come from configuration, a previous
status or listing result, or an `exists(...)` check. A configured warehouse or object-table
location can itself be the file system root; callers treat that location as an ordinary directory.
Core paths do not intentionally pass a regular file to a listing method.

Callers do not depend on listing order or on one snapshot-consistent result while another client is
changing the directory. Correctness-sensitive traversal handles a directory that disappears after
its parent was listed by accepting an empty result or catching `FileNotFoundException` and skipping
that subtree; other `IOException` values fail the operation. Best-effort orphan cleanup may instead
treat any listing `IOException` as an empty result and skip that subtree. Some callers that want a
missing directory to mean an empty listing check `exists(...)` first. This listing practice is
separate from the public `getFileStatus(...)` rule: a missing path from `getFileStatus(...)` must
throw `FileNotFoundException`.

Paimon consumes modification times for both files and directories, including cleanup cutoffs and a
branch directory's reported creation time. The value must follow the `FileStatus` contract, while
its precision and changes during concurrent updates remain file-system-specific. Paimon relies on
logical parent directories being visible, but does not inspect or require physical directory
marker objects.

### Output and Copying

Both `newOutputStream(...)` modes are used. Paimon uses `overwrite=false` for UUID- or
version-derived files expected not to exist; a failed create must preserve an existing target. It
uses `overwrite=true` for replaceable state and copy destinations. Writers rely on exact
`getPos()` values and successful `close()` as the publication boundary. Some branch-copy paths also
create output below parents that have not been created explicitly.

Current `copyFiles(...)` callers copy flat snapshot, schema, and tag metadata directories during
branch fast-forward. They use the same `FileIO` for source and destination and pass
`overwrite=true`. Files are copied one at a time; callers do not assume that a failure rolls back
files copied earlier. This call shape does not remove the public `overwrite=false` behavior from
`copyFile(...)` or `copyFiles(...)`.

### Directories, Deletion, and Rename

Paimon uses `mkdirs(...)` with directory-semantic paths, including paths that may already exist. It
relies on parent creation and treats `false` as a creation failure where the result is checked.
Production code does not intentionally pass an existing file or a child of a file.

Strict `delete(...)` callers normally start with an existing, owned path. They use
`recursive=false` for files or directories expected to be empty and `recursive=true` for complete
owned trees. No caller relies on one particular return value for a missing path: it either ignores
that result or combines a `false` result with `exists(...)`. Quiet deletion is used only where
best-effort cleanup is acceptable.

Raw `rename(...)` calls use an existing source, a distinct exact destination expected not to exist,
the same `FileIO` and underlying file system, and an existing or pre-created destination parent. A
`true` result confirms the move, and some callers check that result. Branch rename currently ignores
the result and assumes that the selected file system provides atomic rename; it has no coordination
fallback. Existing-destination recovery is handled by the higher-level conditional and two-phase
write protocols, not by changing this raw rename shape. Core workflows do not intentionally use
identical paths or move an item into an existing destination directory; those shapes appear only
through optional virtual file system passthroughs.

Snapshot publication is the workflow that adds external locking and content checks when atomic
rename is unavailable. Paimon uses `isObjectStore()` when choosing the default catalog-lock setting,
but the value alone does not change the `rename(...)` contract. The blob-descriptor source-table path
also calls `isObjectStore()` before serialization to initialize lazy credentials. That side effect is
implementation-specific and is not part of the `FileIO` contract.

### Conditional and Two-Phase Publication

Schemas, snapshots, and Iceberg metadata use `tryToWriteAtomic(...)` with an absent target as the
normal case. `true` means this attempt published its content. `false` means the attempt did not
publish because a target already exists; that target can be concurrent or stale. Callers inspect the
existing content, retry, or use an external lock as required by their metadata protocol. Iceberg
metadata may delete a nonmatching stale target and retry. Callers do not use this method as an
overwrite operation for arbitrary existing state.

Format-table writers are the current production users of `newTwoPhaseOutputStream(...)`. They pass
`overwrite=false` and use writer-owned UUID target paths. They rely on staged data remaining hidden,
a serializable committer from `closeForCommit()`, publication by `commit(...)`, and writer-scoped
`discard(...)` and `clean(...)` operations. The public API still supports `overwrite=true` even
though this workflow does not use it.

A remote publication can succeed before reporting an exception, so recovery depends on ownership.
Paimon preserves an ambiguous mutable target when the caller does not own a unique path. The
format-table commit path records every attempted committer and can delete an attempted target after
failure only because each UUID path belongs to that failed batch. This is a format-table recovery
rule, not permission for arbitrary two-phase callers to delete an uncertain target.

### Lifecycle and Optional Operations

Most table code receives an already configured `FileIO` from `Table.fileIO()`. Factory-created
instances are either retained by an owner or should be closed by the code that owns their resource
scope. `CachingFileIO.close()` closes its delegate and then releases its shared cache-manager
reference.

`archive(...)`, `restoreArchive(...)`, and `unarchive(...)` currently have no production caller or
implementation, so their default `UnsupportedOperationException` behavior remains in effect.
`createBlobPresignedUrl(...)` is an active optional operation used by the Blob API and Flink and
Spark SQL functions. Its callers use the `FileIO` and table root from the same loaded table, pass a
table-owned blob descriptor, and do not assume that every `FileIO` supports the operation. Spark
validates a positive whole-second validity before the call. Flink forwards the supplied `Duration`,
so supporting implementations must reject unsupported validity values.
