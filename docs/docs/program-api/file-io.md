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

The behavior below is common to the supported implementations. If a case is not described, callers
should not assume that all storage systems handle it in the same way.

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
  the file content or complete directory tree.
- `copyFile(...)` copies the source bytes to the exact destination. An existing destination is
  replaced only when `overwrite=true`. When a source directory contains files only,
  `copyFiles(...)` applies the same behavior to each direct file.

`checkOrMkdirs(...)` accepts an existing directory or creates a missing one, but throws
`IllegalArgumentException` for an existing file. `deleteQuietly(...)`, `deleteFilesQuietly(...)`,
and `deleteDirectoryQuietly(...)` suppress `IOException`; directory deletion is recursive, while
file deletion is not.

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

`FileIO.get(...)` selects and configures an implementation from a path and `CatalogContext`. When
`resolving-file-io.enabled` is enabled, it returns a `ResolvingFileIO`, which selects an underlying
`FileIO` for each path. Otherwise, a path without a URI scheme uses `LocalFileIO`. For a path with a
scheme, selection considers an accessible configured preferred loader, a discovered loader for the
scheme, a configured fallback, and finally Hadoop.

Applications can obtain the storage for an existing table through `Table.fileIO()`. Code that only
has a path and a `CatalogContext` can call `FileIO.get(...)`. `discoverLoaders()` and
`checkAccess(...)` support this selection and are not normally called directly.

`configure(...)` receives catalog-level settings, `setRuntimeContext(...)` receives runtime
settings, and `close()` releases resources owned by the implementation. The default implementations
of `setRuntimeContext(...)` and `close()` do nothing. `isObjectStore()` is an
implementation-specific hint; callers should not use it to infer the behavior of other methods.

`FileIO` implementations are serializable and thread-safe. Runtime-only state must be restored by
`setRuntimeContext(...)` after deserialization when an implementation requires it.

## Optional Operations

`archive(...)`, `restoreArchive(...)`, `unarchive(...)`, and `createBlobPresignedUrl(...)` are
optional. Their default implementations throw `UnsupportedOperationException`.

## Behavior Not Defined by FileIO

`FileIO` does not define the following behavior. Code intended to work with multiple storage systems
must not depend on:

- listing a missing path, a file, or the file system root;
- listing order or a consistent listing while another client changes the directory;
- `rename(...)` with a missing source, an existing or identical destination, a missing destination
  parent, or paths from different file systems;
- atomic `rename(...)` or Hadoop's behavior of moving an item into an existing destination
  directory;
- `copyFiles(...)` when the source directory contains another directory;
- physical directory markers, stable directory modification times, or file and prefix collisions
  on object stores;
- an exception subtype more specific than the type declared by the method, or the point at which a
  deferred operation reports an error; or
- visibility before an output stream closes, or recovery after a network failure whose result is
  unknown.

The API defines results, not the storage requests used to produce them. Implementations can use
conditional writes, known file lengths, status values returned by listings, and batch operations to
avoid unnecessary metadata requests on object stores. The API does not require a preliminary
`exists(...)` or `getFileStatus(...)` call when an operation can provide the required result itself.
