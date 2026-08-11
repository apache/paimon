---
title: "FileIO Behavior"
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

# FileIO Behavior

`FileIO` abstracts local filesystems, distributed filesystems, and object stores. Every
implementation must provide the behavior below; callers may rely on it when the stated
preconditions hold. The contract does not require POSIX or Hadoop filesystem compatibility. It also
does not require a separate metadata lookup when an operation can return the required result
itself.

## Required operations

| API | Required behavior |
| --- | --- |
| `isObjectStore` | Identifies whether the implementation has object-store characteristics. Callers must not use it to infer semantics that are not stated here. |
| `newInputStream` | Returns an independent seekable stream. Reads return the stored bytes, `getPos` tracks the cursor, seeking to a valid offset works, and reads at end of file return `-1` without advancing the cursor. Opening a missing path or a path that names a directory may fail when the stream is created or on its first read. |
| `newOutputStream` | A successful close publishes exactly the bytes written. `overwrite=false` must not replace an existing file; failure may be reported while opening, writing, or closing. `overwrite=true` replaces the old content. Creating a nested file also makes its logical parent directories available. |
| `getFileStatus` | Returns the path, type, byte length, and modification time for an existing path. A missing path throws `FileNotFoundException`. Modification time is expressed as epoch milliseconds, but exact precision is implementation-specific. |
| `exists` | Returns `true` for an existing file or directory and `false` for a missing path. Callers should not invoke it immediately before an operation that already reports the required outcome. |
| `listStatus` | For a known directory, returns a non-null array containing its direct children. Empty directories return an empty array. Result order is not defined. |
| `delete` | Deleting an existing file or empty directory returns `true`. Deleting a non-empty directory with `recursive=false` throws `IOException` without changing the tree; with `recursive=true`, it removes the complete tree. The return value for a missing path is not defined. |
| `mkdirs` | Makes the requested directory hierarchy available and returns `true`, including when the directory already exists. A file at the target or in its parent chain causes an `IOException`. Implementations do not have to materialize object-store directory markers for every parent. |
| `rename` | The defined success case has an existing source, a distinct missing destination, and an existing destination parent in the same `FileIO`. It returns `true`, removes the source name, and preserves the complete file content or directory tree at the exact destination. |

## Default methods

The methods below are implemented by `FileIO` itself. Implementations may override them to reduce I/O,
but the observable result must remain the same.

| API | Required behavior |
| --- | --- |
| `listFiles`, `listFilesIterative` | Return files under a known directory. Non-recursive listing returns direct files; recursive listing includes files below nested directories. Iteration may perform work lazily. |
| `listDirectories` | Returns only the direct child directories of a known directory. |
| `getFileSize`, `isDir` | Return the corresponding field from `getFileStatus`; they do not require an additional existence check. |
| `checkOrMkdirs` | Accepts an existing directory or creates a missing one. An existing file is rejected. |
| `deleteQuietly`, `deleteFilesQuietly`, `deleteDirectoryQuietly` | Attempt the requested deletion and suppress `IOException`. Directory deletion is recursive; file deletion is not. An implementation may avoid probing a missing target. |
| `readFileUtf8` | Decodes content as UTF-8 and closes the input stream. Preservation of original line separators is not part of this contract. |
| `writeFile`, `overwriteFileUtf8`, `overwriteHintFile` | Write UTF-8 content and close the output stream. `writeFile` forwards its overwrite intent; the overwrite helpers replace visible content. |
| `tryToWriteAtomic` | For a missing target, publishes the supplied content and returns `true`. If the target already exists, returns `false`, preserves its content, and cleans up temporary data. Cross-client atomicity requires support from the storage system; a metadata check followed by a write is not sufficient. |
| `copyFile` | Copies the source bytes to the exact destination. `overwrite=false` preserves an existing destination and reports failure; `overwrite=true` replaces it. |
| `copyFiles` | If every direct child of the source directory is a file, copies each child to the destination directory and forwards the overwrite mode. |
| `readOverwrittenFileUtf8` | Returns the current UTF-8 content, returns an empty `Optional` for a missing file, and retries the remote-file-change failures recognized by the implementation. |
| `newTwoPhaseOutputStream` | When supported, staged data becomes visible at the target only after `commit`. `discard` removes only that writer's staged data, and `clean` does not affect another writer. The overwrite flag has the same meaning as for `newOutputStream`, and a committer remains serializable. |

## Lifecycle, discovery, and optional capabilities

`configure`, `setRuntimeContext`, and `close` manage an implementation's configuration and
resources. After serialization and deserialization, a `FileIO` must remain usable once any required
runtime options have been supplied again through `setRuntimeContext`.

`get`, `discoverLoaders`, and `checkAccess` select and configure a loader for a path. Loader
selection prefers an accessible configured loader, then a discovered scheme loader, a configured
fallback, and finally Hadoop. Duplicate loaders for one scheme are rejected. Callers must not
depend on the number or order of metadata requests used during loader selection.

`archive`, `restoreArchive`, `unarchive`, and `createBlobPresignedUrl` are optional capabilities.
Their default methods throw `UnsupportedOperationException`; implementations must document the behavior
of capabilities they implement.

## Intentionally unspecified behavior

Paimon code must not depend on the following behavior unless a narrower API or capability defines
it:

- listing a missing path, a file, or the filesystem root;
- listing order or a snapshot-consistent listing during concurrent mutation;
- `rename` atomicity, a missing source, an existing destination, identical paths, a missing
  destination parent, or cross-filesystem rename;
- Hadoop's move-into-directory behavior when the destination is an existing directory;
- `copyFiles` when a direct child of the source is a directory;
- physical object-store directory markers, stable directory modification times, or file and prefix
  collision rules;
- exact exception subclasses where the API declares only `IOException`;
- visibility before a stream closes, or the exact point at which a deferred operation reports an
  error;
- consistency, durability, or recovery after a network failure with an unknown remote outcome.

These omissions are intentional. Adding a dependency on one of them requires an explicit FileIO
contract change and tests for every supported implementation; it must not be inferred from one
implementation.

## Object-store request cost

The contract constrains results, not an implementation sequence. Implementations may use conditional
writes, known file length, status returned by a listing, or native batch operations to avoid
redundant `HEAD` and `LIST` requests. Contract tests must keep setup and postcondition checks outside
any implementation-specific request-counting window.

## Related filesystem models

- [Hadoop's filesystem contract tests](https://hadoop.apache.org/docs/r3.4.0/hadoop-project-dist/hadoop-common/filesystem/testing.html)
  provide operation-oriented suites and explicit filesystem differences. Passing them does not prove
  distributed consistency, atomicity, idempotency, scalability, or durability.
- [Iceberg FileIO](https://iceberg.apache.org/docs/latest/fileio/) uses a narrower file-level model
  and does not require rename for table state changes. Its
  [OutputFile](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/io/OutputFile.html)
  separates create from create-or-overwrite intent.
- [POSIX rename](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html) defines
  atomic namespace replacement. POSIX inode, link, permission, and open-file semantics are not part
  of the `FileIO` contract.

`FileIOContractTestBase` covers required operations. `FileIODefaultMethodTest`, `FileIOTest`, and
`FileIOReturnTypeTest` cover default methods, lifecycle and discovery, default optional-capability
failures, and returned types. `FileIOContractCoverageTest` fails if a public `FileIO` method is not
assigned to a test group. These tests define single-operation preconditions and postconditions;
concurrency and fault recovery require separate tests.
