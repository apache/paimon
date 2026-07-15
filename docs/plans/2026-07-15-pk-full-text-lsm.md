# Implementation Plan: Primary-Key Full-Text LSM Maintenance

## Prerequisites

- [x] Work on `codex/pk-fulltext`; preserve the existing uncommitted PK full-text changes.
- [x] Use JDK 8 syntax and the detached verification worktree at
      `/tmp/paimon-pk-fulltext-verify-20260715`.
- [x] Follow RED-GREEN-REFACTOR for every production change.

## Tasks

### Task 1: Validate shared LSM options for full-text definitions

- **Files**:
  - `paimon-core/src/test/java/org/apache/paimon/schema/PrimaryKeyFullTextIndexValidationTest.java`
  - `paimon-core/src/test/java/org/apache/paimon/index/pk/PrimaryKeyIndexDefinitionsTest.java`
  - `paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java`
  - `paimon-core/src/main/java/org/apache/paimon/index/pk/BucketedPrimaryKeyIndexMaintainer.java`
- **Changes**: Replace the existing assertion that full text ignores LSM options with validation
  tests; verify resolved fanout/stale threshold; pass both values into the full-text maintainer.
- **Verify**: Run the two test classes; expect all tests to pass.
- **Dependencies**: None.

### Task 2: Build one archive from ordered multiple source files

- **Files**:
  - `paimon-core/src/test/java/org/apache/paimon/index/pkfulltext/PkFullTextIndexFileTest.java`
  - `paimon-core/src/main/java/org/apache/paimon/index/pkfulltext/PkFullTextIndexFile.java`
  - `paimon-core/src/main/java/org/apache/paimon/index/pkfulltext/PkFullTextIndexBuilder.java`
- **Changes**: Add a lazy source abstraction, concatenate source ordinals, validate row counts, and
  serialize ordered multi-source metadata while retaining the single-source entry point.
- **Verify**: Run `PkFullTextIndexFileTest`; expect all tests to pass.
- **Dependencies**: Task 1.

### Task 3: Restore and validate multi-source archive state

- **Files**:
  - `paimon-core/src/test/java/org/apache/paimon/index/pkfulltext/PkFullTextBucketIndexStateTest.java`
  - `paimon-core/src/main/java/org/apache/paimon/index/pkfulltext/PkFullTextBucketIndexState.java`
- **Changes**: Accept multi-source archives, validate cumulative row metadata, map every source to
  its archive, reject overlapping coverage, and retain old-fingerprint retirement.
- **Verify**: Run `PkFullTextBucketIndexStateTest`; expect all tests to pass.
- **Dependencies**: Task 2.

### Task 4: Add shared level selection and incremental archive replacement

- **Files**:
  - `paimon-core/src/test/java/org/apache/paimon/index/pkfulltext/BucketedFullTextIndexMaintainerTest.java`
  - `paimon-core/src/main/java/org/apache/paimon/index/pkfulltext/BucketedFullTextIndexMaintainer.java`
- **Changes**: Replace per-file concurrent builds with one asynchronous pending build over ordered
  sources, use `PrimaryKeyIndexLevels<IndexFileMeta>` for fanout/stale selection, atomically replace
  inputs, and preserve cancellation, retry, and rollback semantics.
- **Verify**: Run `BucketedFullTextIndexMaintainerTest`; expect uncovered, fanout, stale, async, and
  abort tests to pass.
- **Dependencies**: Tasks 2-3.

### Task 5: Map search and deletion vectors across archive source ranges

- **Files**:
  - `paimon-core/src/test/java/org/apache/paimon/index/pkfulltext/PrimaryKeyFullTextBucketSearchTest.java`
  - `paimon-core/src/main/java/org/apache/paimon/index/pkfulltext/PrimaryKeyFullTextBucketSearch.java`
- **Changes**: Shift per-file live-row bitmaps into archive-global offsets and map scored results
  back to local file positions before RRF.
- **Verify**: Run `PrimaryKeyFullTextBucketSearchTest`; expect multi-source DV and ranking tests to
  pass.
- **Dependencies**: Task 3.

### Task 6: Verify coordinator lifecycle and compatibility

- **Files**:
  - `paimon-core/src/test/java/org/apache/paimon/index/pk/BucketedPrimaryKeyIndexMaintainerTest.java`
  - `paimon-core/src/test/java/org/apache/paimon/operation/PrimaryKeyIndexWriteTest.java`
- **Changes**: Cover factory option propagation, non-blocking LSM completion, snapshot increment
  routing, and restoration from existing single-source archives.
- **Verify**: Run both test classes; expect all tests to pass.
- **Dependencies**: Tasks 1-5.

### Task 7: Update documentation and run the regression matrix

- **Files**:
  - `docs/docs/primary-key-table/full-text-index.md`
  - `docs/docs/primary-key-table/vector-index.md`
  - relevant changed tests and sources above
- **Changes**: Document multi-source LSM maintenance and shared options; run core, Spark 3/4, Flink
  1/2, formal compile, Spotless, and `git diff --check`.
- **Verify**:
  - Core full-text/PK-index test set passes.
  - Spark 3 and Spark 4 full-text/hybrid suites pass.
  - Flink 1 and Flink 2 `FullTextSearchProcedureITCase` pass.
  - JDK 8 non-fast `paimon-core,paimon-full-text` compile passes.
- **Dependencies**: Tasks 1-6.

## Post-Implementation

- [ ] Run `spotless:check` for API, Core, Full Text, Flink, and Spark modules.
- [ ] Run `git diff --check` and inspect all untracked files.
- [ ] Perform a focused code review of LSM state transitions, row-id offsets, and cleanup paths.
- [ ] Do not stage, commit, push, or open a PR without explicit user instruction.
