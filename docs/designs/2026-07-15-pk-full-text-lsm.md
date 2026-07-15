# Primary-Key Full-Text LSM Maintenance Design

## Problem Statement

Primary-key full-text indexes currently maintain one immutable archive for every eligible compact
data file. This keeps writes incremental, but the number of archives and query fan-out can grow
with the compaction history. Full-text maintenance should use the same logical LSM policy as the
primary-key vector and sorted indexes so that similarly sized archives are consolidated and stale
sources are reclaimed incrementally.

## Chosen Approach

Build multi-source full-text archives from the active compact data files selected by
`PrimaryKeyIndexLevels`. Do not add a native archive-merge API. A merge rebuild reads the selected
active source files in deterministic order, concatenates their physical row positions into one
archive row-id space, and atomically replaces the selected input archives.

The implementation reuses the existing per-column primary-key index compaction options:

- `fields.<column>.pk-index.compaction.level-fanout`
- `fields.<column>.pk-index.compaction.stale-ratio-threshold`

No `index.type` option or full-text-specific compaction option is added.

## Design Details

### Multi-Source Archive Format

`PrimaryKeyIndexSourceMeta.sourceFiles()` stores the ordered source files covered by an archive.
The native archive row-id space is the concatenation of those files: the first file starts at zero
and every following file starts at the cumulative row count of its predecessors. Existing
single-source archives remain valid one-unit LSM segments and require no metadata migration.

`PkFullTextIndexFile` accepts ordered sources, validates each reader row count, writes all rows
with cumulative row IDs, and records the complete ordered source list plus the existing definition
fingerprint.

### LSM Maintenance

`BucketedFullTextIndexMaintainer` maintains active source files and active archive segments. It:

1. builds one archive for uncovered active sources;
2. uses `PrimaryKeyIndexLevels<IndexFileMeta>` to select similarly sized or stale archives;
3. rebuilds the selected active sources asynchronously;
4. accepts a completed build only when all sources and input segments are still active and the
   output does not overlap retained segments;
5. publishes the new archive and retires its input archives in the same snapshot increment;
6. restores state and deletes generated unpublished output if commit preparation aborts.

The maintainer follows the existing Vector/Sorted `waitCompaction` behavior and exposes pending
maintenance through the bucket coordinator.

### State Restoration

`PkFullTextBucketIndexState` accepts one or more ordered sources per current-definition archive,
maps every source file to its owning archive, rejects overlapping coverage, and classifies archives
with old definition fingerprints as stale. Active archive validation checks source row counts and
the full concatenated row range.

### Search and Deletion Vectors

For every archive, search builds one include bitmap in archive-global row IDs. Each source file's
live positions are shifted by its cumulative archive offset. Returned archive row IDs are mapped
back to `(sourceFile, localRowPosition)` with the same ordered ranges. Deletion-vector filtering
therefore happens before the archive-local Top-K, as it does for single-source archives.

Archive-local rankings continue to be fused with RRF across active archive segments. Score and tie
ordering remain unchanged.

### Validation and Documentation

Full-text columns participate in validation of the shared primary-key index compaction options.
The primary-key full-text documentation describes multi-source LSM maintenance, asynchronous
visibility, compatibility with single-source archives, and the shared tuning options.

## Open Questions

None for the first implementation. Binary native archive merging can be evaluated separately if
source-file rebuild cost becomes a demonstrated bottleneck.

## Out of Scope

- Native full-text archive binary merging.
- More than one primary-key full-text column per table.
- Changes to Python APIs.
- Changes to `index.type` selection.
