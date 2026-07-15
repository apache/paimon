# DV-aware Primary-key Vector Index Build Design

## Problem Statement

Primary-key vector index maintenance currently reads compact source files without deletion-vector
filtering and therefore indexes physical row positions that were already deleted when an ANN build
was scheduled. Query-time deletion-vector filtering preserves correctness, but rebuilding the same
active source cannot reclaim those holes.

## Chosen Approach

Capture the deletion vectors visible to the bucket writer when a pending ANN build is created. Pass
an immutable per-source snapshot into the existing `excludedPosition` hook while preserving each
vector's physical ordinal (`fileOffset + rowPosition`).

## Design Details

### Maintenance integration

The bucket writer already restores and updates `BucketedDvMaintainer` before primary-key index
maintenance prepares its commit. Its deletion-vector factory will be passed to the primary-key
index maintainer and then to the vector maintainer.

### Asynchronous snapshot semantics

`PendingBuild` will clone relevant deletion vectors synchronously when the build is scheduled. The
executor must not observe later mutations of the writer's deletion-vector state. Deletions created
after scheduling remain covered by query-time filtering.

### Row-id stability

The vector reader continues to read every physical row without applying a filtering reader.
`PkVectorAnnSegmentFile` skips excluded vectors but writes included ids as
`fileOffset + physicalRowPosition`, preserving source metadata and positional reads.

### Fully deleted sources

If every source row is excluded, publish a zero-row segment backed by an empty marker file. This
keeps the source covered without passing deleted row ids to an ANN writer or retrying the same
build indefinitely. Search recognizes the zero-row segment and returns an empty result without
opening the marker as an ANN payload.

### Tests

1. A deletion present before scheduling is absent from the newly built ANN payload.
2. Mutating the writer deletion vector after scheduling does not change the pending build snapshot.
3. A fully deleted source publishes a searchable zero-row segment.
4. Existing query-time deletion-vector filtering remains unchanged.

## Open Questions

None for this scoped change.

## Out of Scope

- Paimon vindex changes.
- Stale asynchronous build discard/retry policy.
- Source-file stale-ratio scheduling.
- Removing query-time deletion-vector filtering.
