# Repair Earliest Snapshot Design

## Problem Statement

If the `EARLIEST` snapshot hint is not advanced after old snapshots are removed, the remaining
snapshot files may contain a gap between the hinted snapshot and the latest snapshot. Flink and
Spark users need a procedure to repair the hint without editing table files directly.

## Chosen Approach

Add `repair_earliest_snapshot(table, snapshot_id)` procedures for Flink and Spark. Both procedures
delegate validation and the hint update to `SnapshotManager`.

## Design Details

### Validation

- The table must have an earliest and latest snapshot.
- The target snapshot must exist.
- The target must be between the current earliest and latest snapshots.
- A target later than the current earliest must immediately follow a missing snapshot, so the
  procedure cannot skip a continuous snapshot range accidentally.
- Repeating the repair with the current earliest snapshot is allowed.

### Result

The procedure returns the previous and current earliest snapshot IDs.

### Compatibility

Flink common provides the procedure and named arguments. Flink 1.18 reuses the common
implementation and supports positional arguments. Spark registers the same procedure name and
parameters in `SparkProcedures`.

## Open Questions

None.

## Out of Scope

- Discovering the target snapshot automatically.
- Repairing or deleting snapshot, manifest, or data files.
- Coordinating with concurrent snapshot expiration.
