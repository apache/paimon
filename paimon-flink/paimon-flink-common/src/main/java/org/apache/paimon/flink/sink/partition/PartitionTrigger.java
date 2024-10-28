package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.manifest.ManifestCommittable;

import java.io.Closeable;
import java.util.List;

public interface PartitionTrigger extends Closeable {

    void notifyCommittable(List<ManifestCommittable> committables);

    void snapshotState() throws Exception;
}
