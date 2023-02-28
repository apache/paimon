package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.file.operation.Lock;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface InnerTableCommit extends TableCommit {

    /** Overwrite writing, same as the 'INSERT OVERWRITE T PARTITION (...)' semantics of SQL. */
    default InnerTableCommit withOverwrite(@Nullable Map<String, String> staticPartition) {
        if (staticPartition != null) {
            withOverwrite(Collections.singletonList(staticPartition));
        }
        return this;
    }

    InnerTableCommit withOverwrite(@Nullable List<Map<String, String>> overwritePartitions);

    @Override
    InnerTableCommit withLock(Lock lock);
}
