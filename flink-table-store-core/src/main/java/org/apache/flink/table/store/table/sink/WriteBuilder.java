package org.apache.flink.table.store.table.sink;

public interface WriteBuilder {

    TableWrite newWrite();

    TableCommit newCommit();

    String commitUser();
}
