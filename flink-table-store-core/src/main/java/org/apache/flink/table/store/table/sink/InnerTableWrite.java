package org.apache.flink.table.store.table.sink;

public interface InnerTableWrite extends TableWrite {

    InnerTableWrite withOverwrite(boolean overwrite);
}
