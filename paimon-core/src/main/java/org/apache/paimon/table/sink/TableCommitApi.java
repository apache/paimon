package org.apache.paimon.table.sink;

/**
 * replace for {@link TableCommitImpl}
 **/
public interface TableCommitApi extends InnerTableCommit{
    void expireSnapshots();
}
