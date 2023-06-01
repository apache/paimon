package org.apache.paimon.flink.action.cdc.mysql;

import java.io.Serializable;

public enum MySqlDatabaseSyncMode implements Serializable {
    STATIC,
    DYNAMIC
}
