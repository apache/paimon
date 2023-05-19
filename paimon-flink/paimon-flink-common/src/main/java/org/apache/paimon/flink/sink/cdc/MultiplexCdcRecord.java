package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.types.RowKind;

import java.util.Map;

public class MultiplexCdcRecord extends CdcRecord {
    private final String databaseName;
    private final String tableName;

    public MultiplexCdcRecord(
            String databaseName, String tableName, RowKind kind, Map<String, String> fields) {
        super(kind, fields);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static MultiplexCdcRecord fromCdcRecord(
            String databaseName, String tableName, CdcRecord record) {
        return new MultiplexCdcRecord(
                databaseName, tableName, record.getKind(), record.getFields());
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }
}
