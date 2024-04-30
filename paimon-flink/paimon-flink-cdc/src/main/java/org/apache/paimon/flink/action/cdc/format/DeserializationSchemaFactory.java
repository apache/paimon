package org.apache.paimon.flink.action.cdc.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import java.util.List;

@FunctionalInterface
public interface DeserializationSchemaFactory {

    DeserializationSchema<CdcSourceRecord> createDeserializationSchema(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns);
}
