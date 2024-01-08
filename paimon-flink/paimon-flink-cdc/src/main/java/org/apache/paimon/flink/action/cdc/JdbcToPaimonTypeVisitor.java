package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

/** jdbc to paimon data type visitor. */
@FunctionalInterface
public interface JdbcToPaimonTypeVisitor {
    DataType visit(
            String type,
            @Nullable Integer length,
            @Nullable Integer scale,
            TypeMapping typeMapping);
}
