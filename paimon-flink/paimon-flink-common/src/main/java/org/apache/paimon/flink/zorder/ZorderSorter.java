/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.zorder;

import org.apache.paimon.flink.action.ZorderRewriteAction;
import org.apache.paimon.utils.ByteBuffers;
import org.apache.paimon.utils.ZOrderByteUtils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.paimon.utils.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

/**
 * This is a table sorter which will sort the records by the z-order of specified columns. It works
 * in a way mixed by table api and stream api. By sql select, it constructs the input stream, then
 * compute the z-order-index using stream api. After add the column of z-order, it transfers the
 * data stream back to table, and use the sql clause "order by" to sort. Finally, {@link
 * ZorderRewriteAction} will remove the "z-order" column and insert sorted record to the target
 * table.
 *
 * <pre>
 *                     toDataStream                    add z-order column                   fromDataStream                           order by
 * Table(sql select) ----------------> DataStream[Row] -------------------> DataStream[Row] -----------------> Table(with z-order) -----------------> Table(sorted)
 * </pre>
 */
public class ZorderSorter {

    private static final String Z_ORDER_COLUMN_NAME = "Z_ORDER";

    private final StreamTableEnvironment batchTEnv;
    private final Table origin;
    private final List<String> zOrderColNames;

    public ZorderSorter(
            StreamTableEnvironment batchTEnv, Table origin, List<String> zOrderColNames) {
        this.batchTEnv = batchTEnv;
        this.origin = origin;
        this.zOrderColNames = zOrderColNames;
        checkColNames();
    }

    public Table apply() {
        return convertTable(origin)
                .orderBy($(Z_ORDER_COLUMN_NAME))
                .dropColumns($(Z_ORDER_COLUMN_NAME));
    }

    private void checkColNames() {
        List<String> columnNames =
                origin.getResolvedSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        for (String zColumn : zOrderColNames) {
            if (!columnNames.contains(zColumn)) {
                throw new RuntimeException(
                        "Can't find column "
                                + zColumn
                                + " in table columns. Possible columns are ["
                                + columnNames.stream().reduce((a, b) -> a + "," + b).get()
                                + "]");
            }
        }
    }

    // This method convert source table to z-order-added table
    private Table convertTable(Table source) {
        // We need this new schema to shift datastream back to table (with z-order column)
        Schema schema =
                Schema.newBuilder()
                        .fromResolvedSchema(origin.getResolvedSchema())
                        .column(Z_ORDER_COLUMN_NAME, DataTypes.BYTES())
                        .build();

        return batchTEnv.fromDataStream(
                addZorderColumnStream(batchTEnv.toDataStream(source)), schema);
    }

    private DataStream<Row> addZorderColumnStream(DataStream<Row> inputStream) {
        // Every z-order related column needs a "function" to devote given bytes.
        final Map<Integer, ZorderBaseFunction> zorderFunctionMap = new LinkedHashMap<>();
        List<Column> columns = origin.getResolvedSchema().getColumns();
        // We need dataTypes and rowFields to construct output type for datastream with z-order.
        List<DataType> dataTypes = new ArrayList<>();
        List<RowType.RowField> rowFields = new ArrayList<>();

        // Construct zorderFunctionmap and fill dataTypes, rowFields
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            Column column = columns.get(columnIndex);
            dataTypes.add(column.getDataType());
            rowFields.add(
                    new RowType.RowField(column.getName(), column.getDataType().getLogicalType()));
            if (zOrderColNames.contains(column.getName())) {
                zorderFunctionMap.put(columnIndex, zmapColumnToCalculator(column));
            }
        }
        // Add the z-order RowField and DataType
        rowFields.add(
                new RowType.RowField(Z_ORDER_COLUMN_NAME, DataTypes.BYTES().getLogicalType()));
        dataTypes.add(DataTypes.BYTES());

        // Add z_order column to data stream with a RichMapFunction, we specify the output type with
        // a Table FieldsDataType.
        return inputStream.map(
                new RichMapFunction<Row, Row>() {
                    private final int totalBytes = PRIMITIVE_BUFFER_SIZE * zorderFunctionMap.size();
                    private transient ByteBuffer reuse;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        reuse = ByteBuffer.allocate(totalBytes);
                        zorderFunctionMap.values().forEach(ZorderBaseFunction::open);
                    }

                    @Override
                    public Row map(Row value) {
                        byte[][] columnBytes = new byte[zorderFunctionMap.size()][];
                        int orderColumnIndex = 0;
                        for (Map.Entry<Integer, ZorderBaseFunction> entry :
                                zorderFunctionMap.entrySet()) {
                            Object v = value.getField(entry.getKey());
                            columnBytes[orderColumnIndex++] = entry.getValue().apply(v);
                        }

                        byte[] zorder =
                                ZOrderByteUtils.interleaveBits(columnBytes, totalBytes, reuse);
                        return Row.join(value, Row.of(zorder));
                    }
                },
                ExternalTypeInfo.of(new FieldsDataType(new RowType(rowFields), dataTypes)));
    }

    public ZorderBaseFunction zmapColumnToCalculator(Column column) {
        DataType type = column.getDataType();
        if (DataTypes.TINYINT().equals(type)) {
            return new ZorderTinyIntFunction();
        } else if (DataTypes.SMALLINT().equals(type)) {
            return new ZorderShortFunction();
        } else if (DataTypes.INT().equals(type)) {
            return new ZorderIntFunction();
        } else if (DataTypes.BIGINT().equals(type)) {
            return new ZorderLongFunction();
        } else if (DataTypes.FLOAT().equals(type)) {
            return new ZorderFloatFunction();
        } else if (DataTypes.DOUBLE().equals(type)) {
            return new ZorderDoubleFunction();
        } else if (DataTypes.STRING().equals(type)) {
            return new ZorderStringFunction();
        } else if (DataTypes.BYTES().equals(type)) {
            return new ZorderBytesFunction();
        } else if (DataTypes.BOOLEAN().equals(type)) {
            return new ZorderBooleanFunction();
        } else if (DataTypes.TIMESTAMP().equals(type)) {
            return new ZorderTimestampFunction();
        } else if (DataTypes.DATE().equals(type)) {
            return new ZorderDateFunction();
        } else if (DataTypes.TIME().equals(type)) {
            return new ZorderTimeFunction();
        } else if (type.getLogicalType() instanceof DecimalType) {
            return new ZorderDecimalFunction();
        } else if (type.getLogicalType() instanceof BinaryType) {
            return new ZorderBytesFunction();
        } else if (type.getLogicalType() instanceof VarBinaryType) {
            return new ZorderBytesFunction();
        } else if (type.getLogicalType() instanceof CharType) {
            return new ZorderStringFunction();
        } else if (type.getLogicalType() instanceof VarCharType) {
            return new ZorderStringFunction();
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot use column %s of type %s in ZOrdering, the type is unsupported",
                            column, type));
        }
    }

    /** BaseFunction to convert row field record to devoted bytes. */
    public abstract static class ZorderBaseFunction
            implements Function<Object, byte[]>, Serializable {

        protected transient ByteBuffer reuse;

        public void open() {
            reuse = ByteBuffer.allocate(PRIMITIVE_BUFFER_SIZE);
        }
    }

    /** Function used for Type Byte(TinyInt). */
    public static class ZorderTinyIntFunction extends ZorderBaseFunction {

        public byte[] apply(Object b) {
            return ZOrderByteUtils.tinyintToOrderedBytes((Byte) b, reuse).array();
        }
    }

    /** Function used for Type Short. */
    public static class ZorderShortFunction extends ZorderBaseFunction {

        public byte[] apply(Object s) {
            return ZOrderByteUtils.shortToOrderedBytes((Short) s, reuse).array();
        }
    }

    /** Function used for Type Int. */
    public static class ZorderIntFunction extends ZorderBaseFunction {

        public byte[] apply(Object i) {
            return ZOrderByteUtils.intToOrderedBytes((Integer) i, reuse).array();
        }
    }

    /** Function used for Type String. */
    public static class ZorderStringFunction extends ZorderBaseFunction {

        public byte[] apply(Object s) {
            return ZOrderByteUtils.stringToOrderedBytes(
                            (String) s,
                            PRIMITIVE_BUFFER_SIZE,
                            reuse,
                            StandardCharsets.UTF_8.newEncoder())
                    .array();
        }
    }

    /** Function used for Type Long. */
    public static class ZorderLongFunction extends ZorderBaseFunction {

        public byte[] apply(Object val) {
            return ZOrderByteUtils.longToOrderedBytes((Long) val, reuse).array();
        }
    }

    /** Function used for Type Date. */
    public static class ZorderDateFunction extends ZorderBaseFunction {

        public byte[] apply(Object val) {
            LocalDate date = (LocalDate) val;
            return ZOrderByteUtils.longToOrderedBytes(date.toEpochDay(), reuse).array();
        }
    }

    /** Function used for Type Time. */
    public static class ZorderTimeFunction extends ZorderBaseFunction {

        public byte[] apply(Object val) {
            LocalTime time = (LocalTime) val;
            return ZOrderByteUtils.longToOrderedBytes(time.toNanoOfDay(), reuse).array();
        }
    }

    /** Function used for Type Timestamp. */
    public static class ZorderTimestampFunction extends ZorderBaseFunction {

        public byte[] apply(Object val) {
            LocalDateTime time = (LocalDateTime) val;
            return ZOrderByteUtils.longToOrderedBytes(time.toEpochSecond(ZoneOffset.UTC), reuse)
                    .array();
        }
    }

    /** Function used for Type Float. */
    public static class ZorderFloatFunction extends ZorderBaseFunction {

        public byte[] apply(Object f) {
            return ZOrderByteUtils.floatToOrderedBytes((Float) f, reuse).array();
        }
    }

    /** Function used for Type Double. */
    public static class ZorderDoubleFunction extends ZorderBaseFunction {

        public byte[] apply(Object d) {
            return ZOrderByteUtils.doubleToOrderedBytes((Double) d, reuse).array();
        }
    }

    /** Function used for Type Decimal. */
    public static class ZorderDecimalFunction extends ZorderBaseFunction {

        public byte[] apply(Object b) {
            BigDecimal bigDecimal = (BigDecimal) b;
            return ZOrderByteUtils.doubleToOrderedBytes(bigDecimal.doubleValue(), reuse).array();
        }
    }

    /** Function used for Type Boolean. */
    public static class ZorderBooleanFunction extends ZorderBaseFunction {

        public byte[] apply(Object b) {
            reuse = ByteBuffers.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
            reuse.put(0, (byte) ((Boolean) b ? -127 : 0));
            return reuse.array();
        }
    }

    /** Function used for Type Bytes. */
    public static class ZorderBytesFunction extends ZorderBaseFunction {

        public byte[] apply(Object bytes) {
            return ZOrderByteUtils.byteTruncateOrFill((byte[]) bytes, PRIMITIVE_BUFFER_SIZE, reuse)
                    .array();
        }
    }
}
