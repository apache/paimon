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

package org.apache.flink.table.store.log;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.table.store.CoreOptions.LOG_FORMAT;
import static org.apache.flink.table.store.CoreOptions.LOG_KEY_FORMAT;

/**
 * Base interface for configuring a default log table connector. The log table is used by managed
 * table factory.
 *
 * <p>Log tables are for processing only unbounded data. Support streaming reading and streaming
 * writing.
 */
public interface LogStoreTableFactory extends DynamicTableFactory {

    /** Enrich options from catalog and session information. */
    Map<String, String> enrichOptions(Context context);

    /** Notifies the listener that a table creation occurred. */
    void onCreateTable(Context context, int numBucket, boolean ignoreIfExists);

    /** Notifies the listener that a table drop occurred. */
    void onDropTable(Context context, boolean ignoreIfNotExists);

    /**
     * Creates a {@link LogSourceProvider} instance from a {@link CatalogTable} and additional
     * context information.
     */
    LogSourceProvider createSourceProvider(
            Context context,
            DynamicTableSource.Context sourceContext,
            @Nullable int[][] projectFields);

    /**
     * Creates a {@link LogSinkProvider} instance from a {@link CatalogTable} and additional context
     * information.
     */
    LogSinkProvider createSinkProvider(Context context, DynamicTableSink.Context sinkContext);

    // --------------------------------------------------------------------------------------------

    static LogStoreTableFactory discoverLogStoreFactory(ClassLoader cl, String identifier) {
        return FactoryUtil.discoverFactory(cl, LogStoreTableFactory.class, identifier);
    }

    static DecodingFormat<DeserializationSchema<RowData>> getKeyDecodingFormat(
            TableFactoryHelper helper) {
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, LOG_KEY_FORMAT);
        validateKeyFormat(format, helper.getOptions().get(LOG_KEY_FORMAT));
        return format;
    }

    static EncodingFormat<SerializationSchema<RowData>> getKeyEncodingFormat(
            TableFactoryHelper helper) {
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, LOG_KEY_FORMAT);
        validateKeyFormat(format, helper.getOptions().get(LOG_KEY_FORMAT));
        return format;
    }

    static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            TableFactoryHelper helper) {
        DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, LOG_FORMAT);
        validateValueFormat(format, helper.getOptions().get(LOG_FORMAT));
        return format;
    }

    static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            TableFactoryHelper helper) {
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, LOG_FORMAT);
        validateValueFormat(format, helper.getOptions().get(LOG_FORMAT));
        return format;
    }

    static void validateKeyFormat(Format format, String name) {
        if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    String.format(
                            "A key format should only deal with INSERT-only records. "
                                    + "But %s has a changelog mode of %s.",
                            name, format.getChangelogMode()));
        }
    }

    static void validateValueFormat(Format format, String name) {
        if (!format.getChangelogMode().equals(ChangelogMode.all())) {
            throw new ValidationException(
                    String.format(
                            "A value format should deal with all records. "
                                    + "But %s has a changelog mode of %s.",
                            name, format.getChangelogMode()));
        }
    }
}
