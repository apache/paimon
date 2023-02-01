/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.format.orc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.store.format.FormatReaderFactory;
import org.apache.flink.table.store.format.FormatWriterFactory;
import org.apache.flink.table.store.format.orc.filter.OrcFileStatsExtractor;
import org.apache.flink.table.store.format.orc.filter.OrcFilters;
import org.apache.flink.table.store.format.orc.filter.OrcPredicateFunctionVisitor;
import org.apache.flink.table.store.format.orc.reader.OrcSplitReaderUtil;
import org.apache.flink.table.store.format.orc.writer.RowDataVectorizer;
import org.apache.flink.table.store.format.orc.writer.Vectorizer;
import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DataTypes;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.MultisetType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.Projection;

import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.types.DataTypeChecks.getFieldTypes;

/** Orc {@link FileFormat}. The main code is copied from Flink {@code OrcFileFormatFactory}. */
public class OrcFileFormat extends FileFormat {

    public static final String IDENTIFIER = "orc";

    private final Properties orcProperties;
    private final org.apache.hadoop.conf.Configuration readerConf;
    private final org.apache.hadoop.conf.Configuration writerConf;

    public OrcFileFormat(Configuration formatOptions) {
        super(IDENTIFIER);
        this.orcProperties = getOrcProperties(formatOptions);
        this.readerConf = new org.apache.hadoop.conf.Configuration();
        this.orcProperties.forEach((k, v) -> readerConf.set(k.toString(), v.toString()));
        this.writerConf = new org.apache.hadoop.conf.Configuration();
    }

    @VisibleForTesting
    Properties orcProperties() {
        return orcProperties;
    }

    @Override
    public Optional<FileStatsExtractor> createStatsExtractor(RowType type) {
        return Optional.of(new OrcFileStatsExtractor(type));
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType type, int[][] projection, @Nullable List<Predicate> filters) {
        List<OrcFilters.Predicate> orcPredicates = new ArrayList<>();

        if (filters != null) {
            for (Predicate pred : filters) {
                Optional<OrcFilters.Predicate> orcPred =
                        pred.visit(OrcPredicateFunctionVisitor.VISITOR);
                orcPred.ifPresent(orcPredicates::add);
            }
        }

        return new OrcReaderFactory(
                readerConf,
                (RowType) refineDataType(type),
                Projection.of(projection).toTopLevelIndexes(),
                orcPredicates,
                2048);
    }

    /**
     * The {@link OrcWriterFactory} will create {@link ThreadLocalClassLoaderConfiguration} from the
     * input writer config to avoid classloader leaks.
     *
     * <p>TODO: The {@link ThreadLocalClassLoaderConfiguration} in {@link OrcWriterFactory} should
     * be removed after https://issues.apache.org/jira/browse/ORC-653 is fixed.
     *
     * @param type The data type for the {@link BulkWriter}
     * @return The factory of the {@link BulkWriter}
     */
    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        DataType refinedType = refineDataType(type);
        DataType[] orcTypes = getFieldTypes(refinedType).toArray(new DataType[0]);

        TypeDescription typeDescription = OrcSplitReaderUtil.toOrcType(refinedType);
        Vectorizer<InternalRow> vectorizer =
                new RowDataVectorizer(typeDescription.toString(), orcTypes);

        return new OrcWriterFactory(vectorizer, orcProperties, writerConf);
    }

    private static Properties getOrcProperties(ReadableConfig options) {
        Properties orcProperties = new Properties();
        Properties properties = new Properties();
        ((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
        properties.forEach((k, v) -> orcProperties.put(IDENTIFIER + "." + k, v));
        return orcProperties;
    }

    private static DataType refineDataType(DataType type) {
        switch (type.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                // OrcSplitReaderUtil#DataTypeToOrcType() only supports the DataTypes.BYTES()
                // logical type for BINARY and VARBINARY.
                return DataTypes.BYTES();
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                return new ArrayType(
                        arrayType.isNullable(), refineDataType(arrayType.getElementType()));
            case MAP:
                MapType mapType = (MapType) type;
                return new MapType(
                        refineDataType(mapType.getKeyType()),
                        refineDataType(mapType.getValueType()));
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return new MapType(
                        refineDataType(multisetType.getElementType()),
                        refineDataType(new IntType(false)));
            case ROW:
                RowType rowType = (RowType) type;
                return new RowType(
                        rowType.isNullable(),
                        rowType.getFields().stream()
                                .map(
                                        f ->
                                                new DataField(
                                                        f.id(),
                                                        f.name(),
                                                        refineDataType(f.type()),
                                                        f.description()))
                                .collect(Collectors.toList()));
            default:
                return type;
        }
    }
}
