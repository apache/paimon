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

package org.apache.paimon.format.orc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.format.orc.filter.OrcFilters;
import org.apache.paimon.format.orc.filter.OrcPredicateFunctionVisitor;
import org.apache.paimon.format.orc.filter.OrcTableStatsExtractor;
import org.apache.paimon.format.orc.reader.OrcSplitReaderUtil;
import org.apache.paimon.format.orc.writer.RowDataVectorizer;
import org.apache.paimon.format.orc.writer.Vectorizer;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.paimon.types.DataTypeChecks.getFieldTypes;

/** Orc {@link FileFormat}. */
@ThreadSafe
public class OrcFileFormat extends FileFormat {

    public static final String IDENTIFIER = "orc";

    private final Properties orcProperties;
    private final org.apache.hadoop.conf.Configuration readerConf;
    private final org.apache.hadoop.conf.Configuration writerConf;

    private final FormatContext formatContext;

    public OrcFileFormat(FormatContext formatContext) {
        super(IDENTIFIER);
        this.orcProperties = getOrcProperties(formatContext.formatOptions());
        this.readerConf = new org.apache.hadoop.conf.Configuration();
        this.orcProperties.forEach((k, v) -> readerConf.set(k.toString(), v.toString()));
        this.writerConf = new org.apache.hadoop.conf.Configuration();
        this.orcProperties.forEach((k, v) -> writerConf.set(k.toString(), v.toString()));
        this.formatContext = formatContext;
    }

    @VisibleForTesting
    public Properties orcProperties() {
        return orcProperties;
    }

    @VisibleForTesting
    public FormatContext formatContext() {
        return formatContext;
    }

    @Override
    public Optional<TableStatsExtractor> createStatsExtractor(
            RowType type, FieldStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new OrcTableStatsExtractor(type, statsCollectors));
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType projectedRowType, @Nullable List<Predicate> filters) {
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
                (RowType) refineDataType(projectedRowType),
                orcPredicates,
                formatContext.readBatchSize());
    }

    @Override
    public void validateDataFields(RowType rowType) {
        DataType refinedType = refineDataType(rowType);
        OrcSplitReaderUtil.toOrcType(refinedType);
    }

    /**
     * The {@link OrcWriterFactory} will create {@link ThreadLocalClassLoaderConfiguration} from the
     * input writer config to avoid classloader leaks.
     *
     * <p>TODO: The {@link ThreadLocalClassLoaderConfiguration} in {@link OrcWriterFactory} should
     * be removed after https://issues.apache.org/jira/browse/ORC-653 is fixed.
     *
     * @param type The data type for the writer
     * @return The factory of the writer
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

    private static Properties getOrcProperties(Options options) {
        Properties orcProperties = new Properties();
        Properties properties = new Properties();
        options.addAllToProperties(properties);
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
