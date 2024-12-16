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
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.orc.filter.OrcFilters;
import org.apache.paimon.format.orc.filter.OrcPredicateFunctionVisitor;
import org.apache.paimon.format.orc.filter.OrcSimpleStatsExtractor;
import org.apache.paimon.format.orc.writer.RowDataVectorizer;
import org.apache.paimon.format.orc.writer.Vectorizer;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.types.DataTypeChecks.getFieldTypes;

/** Orc {@link FileFormat}. */
@ThreadSafe
public class OrcFileFormat extends FileFormat {

    public static final String IDENTIFIER = "orc";

    private final Properties orcProperties;
    private final org.apache.hadoop.conf.Configuration readerConf;
    private final org.apache.hadoop.conf.Configuration writerConf;
    private final int readBatchSize;
    private final int writeBatchSize;
    private final boolean deletionVectorsEnabled;

    private static final Cache<Properties, Configuration> configCache =
            Caffeine.newBuilder().maximumSize(100).expireAfterWrite(Duration.ofMinutes(30)).build();

    public OrcFileFormat(FormatContext formatContext) {
        super(IDENTIFIER);
        this.orcProperties = getOrcProperties(formatContext.options(), formatContext);
        Configuration conf;
        Configuration cachedConf = configCache.getIfPresent(orcProperties);
        if (cachedConf != null) {
            conf = cachedConf;
        } else {
            conf = new org.apache.hadoop.conf.Configuration(false);
            this.orcProperties.forEach((k, v) -> conf.set(k.toString(), v.toString()));
            configCache.put(orcProperties, conf);
        }
        this.readerConf = conf;
        this.writerConf = conf;
        this.readBatchSize = formatContext.readBatchSize();
        this.writeBatchSize = formatContext.writeBatchSize();
        this.deletionVectorsEnabled = formatContext.options().get(DELETION_VECTORS_ENABLED);
    }

    @VisibleForTesting
    public Properties orcProperties() {
        return orcProperties;
    }

    @VisibleForTesting
    public int readBatchSize() {
        return readBatchSize;
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new OrcSimpleStatsExtractor(type, statsCollectors));
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
                readBatchSize,
                deletionVectorsEnabled);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        DataType refinedType = refineDataType(rowType);
        OrcTypeUtil.convertToOrcSchema((RowType) refinedType);
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

        TypeDescription typeDescription = OrcTypeUtil.convertToOrcSchema((RowType) refinedType);
        Vectorizer<InternalRow> vectorizer = new RowDataVectorizer(typeDescription, orcTypes);

        return new OrcWriterFactory(vectorizer, orcProperties, writerConf, writeBatchSize);
    }

    private Properties getOrcProperties(Options options, FormatContext formatContext) {
        Properties orcProperties = new Properties();
        orcProperties.putAll(getIdentifierPrefixOptions(options).toMap());

        if (!orcProperties.containsKey(OrcConf.COMPRESSION_ZSTD_LEVEL.getAttribute())) {
            orcProperties.setProperty(
                    OrcConf.COMPRESSION_ZSTD_LEVEL.getAttribute(),
                    String.valueOf(formatContext.zstdLevel()));
        }

        MemorySize blockSize = formatContext.blockSize();
        if (blockSize != null) {
            orcProperties.setProperty(
                    OrcConf.STRIPE_SIZE.getAttribute(), String.valueOf(blockSize.getBytes()));
        }

        return orcProperties;
    }

    public static DataType refineDataType(DataType type) {
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
