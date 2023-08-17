package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** this is a doc. */
public class CompactorSinkBuilderForDb {
    private final Catalog.Loader catalogLoader;
    protected Catalog catalog;

    private Options options;
    private CoreOptions coreOptions;
    protected BucketMode bucketMode;

    private DataStream<RowData> input;

    public CompactorSinkBuilderForDb(
            Catalog.Loader catalogLoader,
            BucketMode bucketMode,
            Options options,
            CoreOptions coreOptions) {
        this.catalogLoader = catalogLoader;
        this.bucketMode = bucketMode;
        this.options = options;
        this.coreOptions = coreOptions;
    }

    public CompactorSinkBuilderForDb withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    public DataStreamSink<?> build() {
        switch (bucketMode) {
            case FIXED:
            case DYNAMIC:
                return buildForBucketAware();
            case UNAWARE:
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    private DataStreamSink<?> buildForBucketAware() {
        DataStream<RowData> partitioned = partition(input, new BucketsRowChannelComputer(), null);
        // +I 2|20221208|15|0|0|default|table1
        // +I 2|20221209|15|0|0|default|table1
        // +I 2|20221208|16|0|0|default|table1
        // +I 2|20221208|15|0|0|default|table2
        // +I 2|20221209|15|0|0|default|table2
        // +I 2|20221208|16|0|0|default|table2
        return new CompactorSinkForDb(catalogLoader, options, coreOptions).sinkFrom(partitioned);
    }
}
