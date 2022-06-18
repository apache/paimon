package org.apache.flink.table.store.file.data;

import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.KeyValueSerializerTest;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.format.FlushingFileFormat;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataFileReaderTest {
    
    @TempDir
    java.nio.file.Path tempDir;
    private final DataFileTestDataGenerator gen =
        DataFileTestDataGenerator.builder().memTableCapacity(20).build();
    
    @Test
    public void testImplictDataTranslate() throws Exception {
        RowType actualKeyType = new RowType(singletonList(new RowType.RowField("k", new VarCharType())));
        RowType actualValueType = new RowType(singletonList(new RowType.RowField("v", new VarCharType())));
        
        GenericRowData excepted = new GenericRowData(actualKeyType.getFieldCount());
        excepted.setField(0, StringData.fromString("111"));
        excepted.setField(1, StringData.fromString("222"));
        
        KeyValue exceptedResult = new KeyValueSerializer(
            TestKeyValueGenerator.KEY_TYPE, TestKeyValueGenerator.DEFAULT_ROW_TYPE).fromRow(excepted);
        testWriteAndReadDataFileImpl(actualKeyType,actualValueType,"json",exceptedResult);
    }
    
    private void testWriteAndReadDataFileImpl(RowType keyRowType, RowType valueRowType,String format,KeyValue toExcepted) throws Exception {
        DataFileTestDataGenerator.Data data = gen.next();
        DataFileWriter dataFileWriter = createDataFileWriter(tempDir.toString(), format, keyRowType, valueRowType);
        DataFileReader reader = createDataFileReader(keyRowType,valueRowType, tempDir.toString(), format, null, null);
        List<DataFileMeta> actualMetas =
            dataFileWriter.write(CloseableIterator.fromList(data.content, kv -> {}), 0);
        assertData(
            data,
            actualMetas,
            reader,
            toExcepted);
    }
    
    protected DataFileReader createDataFileReader(
        RowType keyRowType, RowType valueRowType, String path, String format, int[][] keyProjection, int[][] valueProjection) {
        FileStorePathFactory pathFactory =new FileStorePathFactory(new Path(path));
        DataFileReader.Factory factory =
            new DataFileReader.Factory(
                keyRowType,
                valueRowType,
                new FlushingFileFormat(format),
                pathFactory);
        if (keyProjection != null) {
            factory.withKeyProjection(keyProjection);
        }
        if (valueProjection != null) {
            factory.withValueProjection(valueProjection);
        }
        return factory.create(BinaryRowDataUtil.EMPTY_ROW, 0);
    }
    
    protected DataFileWriter createDataFileWriter(String path, String format, RowType exceptedKey, RowType exceptedValue) {
        FileStorePathFactory pathFactory =
            new FileStorePathFactory(
                new Path(path),
                RowType.of(),
                FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.defaultValue(),
                format);
        int suggestedFileSize = ThreadLocalRandom.current().nextInt(8192) + 1024;
        return new DataFileWriter.Factory(
            exceptedKey,
            exceptedValue,
           
            new FlushingFileFormat(format),
            pathFactory,
            suggestedFileSize)
            .create(BinaryRowDataUtil.EMPTY_ROW, 0);
    }
    
    
    private void assertData(
        DataFileTestDataGenerator.Data data,
        List<DataFileMeta> actualMetas,
        DataFileReader fileReader,
        KeyValue  toExpectedKv)
        throws Exception {
        Iterator<KeyValue> expectedIterator = data.content.iterator();
        for (DataFileMeta meta : actualMetas) {
            // check the contents of data file
            CloseableIterator<KeyValue> actualKvsIterator =
                new RecordReaderIterator<>(fileReader.read(meta.fileName()));
            while (actualKvsIterator.hasNext()) {
                assertThat(expectedIterator.hasNext()).isTrue();
                KeyValue actualKv = actualKvsIterator.next();
                assertTrue(actualKv.equals(toExpectedKv));
            }
            actualKvsIterator.close();
        }
    }
}
