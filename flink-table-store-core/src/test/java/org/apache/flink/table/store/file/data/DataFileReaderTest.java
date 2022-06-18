package org.apache.flink.table.store.file.data;

import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.format.FlushingFileFormat;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataFileReaderTest {
    
    @TempDir
    java.nio.file.Path tempDir;
    private long sequenceNumber;
    
    @BeforeEach
    public void setUp(){
        this.sequenceNumber = 0;
    }
    
    
    @Test
    public void testImplictDataTranslate() throws Exception {
        RowType actualKeyType = (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.VARBINARY(4)))
                .getLogicalType();
        
        RowType actualValueType = (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("name", DataTypes.VARCHAR(16)))
                .getLogicalType();
        
    
        GenericRowData exceptedK = new GenericRowData(1);
        exceptedK.setField(0, StringData.fromString("id"));
    
        GenericRowData exceptedV = new GenericRowData(1);
        exceptedV.setField(0, StringData.fromString("name"));
        
       
        testWriteAndReadDataFileImpl(actualKeyType,actualValueType,"avro",exceptedK,exceptedV);
    }
    
    private void testWriteAndReadDataFileImpl(RowType keyRowType, RowType valueRowType,String format,GenericRowData toExceptedK,GenericRowData toExceptedV) throws Exception {
        DataFileWriter dataFileWriter = createDataFileWriter(tempDir.toString(), format, keyRowType, valueRowType);
        DataFileReader reader = createDataFileReader(keyRowType,valueRowType, tempDir.toString(), format, null, null);
        
        List<KeyValue> keyValues = new ArrayList<>();
        RowDataSerializer rowDataKeySerializer = new RowDataSerializer(keyRowType);
        RowDataSerializer rowDataValueSerializer = new RowDataSerializer(valueRowType);
        keyValues.add(gengerate(rowDataKeySerializer, rowDataValueSerializer, toExceptedK, toExceptedV));
        List<DataFileMeta> actualMetas =
            dataFileWriter.write(CloseableIterator.fromList(keyValues, kv -> {}), 0);
    
        assertData(
            keyValues,
            actualMetas,
            reader,
            toExceptedK,
            toExceptedV);
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
    
    public KeyValue gengerate(RowDataSerializer keySerializer,RowDataSerializer valueSerializer,  GenericRowData keyRowData,GenericRowData valueRowData) {
        return new KeyValue()
            .replace(
                keySerializer
                    .toBinaryRow(keyRowData)
                    .copy(),
                sequenceNumber++,
                ValueKind.ADD,
                valueSerializer
                    .toBinaryRow(valueRowData)
                    .copy());
    }
    
    private void assertData(
        List<KeyValue> data,
        List<DataFileMeta> actualMetas,
        DataFileReader fileReader,
        GenericRowData exceptedK,
        GenericRowData exceptedV)
        throws Exception {
        Iterator<KeyValue> expectedIterator = data.iterator();
        for (DataFileMeta meta : actualMetas) {
            // check the contents of data file
            CloseableIterator<KeyValue> actualKvsIterator =
                new RecordReaderIterator<>(fileReader.read(meta.fileName()));
            while (actualKvsIterator.hasNext()) {
                assertThat(expectedIterator.hasNext()).isTrue();
                KeyValue actualKv = actualKvsIterator.next();
                RowData actualK = actualKv.key();
                RowData actualV = actualKv.value();
                assertTrue(actualK.equals(exceptedK));
                assertTrue(actualV.equals(exceptedV));
            }
            actualKvsIterator.close();
        }
    }
}
