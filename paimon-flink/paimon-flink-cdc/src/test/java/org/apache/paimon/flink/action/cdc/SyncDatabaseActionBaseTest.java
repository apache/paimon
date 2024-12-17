package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.flink.action.cdc.kafka.KafkaSyncDatabaseAction;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.RowKind;
import org.junit.Assert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyncDatabaseActionBaseTest {
    private static final String ANY_DB = "any_db";
    private static final String WHITE_DB = "white_db";
    private static final String BLACK_DB = "black_db";
    private static final String WHITE_TBL = "white_tbl";
    private static final String BLACK_TBL = "black_tbl";


    private SyncDatabaseActionBase kafkaSyncDbAction;
    private RichCdcMultiplexRecord whiteAnyDbCdcRecord;
    private RichCdcMultiplexRecord blackAnyDbCdcRecord;
    private RichCdcMultiplexRecord whiteCdcRecord;
    private RichCdcMultiplexRecord blackCdcRecord;
    private RichCdcMultiplexRecord whiteDbBlackTblCdcRecord;
    private RichCdcMultiplexRecord blackDbWhiteTblCdcRecord;

    @TempDir
    private java.nio.file.Path tmp;

    @BeforeEach
    public void setUp() throws Exception {
        LocalFileIO localFileIO = new LocalFileIO();
        Path defaultDb = new Path(tmp.toString(), "default.db");
        localFileIO.mkdirs(defaultDb);

        kafkaSyncDbAction = new KafkaSyncDatabaseAction(tmp.toString(), "default", new HashMap<>(), new HashMap<>() );

        Map<String, String> rawData = new HashMap<>();
        rawData.put("field", "value");

        CdcRecord cdcData =new CdcRecord(RowKind.INSERT, rawData);
        whiteAnyDbCdcRecord = new RichCdcMultiplexRecord(ANY_DB, WHITE_TBL, Arrays.asList(), Arrays.asList(),cdcData);
        blackAnyDbCdcRecord = new RichCdcMultiplexRecord(ANY_DB, BLACK_TBL, Arrays.asList(), Arrays.asList(),cdcData);
        whiteCdcRecord = new RichCdcMultiplexRecord(WHITE_DB, WHITE_TBL, Arrays.asList(), Arrays.asList(),cdcData);
        blackCdcRecord = new RichCdcMultiplexRecord(BLACK_DB, WHITE_TBL, Arrays.asList(), Arrays.asList(),cdcData);

        whiteDbBlackTblCdcRecord = new RichCdcMultiplexRecord(WHITE_DB, BLACK_TBL, Arrays.asList(), Arrays.asList(),cdcData);
        blackDbWhiteTblCdcRecord = new RichCdcMultiplexRecord(BLACK_DB, WHITE_TBL, Arrays.asList(), Arrays.asList(),cdcData);
    }

    @Test
    public void testSyncTablesWithoutDbLists() throws NoSuchMethodException, IOException {

        kafkaSyncDbAction.includingTables(WHITE_TBL);
        kafkaSyncDbAction.excludingTables(BLACK_TBL);

        RichCdcMultiplexRecordEventParser parser = (RichCdcMultiplexRecordEventParser) kafkaSyncDbAction.buildEventParserFactory().create();
        List<CdcRecord> parsedRecords;

        parser.setRawEvent(whiteAnyDbCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(1, parsedRecords.size());

        parser.setRawEvent(blackAnyDbCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(0, parsedRecords.size());
    }

    @Test
    public void testSyncTablesWithDbList(){
        kafkaSyncDbAction.includingDbs(WHITE_DB);
        kafkaSyncDbAction.excludingDbs(BLACK_DB);
        RichCdcMultiplexRecordEventParser parser = (RichCdcMultiplexRecordEventParser) kafkaSyncDbAction.buildEventParserFactory().create();
        List<CdcRecord> parsedRecords;

        parser.setRawEvent(whiteAnyDbCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(0, parsedRecords.size());

        parser.setRawEvent(blackAnyDbCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(0, parsedRecords.size());

        // white db and white table
        parser.setRawEvent(whiteCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(1, parsedRecords.size());

        parser.setRawEvent(blackAnyDbCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(0, parsedRecords.size());

    }

    @Test
    public void testSycTablesCrossDB(){
        kafkaSyncDbAction.includingDbs(WHITE_DB);
        kafkaSyncDbAction.excludingDbs(BLACK_DB);
        kafkaSyncDbAction.excludingTables(BLACK_TBL);
        RichCdcMultiplexRecordEventParser parser = (RichCdcMultiplexRecordEventParser) kafkaSyncDbAction.buildEventParserFactory().create();
        List<CdcRecord> parsedRecords;

        parser.setRawEvent(whiteDbBlackTblCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(0, parsedRecords.size());

        parser.setRawEvent(blackDbWhiteTblCdcRecord);
        parsedRecords = parser.parseRecords();
        Assert.assertEquals(0, parsedRecords.size());
    }


}
