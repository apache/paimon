package org.apache.paimon.flink;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.utils.BlockingIterator;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerExpITCase extends CatalogITCaseBase {
    @Test
    public void testConsumerIdExceptionInBatchMode() throws Exception {
        sql("CREATE TABLE T (a INT PRIMARY KEY NOT ENFORCED, b INT) WITH ('bucket' = '5')");
        writeRecords(false);
        innerTestSingleFieldOnce(false);
    }

    @Test
    public void testConsumerIdExceptionInStreamMode() throws Exception {
        streamSql("CREATE TABLE T (a INT PRIMARY KEY NOT ENFORCED, b INT) WITH ('bucket' = '5')");
        writeRecords(true);
        innerTestSingleFieldOnce(true);
    }

    private void writeRecords(boolean isStream) throws Exception {
        if (isStream) streamSql("INSERT INTO T VALUES (1, 2), (3, 4), (5, 6), (7, 8), (9, 10)");
        else sql("INSERT INTO T VALUES (1, 2), (3, 4), (5, 6), (7, 8), (9, 10)");
    }

    private void innerTestSingleFieldOnce(boolean isStream) throws Exception {
        if (isStream) {
            BlockingIterator<Row, Row> iterator =
                    BlockingIterator.of(
                            streamSqlIter(
                                    "SELECT * FROM T /*+ OPTIONS('consumer-id' = 'test-id','consumer.expiration-time'='3d') */ WHERE a = 1"));
            assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(1, 2));
            iterator.close();
        } else
            assertThat(
                            sql(
                                    "SELECT * FROM T /*+ OPTIONS('consumer-id' = 'test-id','consumer.expiration-time'='3d') */ WHERE a = 1"))
                    .containsExactlyInAnyOrder(Row.of(1, 2));
    }

    protected List<Row> streamSql(String query, Object... args) {
        try (CloseableIterator<Row> iter = sEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
