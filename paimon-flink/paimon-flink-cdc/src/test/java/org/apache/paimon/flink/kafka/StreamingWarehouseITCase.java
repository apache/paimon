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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.function.Function;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.assertj.core.api.Assertions.assertThat;

/** Paimon IT case to test concurrent batch overwrite and streaming insert into. */
public class StreamingWarehouseITCase extends KafkaTableTestBase {

    @Test
    public void testUserStory() throws Exception {
        init(createAndRegisterTempFile("").toString(), 1);
        // Step1: define trade order table schema
        String orderSource =
                "CREATE TEMPORARY TABLE IF NOT EXISTS trade_orders (\n"
                        + "    order_id BIGINT NOT NULL,\n"
                        + "    order_timestamp AS LOCALTIMESTAMP,\n"
                        + "    buyer_id STRING,\n"
                        + "    order_amount DOUBLE,\n"
                        + "    loyalty_discount DOUBLE,\n"
                        + "    shipping_fee DOUBLE,\n"
                        + "    order_verified BOOLEAN,\n"
                        + "    PRIMARY KEY (order_id) NOT ENFORCED\n"
                        + "  )\n"
                        + "WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '10',\n"
                        + "    'fields.order_id.kind' = 'random',\n"
                        + "    'fields.order_id.min' = '1',\n"
                        + "    'fields.buyer_id.kind' = 'random',\n"
                        + "    'fields.buyer_id.length' = '3',\n"
                        + "    'fields.order_amount.min' = '10',\n"
                        + "    'fields.order_amount.max' = '1000',\n"
                        + "    'fields.loyalty_discount.min' = '0',\n"
                        + "    'fields.loyalty_discount.max' = '10',\n"
                        + "    'fields.shipping_fee.min' = '5',\n"
                        + "    'fields.shipping_fee.max' = '20'\n"
                        + "  );";

        String cleanedOrders =
                String.format(
                        "CREATE TABLE IF NOT EXISTS cleaned_trade_order (\n"
                                + "    order_id BIGINT NOT NULL,\n"
                                + "    order_timestamp TIMESTAMP (3),\n"
                                + "    buyer_id STRING,\n"
                                + "    order_amount DOUBLE,\n"
                                + "    loyalty_discount DOUBLE,\n"
                                + "    shipping_fee DOUBLE,\n"
                                + "    order_verified BOOLEAN,\n"
                                + "    actual_gmv DOUBLE,\n"
                                + "    dt STRING,\n"
                                + "    PRIMARY KEY (dt, order_id) NOT ENFORCED\n"
                                + "  )\n"
                                + "PARTITIONED BY (dt)\n"
                                + "WITH (\n"
                                + "    'bucket' = '1',\n"
                                + "    'log.system' = 'kafka',\n"
                                + "    'kafka.bootstrap.servers' = '%s',\n"
                                + "    'kafka.topic' = 'cleaned_trade_order');",
                        getBootstrapServers());
        sEnv.executeSql(orderSource);
        bEnv.executeSql(orderSource);
        sEnv.executeSql(cleanedOrders);

        // Step2: batch write some corrupted historical data
        String corruptedHistoricalData =
                "INSERT INTO cleaned_trade_order\n"
                        + "PARTITION (dt = '2022-04-14')\n"
                        + "SELECT order_id,\n"
                        + "  TIMESTAMPADD (\n"
                        + "    HOUR,\n"
                        + "    RAND_INTEGER (24),\n"
                        + "    TO_TIMESTAMP ('2022-04-14', 'yyyy-MM-dd')\n"
                        + "  ) AS order_timestamp,\n"
                        + "  IF (\n"
                        + "    order_verified\n"
                        + "    AND order_id % 2 = 1,\n"
                        + "    '404NotFound',\n"
                        + "    buyer_id\n"
                        + "  ) AS buyer_id,\n" // corrupt data conditionally
                        + "  order_amount,\n"
                        + "  loyalty_discount,\n"
                        + "  shipping_fee,\n"
                        + "  order_verified,\n"
                        + "  IF (\n"
                        + "    order_verified\n"
                        + "    AND order_id % 2 = 1,\n"
                        + "    -1,\n"
                        + "    order_amount + shipping_fee - loyalty_discount\n"
                        + "  ) AS actual_gmv\n" // corrupt data conditionally
                        + "FROM\n"
                        + "  trade_orders\n"
                        + "  /*+ OPTIONS ('number-of-rows' = '50')  */";
        bEnv.executeSql(corruptedHistoricalData).await();

        // Step3: start downstream streaming task to read
        String streamingRead = "SELECT * FROM cleaned_trade_order";
        BlockingIterator<Row, CleanedTradeOrder> streamIter =
                BlockingIterator.of(sEnv.executeSql(streamingRead).collect(), ORDER_CONVERTER);
        // verify historical data is corrupted
        streamIter.collect(50).stream()
                .filter(order -> order.orderVerified && order.orderId % 2 == 1)
                .forEach(
                        order -> {
                            assertThat(order.buyerId).isEqualTo("404NotFound");
                            assertThat(order.actualGmv).isEqualTo(-1);
                            assertThat(order.dt).isEqualTo("2022-04-14");
                        });

        // Step4: prepare day-to-day streaming sync task
        String streamingWrite =
                "INSERT INTO cleaned_trade_order\n"
                        + "SELECT order_id,\n"
                        + "  order_timestamp,\n"
                        + "  buyer_id,\n"
                        + "  order_amount,\n"
                        + "  loyalty_discount,\n"
                        + "  shipping_fee,\n"
                        + "  order_verified,\n"
                        + "  order_amount + shipping_fee - loyalty_discount AS actual_gmv,\n"
                        + "  DATE_FORMAT (order_timestamp, 'yyyy-MM-dd') AS dt\n"
                        + "FROM\n"
                        + "  trade_orders";
        JobClient dailyTaskHandler = sEnv.executeSql(streamingWrite).getJobClient().get();
        while (true) {
            if (dailyTaskHandler.getJobStatus().get() == JobStatus.RUNNING) {
                break;
            }
        }

        // Step5: prepare back-fill task to correct historical data
        String backFillOverwrite =
                "INSERT OVERWRITE cleaned_trade_order\n"
                        + "SELECT order_id,\n"
                        + "  order_timestamp,\n"
                        + "  IF (buyer_id = '404NotFound', '_ANONYMOUS_USER_', buyer_id) AS buyer_id,\n"
                        + "  order_amount,\n"
                        + "  loyalty_discount,\n"
                        + "  shipping_fee,\n"
                        + "  order_verified,\n"
                        + "  IF (\n"
                        + "    actual_gmv = -1,\n"
                        + "    order_amount + shipping_fee - loyalty_discount,\n"
                        + "    actual_gmv\n"
                        + "  ) AS actual_gmv,\n"
                        + "  dt\n"
                        + "FROM\n"
                        + "  cleaned_trade_order\n"
                        + "WHERE\n"
                        + "  dt = '2022-04-14';";

        // wait for back-fill task to finish
        bEnv.executeSql(backFillOverwrite).await();

        // Step6: check streaming read does not achieve any changelog
        int checkSize = 200;
        while (checkSize > 0) {
            Thread.sleep(1000L);
            streamIter
                    .collect(10) // rows-per-second is 10
                    .forEach(order -> assertThat(order.dt).isGreaterThan("2022-04-14"));
            checkSize -= 10;
        }

        // verify corrupted historical data is corrected
        BlockingIterator<Row, CleanedTradeOrder> batchIter =
                BlockingIterator.of(
                        bEnv.executeSql("SELECT * FROM cleaned_trade_order WHERE dt ='2022-04-14'")
                                .collect(),
                        ORDER_CONVERTER);
        batchIter.collect(50).stream()
                .filter(order -> order.orderVerified && order.orderId % 2 == 1)
                .forEach(
                        order -> {
                            assertThat(order.buyerId).isEqualTo("_ANONYMOUS_USER_");
                            assertThat(order.actualGmv)
                                    .isEqualTo(
                                            order.orderAmount
                                                    + order.shippingFee
                                                    - order.loyaltyDiscount);
                            assertThat(order.dt).isEqualTo("2022-04-14");
                        });

        streamIter.close();
        dailyTaskHandler.cancel().get();
    }

    private static final Function<Row, CleanedTradeOrder> ORDER_CONVERTER =
            (row) -> {
                assert row != null && row.getArity() == 9;
                CleanedTradeOrder order = new CleanedTradeOrder();
                order.orderId = (Long) row.getField(0);
                order.orderTimestamp = (LocalDateTime) row.getField(1);
                order.buyerId = (String) row.getField(2);
                order.orderAmount = (Double) row.getField(3);
                order.loyaltyDiscount = (Double) row.getField(4);
                order.shippingFee = (Double) row.getField(5);
                order.orderVerified = (Boolean) row.getField(6);
                order.actualGmv = (Double) row.getField(7);
                order.dt = (String) row.getField(8);
                return order;
            };

    /** A test POJO. */
    private static class CleanedTradeOrder {
        protected Long orderId;
        protected LocalDateTime orderTimestamp;
        protected String buyerId;
        protected Double orderAmount;
        protected Double loyaltyDiscount;
        protected Double shippingFee;
        protected Boolean orderVerified;
        protected Double actualGmv;
        protected String dt;
    }
}
