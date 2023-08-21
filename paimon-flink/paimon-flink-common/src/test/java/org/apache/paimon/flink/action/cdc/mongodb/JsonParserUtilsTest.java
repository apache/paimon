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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** UT cases for {@link JsonParserUtils}. */
public class JsonParserUtilsTest {

    @Test
    public void testGetJsonObject() {
        String json =
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": { \"totalResults\": 1, \"resultsPerPage\": 1 }, \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }";
        String output = JsonParserUtils.evaluate(json, "$.pageInfo.totalResults");
        assertEquals("1", output);
    }

    @Test
    public void testNestedGetJsonObject() {
        String json =
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": { \"pagehit\":{ \"kind\": \"youtube#video\" } ,\"totalResults\": 1, \"resultsPerPage\": 1 }, \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }";
        String output = JsonParserUtils.evaluate(json, "$.pageInfo.pagehit");
        assertEquals("{\"kind\":\"youtube#video\"}", output);
    }

    @Test
    public void testStringWhenNotJson() {
        String json =
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": \"page\", \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }";
        String output = JsonParserUtils.evaluate(json, "$.pageinfo.test_field");
        assertNull(output);
    }

    @Test
    public void testStringWhenJson() {
        String json =
                "{ \"kind\": \"youtube#videoListResponse\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/mYlS4-ghMGhc1wTFCwoQl3IYDZc\\\"\", \"pageInfo\": \"page\", \"items\": [ { \"kind\": \"youtube#video\", \"etag\": \"\\\"79S54kzisD_9SOTfQLu_0TVQSpY/A4foLs-VO317Po_ulY6b5mSimZA\\\"\", \"id\": \"wHkPb68dxEw\", \"statistics\": { \"viewCount\": \"9211\", \"likeCount\": \"79\", \"dislikeCount\": \"11\", \"favoriteCount\": \"0\", \"commentCount\": \"29\" }, \"topicDetails\": { \"topicIds\": [ \"/m/02mjmr\" ], \"relevantTopicIds\": [ \"/m/0cnfvd\", \"/m/01jdpf\" ] } } ] }";
        String output = JsonParserUtils.evaluate(json, "$.pageInfo");
        assertEquals("page", output);
    }

    @Test
    public void testJsonArray() {
        String json =
                "{\"country\":\"Switzerland\",\"languages\": [ \"Italian\" ],\"religions\": \"christian\"}";
        String output = JsonParserUtils.evaluate(json, "$.languages[0]");
        assertEquals("Italian", output);
    }

    @Test
    public void testJsonMap() {
        String json =
                "{\"country\":\"Switzerland\",\"languages\":\"Italian\",\"religions\": { \"f\": \"v\", \"n\":null} }";
        String output = JsonParserUtils.evaluate(json, "$.religions.f");
        assertEquals("v", output);
    }
}
