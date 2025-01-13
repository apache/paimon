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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.actions.HttpReportMarkDoneAction;
import org.apache.paimon.partition.actions.HttpReportMarkDoneAction.HttpReportMarkDoneRequest;
import org.apache.paimon.partition.actions.HttpReportMarkDoneException;
import org.apache.paimon.rest.TestHttpWebServer;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.RecordedRequest;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION_PARAMS;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION_TIMEOUT;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION_URL;
import static org.apache.paimon.utils.InternalRowUtilsTest.ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case fo {@link HttpReportMarkDoneAction}. */
public class HttpReportMarkDoneActionTest {

    private static TestHttpWebServer server;

    private static final String partition = "partition";
    private static String params = "key1=value1,key2=value2";
    private static final String successResponse = "{\"result\":\"success\"}";
    private static final String failedResponse = "{\"result\":\"failed\"}";
    private static FileStoreTable fileStoreTable;
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void startServer() throws Exception {
        server = new TestHttpWebServer("");
        server.start();
        fileStoreTable = createFileStoreTable();
    }

    @After
    public void stopServer() throws Exception {
        server.stop();
    }

    @Test
    public void testHttpReportMarkDoneActionSuccessResponse() throws Exception {
        HttpReportMarkDoneAction httpReportMarkDoneAction = createHttpReportMarkDoneAction();

        server.enqueueResponse(successResponse, 200);

        httpReportMarkDoneAction.markDone(partition);
        RecordedRequest request = server.takeRequest(10, TimeUnit.SECONDS);
        assertRequest(request);

        String expectedResponse2 = "{\"unknow\" :\"unknow\", \"result\" :\"success\"}";
        server.enqueueResponse(expectedResponse2, 200);

        httpReportMarkDoneAction.markDone(partition);
        RecordedRequest request2 = server.takeRequest(10, TimeUnit.SECONDS);
        assertRequest(request2);

        // test params is null.
        params = null;
        HttpReportMarkDoneAction httpReportMarkDoneAction3 = createHttpReportMarkDoneAction();
        server.enqueueResponse(successResponse, 200);
        httpReportMarkDoneAction3.markDone(partition);
        RecordedRequest request3 = server.takeRequest(10, TimeUnit.SECONDS);
        assertRequest(request3);
    }

    @Test
    public void testHttpReportMarkDoneActionFailedResponse() throws Exception {
        HttpReportMarkDoneAction markDoneAction = createHttpReportMarkDoneAction();

        // status failed.
        server.enqueueResponse(failedResponse, 200);
        Assertions.assertThatThrownBy(() -> markDoneAction.markDone(partition))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "The http-report action's response attribute `result` should be 'SUCCESS' but is 'failed'.");

        // Illegal response body.
        String unExpectResponse = "{\"unknow\" :\"unknow\"}";
        server.enqueueResponse(unExpectResponse, 200);
        Assertions.assertThatThrownBy(() -> markDoneAction.markDone(partition))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "The http-report action's response attribute `result` should be 'SUCCESS' but is 'null'.");

        // empty response.
        server.enqueueResponse("", 200);
        Assertions.assertThatThrownBy(() -> markDoneAction.markDone(partition))
                .isInstanceOf(HttpReportMarkDoneException.class)
                .hasMessageContaining("ResponseBody is null or empty.");

        // 400.
        server.enqueueResponse(successResponse, 400);
        Assertions.assertThatThrownBy(() -> markDoneAction.markDone(partition))
                .isInstanceOf(HttpReportMarkDoneException.class)
                .hasMessageContaining("Response is not successful");
    }

    public static void assertRequest(RecordedRequest recordedRequest)
            throws JsonProcessingException {
        String requestBody = recordedRequest.getBody().readUtf8();
        HttpReportMarkDoneRequest request =
                server.readRequestBody(requestBody, HttpReportMarkDoneRequest.class);

        assertThat(
                        request.getPath().equals(fileStoreTable.location().toString())
                                && request.getPartition().equals(partition)
                                && request.getTable().equals(fileStoreTable.fullName())
                                && (params == null || params.equals(request.getParams())))
                .isTrue();
    }

    public static CoreOptions createCoreOptions() {
        HashMap<String, String> httpOptions = new HashMap<>();
        httpOptions.put(PARTITION_MARK_DONE_ACTION_URL.key(), server.getBaseUrl());
        httpOptions.put(PARTITION_MARK_DONE_ACTION_TIMEOUT.key(), "2 s");
        if (params != null) {
            httpOptions.put(PARTITION_MARK_DONE_ACTION_PARAMS.key(), params);
        }
        return new CoreOptions(httpOptions);
    }

    public HttpReportMarkDoneAction createHttpReportMarkDoneAction() {
        return new HttpReportMarkDoneAction(fileStoreTable, createCoreOptions());
    }

    public FileStoreTable createFileStoreTable() throws Exception {
        org.apache.paimon.fs.Path tablePath =
                new org.apache.paimon.fs.Path(folder.newFolder().toURI().toString());
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                FileIOFinder.find(tablePath),
                tablePath,
                tableSchema,
                options,
                CatalogEnvironment.empty());
    }
}
