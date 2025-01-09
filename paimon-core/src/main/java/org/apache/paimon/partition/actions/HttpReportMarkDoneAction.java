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

package org.apache.paimon.partition.actions;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.rest.DefaultErrorHandler;
import org.apache.paimon.rest.HttpClient;
import org.apache.paimon.rest.HttpClientOptions;
import org.apache.paimon.rest.RESTClient;
import org.apache.paimon.rest.RESTObjectMapper;
import org.apache.paimon.rest.RESTRequest;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION_URL;

/** Report partition submission information to remote http server. */
public class HttpReportMarkDoneAction implements PartitionMarkDoneAction {

    private final RESTClient client;

    private final FileStoreTable fileStoreTable;

    private final String params;

    private static final String RESPONSE_SUCCESS = "SUCCESS";

    public HttpReportMarkDoneAction(FileStoreTable fileStoreTable, CoreOptions options) {

        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(options.httpReportMarkDoneActionUrl()),
                String.format(
                        "Parameter %s must be non-empty when you use `http-report` partition mark done action.",
                        PARTITION_MARK_DONE_ACTION_URL.key()));

        this.fileStoreTable = fileStoreTable;
        this.params = options.httpReportMarkDoneActionParams();

        HttpClientOptions httpClientOptions =
                new HttpClientOptions(
                        options.httpReportMarkDoneActionUrl(),
                        Optional.of(options.httpReportMarkDoneActionTimeout()),
                        Optional.of(options.httpReportMarkDoneActionTimeout()),
                        RESTObjectMapper.create(),
                        1,
                        DefaultErrorHandler.getInstance());
        this.client = new HttpClient(httpClientOptions);
    }

    @Override
    public void markDone(String partition) throws Exception {
        HttpReportMarkDoneResponse response =
                client.post(
                        null,
                        new HttpReportMarkDoneRequest(
                                params,
                                fileStoreTable.fullName(),
                                fileStoreTable.location().toString(),
                                partition),
                        HttpReportMarkDoneResponse.class,
                        Collections.emptyMap());
        Preconditions.checkState(
                reportIsSuccess(response),
                String.format(
                        "The http-report action's response attribute `result` should be 'SUCCESS' but is '%s'.",
                        response.getResult()));
    }

    private boolean reportIsSuccess(HttpReportMarkDoneResponse response) {
        return response != null && RESPONSE_SUCCESS.equalsIgnoreCase(response.getResult());
    }

    @Override
    public void close() throws IOException {
        try {
            this.client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** RestRequest only for HttpReportMarkDoneAction. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class HttpReportMarkDoneRequest implements RESTRequest {

        private static final String MARK_DONE_PARTITION = "partition";
        private static final String TABLE = "table";
        private static final String PATH = "path";
        private static final String PARAMS = "params";

        @JsonProperty(MARK_DONE_PARTITION)
        private final String partition;

        @JsonProperty(TABLE)
        private final String table;

        @JsonProperty(PATH)
        private final String path;

        @JsonProperty(PARAMS)
        private final String params;

        @JsonCreator
        public HttpReportMarkDoneRequest(
                @JsonProperty(PARAMS) String params,
                @JsonProperty(TABLE) String table,
                @JsonProperty(PATH) String path,
                @JsonProperty(MARK_DONE_PARTITION) String partition) {
            this.params = params;
            this.table = table;
            this.path = path;
            this.partition = partition;
        }

        @JsonGetter(MARK_DONE_PARTITION)
        public String getPartition() {
            return partition;
        }

        @JsonGetter(TABLE)
        public String getTable() {
            return table;
        }

        @JsonGetter(PATH)
        public String getPath() {
            return path;
        }

        @JsonGetter(PARAMS)
        public String getParams() {
            return params;
        }
    }

    /** Response only for HttpReportMarkDoneAction. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class HttpReportMarkDoneResponse implements RESTResponse {
        private static final String RESULT = "result";

        @JsonProperty(RESULT)
        private final String result;

        public HttpReportMarkDoneResponse(@JsonProperty(RESULT) String result) {
            this.result = result;
        }

        @JsonGetter(RESULT)
        public String getResult() {
            return result;
        }
    }
}
