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

package org.apache.paimon.benchmark.metric;

import org.apache.paimon.benchmark.utils.BenchmarkUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A HTTP client to request BPS metric to JobMaster REST API. */
public class FlinkRestClient {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRestClient.class);

    private static final int CONNECT_TIMEOUT = 5000;
    private static final int SOCKET_TIMEOUT = 60000;
    private static final int CONNECTION_REQUEST_TIMEOUT = 10000;
    private static final int MAX_IDLE_TIME = 60000;
    private static final int MAX_CONN_TOTAL = 60;
    private static final int MAX_CONN_PER_ROUTE = 30;

    private final String jmEndpoint;
    private final CloseableHttpClient httpClient;

    public FlinkRestClient(String jmAddress, int jmPort) {
        this.jmEndpoint = jmAddress + ":" + jmPort;

        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setSocketTimeout(SOCKET_TIMEOUT)
                        .setConnectTimeout(CONNECT_TIMEOUT)
                        .setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
                        .build();
        PoolingHttpClientConnectionManager httpClientConnectionManager =
                new PoolingHttpClientConnectionManager();
        httpClientConnectionManager.setValidateAfterInactivity(MAX_IDLE_TIME);
        httpClientConnectionManager.setDefaultMaxPerRoute(MAX_CONN_PER_ROUTE);
        httpClientConnectionManager.setMaxTotal(MAX_CONN_TOTAL);

        this.httpClient =
                HttpClientBuilder.create()
                        .setConnectionManager(httpClientConnectionManager)
                        .setDefaultRequestConfig(requestConfig)
                        .build();
    }

    public void cancelJob(String jobId) {
        LOG.info("Cancelling Job: {}", jobId);
        String url = String.format("http://%s/jobs/%s?mode=cancel", jmEndpoint, jobId);
        patch(url);
    }

    public void stopJobWithSavepoint(String jobId, String savepointPath) {
        LOG.info("Stopping Job: {}", jobId);
        String url = String.format("http://%s/jobs/%s/stop", jmEndpoint, jobId);
        post(url, "{\"targetDirectory\": \"" + savepointPath + "\", \"drain\": false}");
    }

    public boolean isJobRunning(String jobId) {
        String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
        String response = executeAsString(url);
        try {
            JsonNode jsonNode = BenchmarkUtils.JSON_MAPPER.readTree(response);
            return jsonNode.get("state").asText().equals("RUNNING");
        } catch (Exception e) {
            throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
        }
    }

    public void waitUntilNumberOfRows(String jobId, long numberOfRows) throws InterruptedException {
        while (true) {
            String sourceVertexId = getSourceVertexId(jobId);
            double actualNumRecords = getTotalNumRecords(jobId, sourceVertexId);
            if (actualNumRecords >= numberOfRows) {
                return;
            }
            Thread.sleep(1000);
        }
    }

    public long waitUntilJobFinished(String jobId) throws InterruptedException {
        while (true) {
            String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
            String response = executeAsString(url);
            try {
                JsonNode jsonNode = BenchmarkUtils.JSON_MAPPER.readTree(response);
                boolean finished = jsonNode.get("state").asText().equals("FINISHED");
                if (finished) {
                    return jsonNode.get("duration").asLong();
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "The response is not a valid JSON string:\n" + response, e);
            }
            Thread.sleep(1000);
        }
    }

    public String getSourceVertexId(String jobId) {
        String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
        String response = executeAsString(url);
        try {
            JsonNode jsonNode = BenchmarkUtils.JSON_MAPPER.readTree(response);
            JsonNode vertices = jsonNode.get("vertices");
            JsonNode sourceVertex = vertices.get(0);
            checkArgument(
                    sourceVertex.get("name").asText().startsWith("Source:"),
                    "The first vertex is not a source.");
            return sourceVertex.get("id").asText();
        } catch (Exception e) {
            throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
        }
    }

    @Nullable
    public synchronized Long getDataFreshness(String jobId) {
        String url = String.format("http://%s/jobs/%s/checkpoints", jmEndpoint, jobId);
        String response = executeAsString(url);
        try {
            return System.currentTimeMillis()
                    - BenchmarkUtils.JSON_MAPPER
                            .readTree(response)
                            .get("latest")
                            .get("completed")
                            .get("trigger_timestamp")
                            .asLong();
        } catch (Exception e) {
            return null;
        }
    }

    public synchronized double getNumRecordsPerSecond(String jobId, String vertexId) {
        return getSourceMetric(jobId, vertexId, "numRecordsOutPerSecond");
    }

    public synchronized double getTotalNumRecords(String jobId, String vertexId) {
        return getSourceMetric(jobId, vertexId, "numRecordsOut");
    }

    public synchronized double getSourceMetric(String jobId, String vertexId, String metric) {
        String url =
                String.format(
                        "http://%s/jobs/%s/vertices/%s/subtasks/metrics",
                        jmEndpoint, jobId, vertexId);
        String response = executeAsString(url);
        String numRecordsMetricName = null;
        try {
            ArrayNode arrayNode = (ArrayNode) BenchmarkUtils.JSON_MAPPER.readTree(response);
            for (JsonNode node : arrayNode) {
                String metricName = node.get("id").asText();
                if (metricName.startsWith("Source_") && metricName.endsWith("." + metric)) {
                    numRecordsMetricName = metricName;
                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
        }

        if (numRecordsMetricName == null) {
            LOG.warn("Can't find " + metric + " name from the response: " + response);
            return -1;
        }

        url =
                String.format(
                        "http://%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s",
                        jmEndpoint, jobId, vertexId, numRecordsMetricName);
        response = executeAsString(url);
        try {
            return BenchmarkUtils.JSON_MAPPER.readTree(response).get(0).get("sum").doubleValue();
        } catch (Exception e) {
            throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
        }
    }

    private void post(String url, String json) {
        HttpPost httpPost = new HttpPost();
        httpPost.setURI(URI.create(url));
        try {
            httpPost.setEntity(new StringEntity(json));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        HttpResponse response;
        try {
            httpPost.setHeader("Connection", "close");
            response = httpClient.execute(httpPost);
            int httpCode = response.getStatusLine().getStatusCode();
            if (httpCode != HttpStatus.SC_ACCEPTED) {
                String msg = String.format("http execute failed,status code is %d", httpCode);
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            httpPost.abort();
            throw new RuntimeException(e);
        }
    }

    private void patch(String url) {
        HttpPatch httpPatch = new HttpPatch();
        httpPatch.setURI(URI.create(url));
        HttpResponse response;
        try {
            httpPatch.setHeader("Connection", "close");
            response = httpClient.execute(httpPatch);
            int httpCode = response.getStatusLine().getStatusCode();
            if (httpCode != HttpStatus.SC_ACCEPTED) {
                String msg = String.format("http execute failed,status code is %d", httpCode);
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            httpPatch.abort();
            throw new RuntimeException(e);
        }
    }

    private String executeAsString(String url) {
        HttpGet httpGet = new HttpGet();
        httpGet.setURI(URI.create(url));
        try {
            HttpEntity entity = execute(httpGet).getEntity();
            if (entity != null) {
                return EntityUtils.toString(entity, Consts.UTF_8);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to request URL " + url, e);
        }
        throw new RuntimeException(String.format("Response of URL %s is null.", url));
    }

    private HttpResponse execute(HttpRequestBase httpRequestBase) throws Exception {
        HttpResponse response;
        try {
            httpRequestBase.setHeader("Connection", "close");
            response = httpClient.execute(httpRequestBase);
            int httpCode = response.getStatusLine().getStatusCode();
            if (httpCode != HttpStatus.SC_OK) {
                String msg = String.format("http execute failed,status code is %d", httpCode);
                throw new RuntimeException(msg);
            }
            return response;
        } catch (Exception e) {
            httpRequestBase.abort();
            throw e;
        }
    }

    public synchronized void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
