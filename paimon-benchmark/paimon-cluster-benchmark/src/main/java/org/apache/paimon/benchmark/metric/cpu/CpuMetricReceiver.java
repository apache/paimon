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

package org.apache.paimon.benchmark.metric.cpu;

import org.apache.paimon.benchmark.BenchmarkOptions;
import org.apache.paimon.benchmark.utils.BenchmarkGlobalConfiguration;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CpuMetricReceiver implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CpuMetricReceiver.class);

    /** Server socket to listen at. */
    private final ServerSocket server;

    private final ConcurrentHashMap<String, Double> cpuMetrics = new ConcurrentHashMap<>();

    private final ExecutorService service = Executors.newCachedThreadPool();

    public CpuMetricReceiver(String host, int port) {
        try {
            InetAddress address = InetAddress.getByName(host);
            server = new ServerSocket(port, 10, address);
        } catch (IOException e) {
            throw new RuntimeException("Could not open socket to receive back cpu metrics.");
        }
    }

    public void runServer() {
        service.submit(this::runServerBlocking);
    }

    public void runServerBlocking() {
        try {
            //noinspection InfiniteLoopStatement
            while (true) {
                Socket socket = server.accept();
                service.submit(new ServerThread(socket, cpuMetrics));
            }
        } catch (IOException e) {
            LOG.error("Failed to start the socket server.", e);
            try {
                server.close();
            } catch (Throwable ignored) {
            }
        }
    }

    public double getTotalCpu() {
        double sumCpu = 0.0;
        int size = 0;
        for (Double cpu : cpuMetrics.values()) {
            size++;
            sumCpu += cpu;
        }
        if (size == 0) {
            LOG.warn("The cpu metric receiver doesn't receive any metrics.");
        }
        return sumCpu;
    }

    public int getNumberOfTM() {
        return cpuMetrics.size();
    }

    @Override
    public void close() {
        try {
            server.close();
        } catch (Throwable ignored) {
        }

        service.shutdownNow();
    }

    private static final class ServerThread implements Runnable {

        private final Socket socket;
        private final ConcurrentHashMap<String, Double> cpuMetrics;

        private ServerThread(Socket socket, ConcurrentHashMap<String, Double> cpuMetrics) {
            this.socket = socket;
            this.cpuMetrics = cpuMetrics;
        }

        @Override
        public void run() {
            try {
                InputStream inStream = socket.getInputStream();
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                int b;
                while ((b = inStream.read()) >= 0) {
                    // buffer until delimiter
                    if (b != CpuMetricSender.DELIMITER) {
                        buffer.write(b);
                    }
                    // decode and emit record
                    else {
                        byte[] bytes = buffer.toByteArray();
                        String message = new String(bytes, StandardCharsets.UTF_8);
                        LOG.info("Received CPU metric report: {}", message);
                        List<CpuMetric> receivedMetrics = CpuMetric.fromJsonArray(message);
                        for (CpuMetric metric : receivedMetrics) {
                            cpuMetrics.put(
                                    metric.getHost() + ":" + metric.getPid(), metric.getCpu());
                        }
                        buffer.reset();
                    }
                }
            } catch (IOException e) {
                LOG.error("Socket server error.", e);
            } finally {
                try {
                    socket.close();
                } catch (IOException ex) {
                    // ignore
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // start metric servers
        Configuration conf = BenchmarkGlobalConfiguration.loadConfiguration();
        String reporterAddress = conf.get(BenchmarkOptions.METRIC_REPORTER_HOST);
        int reporterPort = conf.get(BenchmarkOptions.METRIC_REPORTER_PORT);
        CpuMetricReceiver cpuMetricReceiver = new CpuMetricReceiver(reporterAddress, reporterPort);
        cpuMetricReceiver.runServer();
        cpuMetricReceiver.service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
