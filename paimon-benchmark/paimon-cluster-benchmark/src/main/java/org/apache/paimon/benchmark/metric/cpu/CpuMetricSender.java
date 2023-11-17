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
import org.apache.paimon.benchmark.utils.AutoClosableProcess;
import org.apache.paimon.benchmark.utils.BenchmarkGlobalConfiguration;
import org.apache.paimon.benchmark.utils.BenchmarkUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CpuMetricSender implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CpuMetricSender.class);
    public static final char DELIMITER = '\n';

    private final String serverHostIp;
    private final int serverPort;
    private final Duration interval;
    private final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
    private InetAddress serverAddress;
    private String localHostIp;
    private ConcurrentHashMap<Integer, ProcfsBasedProcessTree> processTrees;
    private Socket socket;
    private OutputStream outputStream;

    public CpuMetricSender(String serverHostIp, int serverPort, Duration interval) {
        this.serverHostIp = serverHostIp;
        this.serverPort = serverPort;
        this.interval = interval;
    }

    public void startClient() throws Exception {
        List<Integer> taskmanagers = getTaskManagerPidList();
        if (taskmanagers.isEmpty()) {
            throw new RuntimeException("There is no Flink TaskManager is running.");
        }
        this.serverAddress = InetAddress.getByName(serverHostIp);
        this.processTrees = new ConcurrentHashMap<>();
        for (Integer pid : taskmanagers) {
            processTrees.put(pid, new ProcfsBasedProcessTree(String.valueOf(pid)));
        }
        LOG.info("Start to monitor process: {}", taskmanagers);
        this.service.scheduleAtFixedRate(
                this::reportCpuMetric, 0L, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void close() throws Exception {
        try {
            if (outputStream != null) {
                outputStream.flush();
                outputStream.close();
            }

            // first regular attempt to cleanly close. Failing that will escalate
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {
            throw new IOException(
                    "Error while closing connection that streams data back to client at "
                            + serverAddress.toString()
                            + ":"
                            + serverPort,
                    e);
        } finally {
            // if we failed prior to closing the client, close it
            if (socket != null) {
                try {
                    socket.close();
                } catch (Throwable t) {
                    // best effort to close, we do not care about an exception here any more
                }
            }
        }
    }

    private void reportCpuMetric() {
        try {
            socket = new Socket(serverAddress, serverPort);
            outputStream = socket.getOutputStream();
            InetAddress localAddress =
                    ConnectionUtils.findConnectingAddress(
                            new InetSocketAddress(serverAddress, serverPort), 10000L, 400);
            this.localHostIp = localAddress.getHostAddress();
        } catch (IOException e) {
            LOG.warn("Can't connect to metric server. Skip to report metric for this round.", e);
            return;
        }

        try {
            List<CpuMetric> cpuMetrics = getCpuMetrics();
            String jsonMessage = BenchmarkUtils.JSON_MAPPER.writeValueAsString(cpuMetrics);
            this.outputStream.write(jsonMessage.getBytes(StandardCharsets.UTF_8));
            this.outputStream.write(DELIMITER);
            LOG.info("Report CPU metric: {}", jsonMessage);
        } catch (Exception e) {
            LOG.error("Report CPU metric error.", e);
        }

        // close
        if (socket != null) {
            try {
                socket.close();
            } catch (Throwable t) {
                // ignore
            }
        }
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private List<CpuMetric> getCpuMetrics() {
        List<CpuMetric> cpuMetrics = new ArrayList<>();
        for (Map.Entry<Integer, ProcfsBasedProcessTree> entry : processTrees.entrySet()) {
            ProcfsBasedProcessTree processTree = entry.getValue();
            processTree.updateProcessTree();
            int pid = entry.getKey();
            double cpuCores = processTree.getCpuUsagePercent() / 100.0;
            cpuMetrics.add(new CpuMetric(localHostIp, pid, cpuCores));
        }
        return cpuMetrics;
    }

    public static List<Integer> getTaskManagerPidList() throws IOException {
        List<String> javaProcessors = new ArrayList<>();
        AutoClosableProcess.create("jps").setStdoutProcessor(javaProcessors::add).runBlocking();
        List<Integer> taskManagers = new ArrayList<>();
        for (String processor : javaProcessors) {
            if (processor.endsWith("TaskManagerRunner")) {
                String pid = processor.split(" ")[0];
                taskManagers.add(Integer.parseInt(pid));
            }
        }
        return taskManagers;
    }

    public static void main(String[] args) throws Exception {
        // start metric servers
        Configuration conf = BenchmarkGlobalConfiguration.loadConfiguration();
        String reporterAddress = conf.get(BenchmarkOptions.METRIC_REPORTER_HOST);
        int reporterPort = conf.get(BenchmarkOptions.METRIC_REPORTER_PORT);
        Duration reportInterval = conf.get(BenchmarkOptions.METRIC_MONITOR_INTERVAL);

        CpuMetricSender sender = new CpuMetricSender(reporterAddress, reporterPort, reportInterval);
        sender.startClient();
        sender.service.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
}
