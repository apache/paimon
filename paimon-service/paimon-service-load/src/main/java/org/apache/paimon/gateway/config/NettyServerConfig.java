/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.gateway.config;

/** dfdf. */
public class NettyServerConfig {

    private String serverName;
    private int soBacklog = 1024;
    private boolean tcpNoDelay = true;
    private boolean soKeepalive = true;
    private int sendBufferSize = 65535;
    private int receiveBufferSize = 65535;
    private int workerThread = Runtime.getRuntime().availableProcessors() * 2;
    private int listenPort;

    // Constructor without parameters
    public NettyServerConfig() {}

    // Constructor with all parameters
    public NettyServerConfig(
            String serverName,
            int soBacklog,
            boolean tcpNoDelay,
            boolean soKeepalive,
            int sendBufferSize,
            int receiveBufferSize,
            int workerThread,
            int listenPort) {
        this.serverName = serverName;
        this.soBacklog = soBacklog;
        this.tcpNoDelay = tcpNoDelay;
        this.soKeepalive = soKeepalive;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        this.workerThread = workerThread;
        this.listenPort = listenPort;
    }

    // Getters and Setters
    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public int getSoBacklog() {
        return soBacklog;
    }

    public void setSoBacklog(int soBacklog) {
        this.soBacklog = soBacklog;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public boolean isSoKeepalive() {
        return soKeepalive;
    }

    public void setSoKeepalive(boolean soKeepalive) {
        this.soKeepalive = soKeepalive;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public int getWorkerThread() {
        return workerThread;
    }

    public void setWorkerThread(int workerThread) {
        this.workerThread = workerThread;
    }

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    /** Builder class. */
    public static class NettyServerConfigBuilder {
        private String serverName;
        private int soBacklog = 1024;
        private boolean tcpNoDelay = true;
        private boolean soKeepalive = true;
        private int sendBufferSize = 65535;
        private int receiveBufferSize = 65535;
        private int workerThread = Runtime.getRuntime().availableProcessors() * 2;
        private int listenPort;

        public NettyServerConfigBuilder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        public NettyServerConfigBuilder soBacklog(int soBacklog) {
            this.soBacklog = soBacklog;
            return this;
        }

        public NettyServerConfigBuilder tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public NettyServerConfigBuilder soKeepalive(boolean soKeepalive) {
            this.soKeepalive = soKeepalive;
            return this;
        }

        public NettyServerConfigBuilder sendBufferSize(int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        public NettyServerConfigBuilder receiveBufferSize(int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public NettyServerConfigBuilder workerThread(int workerThread) {
            this.workerThread = workerThread;
            return this;
        }

        public NettyServerConfigBuilder listenPort(int listenPort) {
            this.listenPort = listenPort;
            return this;
        }

        public NettyServerConfig build() {
            return new NettyServerConfig(
                    serverName,
                    soBacklog,
                    tcpNoDelay,
                    soKeepalive,
                    sendBufferSize,
                    receiveBufferSize,
                    workerThread,
                    listenPort);
        }
    }

    // Static method to create builder
    public static NettyServerConfigBuilder builder() {
        return new NettyServerConfigBuilder();
    }
}
