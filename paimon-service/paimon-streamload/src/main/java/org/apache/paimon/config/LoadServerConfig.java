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

package org.apache.paimon.config;

import java.io.File;
import java.net.InetSocketAddress;

/** asas. */
public class LoadServerConfig {
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    private static final int DEFAULT_MAX_LOGINS = 100;

    private static final int DEFAULT_MAX_IDLE_SECONDS = 120;

    private static final int DEFAULT_DATA_CONNECTION_MAX_IDLE_SECONDS = 30;

    private static final String DEFAULT_LOCAL_IP = "0.0.0.0";

    private static final int DEFAULT_LOCAL_PORT = 21;

    private static final String DEFAULT_PASSIVE_ADDRESS = "127.0.0.1";

    private static final String DEFAULT_PASSIVE_PORTS = "40000-50000";

    // 在实际协议中，每个session只能开启一个上传下载通道
    // 客户端是通过同时登录多次，实现的多个文件并行下载
    // 即实际通过一个账户的最大登录数限制并行下载数即可
    private static final int DEFAULT_MAX_UPLOAD_CONNECTIONS_PER_SESSION = 10;

    private static final int DEFAULT_MAX_DOWNLOAD_CONNECTIONS_PER_SESSION = 10;

    private static final long DEFAULT_UPLOAD_BYTES_PER_SECOND = 500 * 1024L;

    private static final long DEFAULT_DOWNLOAD_BYTES_PER_SECOND = 500 * 1024L;

    private static final String DEFAULT_HOME_DIRECTORY = "res/";

    private InetSocketAddress localAddress;

    private int maxLogins;

    private int maxIdleSeconds;

    private int dataConnectionMaxIdleSeconds;

    private String passiveAddress;

    private String passivePortsString;

    private int defaultMaxUploadConnectionsPerSession;

    private int defaultMaxDownloadConnectionsPerSession;

    private long defaultUploadBytesPerSecond;

    private long defaultDownloadBytesPerSecond;

    private String defaultHomeDirectory;

    private LoadServerConfig() {}

    public static LoadServerConfig loadConfig(File configFile) {

        LoadServerConfig config = new LoadServerConfig();
        return config;
    }
}
