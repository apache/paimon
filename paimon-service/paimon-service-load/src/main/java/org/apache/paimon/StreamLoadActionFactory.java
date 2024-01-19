/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package org.apache.paimon;
//
// import com.beust.jcommander.JCommander;
// import com.beust.jcommander.Parameter;
// import java.io.File;
// import java.net.InetSocketAddress;
// import lombok.AllArgsConstructor;
// import lombok.Builder;
// import lombok.Data;
// import lombok.NoArgsConstructor;
// import org.apache.commons.lang3.StringUtils;
// import org.apache.paimon.config.NettyServerConfig;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
/// ** fddfd. */
//
// @Data
// @Builder
// @NoArgsConstructor
// @AllArgsConstructor
// public class StreamLoadActionFactory {
//
//    public StreamLoadServer buildNettyRemotingServer(NettyServerConfig nettyServerConfig) {
//        return new StreamLoadServer(nettyServerConfig);
//    }
//
//    @Parameter(
//            names = {"-h", "--help"},
//            description = "print this message", help = true)
//    private boolean help;
//
//    @Parameter(
//            names = {"-c", "--config"},
//            description = "specify the config file position",
//            required = false)
//    @Builder.Default
//    private File configFile = new File("conf/server.config");
//
//    @Parameter(
//            names = {"-l", "--log"},
//            description = "specify the log file position",
//            required = false)
//    @Builder.Default
//    private File logFile = new File("log/zy-ftp.log");
//
//    @Parameter(
//            names = {"--local-ip"},
//            description = "specify the local ip",
//            required = false)
//    private String localIp;
//
//    @Parameter(
//            names = {"--local-port"},
//            description = "specify the local port",
//            required = false)
//    private Integer localPort;
//
//    @Parameter(
//            names = {"--passive-address"},
//            description = "specify the passive address",
//            required = false)
//    private String passiveAddress;
//
//    @Parameter(
//            names = {"--passive-ports"},
//            description = "specify the passive ports",
//            required = false)
//    private String passivePorts;
//
//    public static void main(String[] args) throws Exception {
//
//        StreamLoadActionFactory runner = new StreamLoadActionFactory();
//
//        JCommander commander = JCommander.newBuilder()
//                .addObject(runner)
//                .build();
//
//        commander.parse(args);
//
//        if (runner.help) {
//            commander.usage();
//            return;
//        }
//
//        runner.run();
//    }
//
//    public void run() throws Exception {
//
//        System.setProperty("zy-ftp.log", logFile.getCanonicalPath());
//
//        Logger logger = LoggerFactory.getLogger(this.getClass());
//
//        if (!configFile.isFile()) {
//            logger.error("configFile[{}] does not exist!", configFile.getCanonicalPath());
//            System.exit(1);
//        }
//
//        logger.info("use {} as the config file", configFile.getCanonicalPath());
//
//        logger.info("the log file position is {}", logFile.getCanonicalPath());
//
//        FtpServerContext context = new FtpServerContext(configFile);
//
//        InetSocketAddress address = context.getServerConfig().getLocalAddress();
//        if (StringUtils.isNotBlank(localIp) && !localIp.equals(address.getHostString())) {
//            address = new InetSocketAddress(localIp, address.getPort());
//            context.getServerConfig().setLocalAddress(address);
//        }
//        if (localPort != null && !localPort.equals(address.getPort())) {
//            address = new InetSocketAddress(address.getHostString(), localPort);
//            context.getServerConfig().setLocalAddress(address);
//        }
//        if (StringUtils.isNotBlank(passiveAddress)) {
//            context.getServerConfig().setPassiveAddress(passiveAddress);
//        }
//        if (StringUtils.isNotBlank(passivePorts)) {
//            context.getServerConfig().setPassivePortsString(passivePorts);
//            context.refresh();
//        }
//
//        FtpServer server = new FtpServer(context);
//
//        server.start();
//
//        logger.info("FtpServer started");
//
//        addShutdownHook(server);
//    }
//
//
//
//    protected static void addShutdownHook(StreamLoadServer server) {
//        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
//    }
// }
