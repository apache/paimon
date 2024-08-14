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

package org.apache.paimon.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;

/* This file is based on source code from the Iceberg Project (http://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A metastore to run locally.
 *
 * <p>Mostly copied from iceberg.
 */
public class TestHiveMetastore {

    private static final int DEFAULT_POOL_SIZE = 15;

    // It's tricky to clear all static fields in an HMS instance in order to switch derby root dir.
    // Therefore, we reuse the same derby root between tests and remove it after JVM exits.
    private static File hiveLocalDir;
    private static String derbyPath;

    static {
        setup();
    }

    private static void setup() {
        try {
            hiveLocalDir =
                    createTempDirectory("hive", asFileAttribute(fromString("rwxrwxrwx"))).toFile();
            derbyPath = new File(hiveLocalDir, "metastore_db").getPath();
            File derbyLogFile = new File(hiveLocalDir, "derby.log");
            System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
            setupMetastoreDB("jdbc:derby:" + derbyPath + ";create=true");
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> {
                                        Path localDirPath =
                                                new Path(hiveLocalDir.getAbsolutePath());
                                        FileSystem fs;
                                        try {
                                            fs =
                                                    FileSystem.get(
                                                            localDirPath.toUri(),
                                                            new Configuration());
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                        String errMsg = "Failed to delete " + localDirPath;
                                        try {
                                            if (!fs.delete(localDirPath, true)) {
                                                Assert.assertFalse(errMsg, fs.exists(localDirPath));
                                            }
                                        } catch (IOException e) {
                                            throw new RuntimeException(errMsg, e);
                                        }
                                    }));
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup local dir for hive metastore", e);
        }
    }

    private HiveConf hiveConf;
    private ExecutorService executorService;
    private TServer server;
    private HiveMetaStore.HMSHandler baseHandler;

    /**
     * Starts a TestHiveMetastore with the default connection pool size (5) and the default
     * HiveConf.
     */
    public void start(int port) {
        start(new HiveConf(new Configuration(), TestHiveMetastore.class), DEFAULT_POOL_SIZE, port);
    }

    /**
     * Starts a TestHiveMetastore with the default connection pool size (5) and the default
     * HiveConf.
     */
    public void start() {
        start(new HiveConf(new Configuration(), TestHiveMetastore.class), DEFAULT_POOL_SIZE, 9083);
    }

    /**
     * Starts a TestHiveMetastore with a provided connection pool size and HiveConf.
     *
     * @param conf The hive configuration to use
     * @param poolSize The number of threads in the executor pool
     */
    public void start(HiveConf conf, int poolSize, int portNum) {
        try {
            TServerSocket socket = new TServerSocket(portNum);
            int port = socket.getServerSocket().getLocalPort();
            initConf(conf, port);

            this.hiveConf = conf;
            this.server = newThriftServer(socket, poolSize, hiveConf);
            this.executorService = Executors.newSingleThreadExecutor();
            this.executorService.submit(() -> server.serve());

            // in Hive3, setting this as a system prop ensures that it will be picked up whenever a
            // new
            // HiveConf is created
            System.setProperty(
                    HiveConf.ConfVars.METASTOREURIS.varname,
                    hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
            System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseDir());
        } catch (Exception e) {
            throw new RuntimeException("Cannot start TestHiveMetastore", e);
        }
    }

    public void stop() throws Exception {
        reset();
        if (server != null) {
            server.stop();
        }
        if (executorService != null) {
            executorService.shutdown();
        }
        if (baseHandler != null) {
            baseHandler.shutdown();
        }
        System.clearProperty(HiveConf.ConfVars.METASTOREURIS.varname);
        System.clearProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    }

    public void reset() throws Exception {
        setup();
        Path warehouseRoot = new Path(hiveLocalDir.getAbsolutePath());
        FileSystem fs = FileSystem.get(warehouseRoot.toUri(), hiveConf);
        for (FileStatus fileStatus : fs.listStatus(warehouseRoot)) {
            if (!fileStatus.getPath().getName().equals("derby.log")
                    && !fileStatus.getPath().getName().equals("metastore_db")) {
                fs.delete(fileStatus.getPath(), true);
            }
        }
    }

    private static String warehouseDir() {
        return "file:" + hiveLocalDir.getAbsolutePath();
    }

    private TServer newThriftServer(TServerSocket socket, int poolSize, HiveConf conf)
            throws Exception {
        HiveConf serverConf = new HiveConf(conf);
        serverConf.set(
                HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
                "jdbc:derby:" + derbyPath + ";create=true");
        baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", serverConf);
        IHMSHandler handler = RetryingHMSHandler.getProxy(serverConf, baseHandler, false);

        TThreadPoolServer.Args args =
                new TThreadPoolServer.Args(socket)
                        .processor(new TSetIpAddressProcessor<>(handler))
                        .transportFactory(new TTransportFactory())
                        .protocolFactory(new TBinaryProtocol.Factory())
                        .minWorkerThreads(poolSize)
                        .maxWorkerThreads(poolSize);

        return new TThreadPoolServer(args);
    }

    private void initConf(HiveConf conf, int port) {
        conf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
        conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseDir());
        conf.set(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname, "false");
        conf.set(
                HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname,
                "false");
        conf.set(
                HiveConf.ConfVars.HIVE_IN_TEST.varname,
                HiveConf.ConfVars.HIVE_IN_TEST.getDefaultValue());
    }

    private static void setupMetastoreDB(String dbURL) throws SQLException, IOException {
        Connection connection = DriverManager.getConnection(dbURL);
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("hive-schema-3.1.0.derby.sql");
        try (Reader reader = new InputStreamReader(inputStream)) {
            runScript(connection, reader);
        }
    }

    // This method is copied from iceberg `ScriptRunner`
    private static void runScript(Connection conn, Reader reader) throws SQLException, IOException {
        StringBuilder command = null;
        try {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuilder();
                }
                String trimmedLine = line.trim();
                if (trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (trimmedLine.length() < 1 || trimmedLine.startsWith("//")) {
                    // Do nothing
                } else if (trimmedLine.length() < 1 || trimmedLine.startsWith("--")) {
                    // Do nothing
                } else if (trimmedLine.endsWith(";")) {
                    command.append(line.substring(0, line.lastIndexOf(";")));
                    command.append(" ");
                    Statement statement = conn.createStatement();

                    statement.execute(command.toString());

                    if (!conn.getAutoCommit()) {
                        conn.commit();
                    }
                    command = null;
                    try {
                        statement.close();
                    } catch (Exception e) {
                        // Ignore to workaround a bug in Jakarta DBCP
                    }
                    Thread.yield();
                } else {
                    command.append(line);
                    command.append(" ");
                }
            }
        } catch (IOException | SQLException e) {
            e.fillInStackTrace();
            throw e;
        } finally {
            conn.rollback();
        }
    }
}
