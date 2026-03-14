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

package org.apache.paimon.fs.hadoop;

import org.apache.paimon.options.Options;
import org.apache.paimon.security.HadoopModule;
import org.apache.paimon.security.SecurityConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

/** {@link FileSystem} with a hadoop login user installed through {@link HadoopModule}. */
public class HadoopSecuredFileSystem extends FileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopSecuredFileSystem.class);

    private final FileSystem fileSystem;
    private final UserGroupInformation ugi;

    private HadoopSecuredFileSystem(FileSystem fileSystem, UserGroupInformation ugi) {
        this.fileSystem = fileSystem;
        this.ugi = ugi;
    }

    @Override
    public Configuration getConf() {
        return fileSystem.getConf();
    }

    @Override
    public URI getUri() {
        return runSecured(fileSystem::getUri);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.open(path, bufferSize));
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission fsPermission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progressable)
            throws IOException {
        return runSecuredWithIOException(
                () ->
                        fileSystem.create(
                                path,
                                fsPermission,
                                overwrite,
                                bufferSize,
                                replication,
                                blockSize,
                                progressable));
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.create(path, overwrite));
    }

    @Override
    public FSDataOutputStream create(
            Path path, boolean overwrite, int bufferSize, short replication, long blockSize)
            throws IOException {
        return runSecuredWithIOException(
                () -> fileSystem.create(path, overwrite, bufferSize, replication, blockSize));
    }

    @Override
    public short getDefaultReplication(Path f) {
        return runSecured(() -> fileSystem.getDefaultReplication(f));
    }

    @Deprecated
    @Override
    public short getDefaultReplication() {
        return runSecured(() -> fileSystem.getDefaultReplication());
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return runSecured(() -> fileSystem.getDefaultBlockSize(f));
    }

    @Deprecated
    @Override
    public long getDefaultBlockSize() {
        return runSecured(() -> fileSystem.getDefaultBlockSize());
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.exists(f));
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable)
            throws IOException {
        return runSecuredWithIOException(() -> fileSystem.append(path, bufferSize, progressable));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.rename(src, dst));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.delete(path, recursive));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.listStatus(path));
    }

    @Override
    public void setWorkingDirectory(Path path) {
        runSecured(() -> fileSystem.setWorkingDirectory(path));
    }

    @Override
    public Path getWorkingDirectory() {
        return runSecured(fileSystem::getWorkingDirectory);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.mkdirs(path, fsPermission));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return runSecuredWithIOException(() -> fileSystem.getFileStatus(path));
    }

    private void runSecured(final Runnable securedRunnable) {
        runSecured(
                () -> {
                    securedRunnable.run();
                    return null;
                });
    }

    private <T> T runSecured(final Callable<T> securedCallable) {
        try {
            return ugi.doAs((PrivilegedExceptionAction<T>) securedCallable::call);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T runSecuredWithIOException(final Callable<T> securedCallable) throws IOException {
        try {
            return ugi.doAs((PrivilegedExceptionAction<T>) securedCallable::call);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static FileSystem trySecureFileSystem(
            FileSystem fileSystem, Options options, Configuration configuration)
            throws IOException {
        SecurityConfiguration config = new SecurityConfiguration(options);
        if (config.isLegal()) {
            LOG.info("Hadoop security configuration is legal, use the secured FileSystem.");
            HadoopModule module = new HadoopModule(config, configuration);
            module.install();
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            return new HadoopSecuredFileSystem(fileSystem, ugi);
        } else {
            LOG.info("Hadoop security configuration is illegal, use the original FileSystem.");
            return fileSystem;
        }
    }
}
