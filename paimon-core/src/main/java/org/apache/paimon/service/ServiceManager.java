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

package org.apache.paimon.service;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Optional;

/** A manager to manage services, for example, the service to lookup row from the primary key. */
public class ServiceManager implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SERVICE_PREFIX = "service-";

    public static final String PRIMARY_KEY_LOOKUP = "primary-key-lookup";

    private final FileIO fileIO;
    private final Path tablePath;

    public ServiceManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public Path tablePath() {
        return tablePath;
    }

    public Optional<InetSocketAddress[]> service(String id) {
        try {
            return fileIO.readOverwrittenFileUtf8(servicePath(id))
                    .map(s -> JsonSerdeUtil.fromJson(s, InetSocketAddress[].class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void resetService(String id, InetSocketAddress[] addresses) {
        try {
            fileIO.overwriteFileUtf8(servicePath(id), JsonSerdeUtil.toJson(addresses));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteService(String id) {
        fileIO.deleteQuietly(servicePath(id));
    }

    private Path servicePath(String id) {
        return new Path(tablePath + "/service/" + SERVICE_PREFIX + id);
    }
}
