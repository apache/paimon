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

package org.apache.paimon.data;

import org.apache.paimon.utils.SensitiveConfigUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Emits access events for identified BLOB values. */
public final class BlobAccessLogger {

    private static final Logger LOG = LoggerFactory.getLogger(BlobAccessLogger.class);

    private BlobAccessLogger() {}

    public static void logDescriptor(BlobDescriptor descriptor) {
        log("DESCRIPTOR_ACCESS", descriptor.uri(), descriptor.offset(), descriptor.length());
    }

    public static void logContent(String path, long offset, long length) {
        log("CONTENT_ACCESS", path, offset, length);
    }

    private static void log(String event, String path, long offset, long length) {
        if (LOG.isInfoEnabled()) {
            String safePath =
                    path.indexOf('?') >= 0 || path.indexOf('@') >= 0
                            ? SensitiveConfigUtils.sanitizeUri(path)
                            : path;
            LOG.info(
                    "event={} path={} offset={} length={}",
                    event,
                    safePath.replace("\r", "%0D").replace("\n", "%0A"),
                    offset,
                    length);
        }
    }
}
