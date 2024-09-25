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

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A class to discover {@link FileFormat}. */
@ThreadSafe
public interface FileFormatDiscover {

    static FileFormatDiscover of(CoreOptions options) {
        Map<String, FileFormat> formats = new ConcurrentHashMap<>();
        return new FileFormatDiscover() {

            @Override
            public FileFormat discover(String identifier) {
                return formats.computeIfAbsent(identifier, this::create);
            }

            private FileFormat create(String identifier) {
                return FileFormat.fromIdentifier(identifier, options.toConfiguration());
            }
        };
    }

    FileFormat discover(String identifier);
}
