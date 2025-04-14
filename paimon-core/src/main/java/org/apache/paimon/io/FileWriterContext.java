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

package org.apache.paimon.io;

import org.apache.paimon.format.FormatWriterFactory;

/** Context of file writer. */
public class FileWriterContext {

    private final FormatWriterFactory factory;
    private final SimpleStatsProducer statsProducer;
    private final String compression;

    public FileWriterContext(
            FormatWriterFactory factory, SimpleStatsProducer statsProducer, String compression) {
        this.factory = factory;
        this.statsProducer = statsProducer;
        this.compression = compression;
    }

    public FormatWriterFactory factory() {
        return factory;
    }

    public SimpleStatsProducer statsProducer() {
        return statsProducer;
    }

    public String compression() {
        return compression;
    }
}
