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

package org.apache.paimon.elasticsearch.index.util;

import org.elasticsearch.vectorindex.impl.DiskBBQCodec;

/**
 * No-arg constructor wrapper for {@link DiskBBQCodec} so that Lucene's SPI ({@code NamedSPILoader})
 * can instantiate it when reading an index. The constructor parameters only affect writing; the
 * reader reads format details from the index itself.
 */
public class DiskBBQCodecProvider extends DiskBBQCodec {
    public DiskBBQCodecProvider() {
        super(1000, 8);
    }
}
