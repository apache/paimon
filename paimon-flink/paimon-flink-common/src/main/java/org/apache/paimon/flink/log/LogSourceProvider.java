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

package org.apache.paimon.flink.log;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

/**
 * A {@link Serializable} source provider for log store.
 *
 * <p>This class is serializable, it can be wrapped as the {@link HybridSource.SourceFactory}.
 */
public interface LogSourceProvider extends Serializable {

    /**
     * Creates a {@link Source} instance.
     *
     * @param bucketOffsets optional, configure if you need to specify the startup offset.
     */
    Source<RowData, ?, ?> createSource(@Nullable Map<Integer, Long> bucketOffsets);

    /** Pre-operations before create log {@link Source}. */
    void preCreateSource();
}
