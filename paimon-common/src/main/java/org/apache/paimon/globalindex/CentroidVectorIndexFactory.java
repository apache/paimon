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

package org.apache.paimon.globalindex;

import org.apache.paimon.utils.Range;

import java.util.List;

/**
 * A centroid index groups all files for one field in a partition into a single generation. Its
 * reader accepts and returns absolute table row IDs. Committers and scan planners can inspect the
 * generation through this factory without opening files or initializing the native runtime.
 */
public interface CentroidVectorIndexFactory extends GlobalIndexerFactory {
    /** Exact indexed row ranges for a model entry, or an empty list for a shard entry. */
    List<Range> sourceRowRanges(byte[] indexMeta);

    /** Validate that these entries form one complete generation and return its source coverage. */
    List<Range> validateGeneration(List<byte[]> indexMetas);
}
