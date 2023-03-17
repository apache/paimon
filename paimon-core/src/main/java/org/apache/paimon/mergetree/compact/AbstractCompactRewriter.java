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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Common implementation of {@link CompactRewriter}. */
public abstract class AbstractCompactRewriter implements CompactRewriter {

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        return new CompactResult(file, file.upgrade(outputLevel));
    }

    protected static List<DataFileMeta> extractFilesFromSections(List<List<SortedRun>> sections) {
        return sections.stream()
                .flatMap(Collection::stream)
                .map(SortedRun::files)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {}
}
