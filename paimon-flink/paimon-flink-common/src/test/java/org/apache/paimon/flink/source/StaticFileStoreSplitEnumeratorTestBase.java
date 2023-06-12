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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.FlinkConnectorOptions;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;

import java.util.List;

import static org.apache.paimon.flink.source.StaticFileStoreSource.createSplitAssigner;

/** Base test class of {@link StaticFileStoreSplitEnumerator}. */
public abstract class StaticFileStoreSplitEnumeratorTestBase {

    protected TestingSplitEnumeratorContext<FileStoreSourceSplit> getSplitEnumeratorContext(
            int parallelism) {
        final TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            context.registerReader(i, "test-host");
        }
        return context;
    }

    protected StaticFileStoreSplitEnumerator getSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            List<FileStoreSourceSplit> splits) {
        return new StaticFileStoreSplitEnumerator(
                context, null, createSplitAssigner(context, 10, splitAssignMode(), splits));
    }

    protected abstract FlinkConnectorOptions.SplitAssignMode splitAssignMode();
}
