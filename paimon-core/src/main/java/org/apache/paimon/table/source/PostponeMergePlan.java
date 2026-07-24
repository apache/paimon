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

package org.apache.paimon.table.source;

import org.apache.paimon.table.PostponeUtils.PostponeBucketRouter;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Serializable plan shared by execution-engine postpone merge adapters. */
public final class PostponeMergePlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<DataSplit> realSplits;
    private final List<PostponeFileReadTask> postponeFileTasks;
    private final PostponeBucketRouter bucketRouter;
    private final RowType keyType;
    private final RowType resultReadType;
    private final RowType mergeReadType;

    PostponeMergePlan(
            List<DataSplit> realSplits,
            List<PostponeFileReadTask> postponeFileTasks,
            PostponeBucketRouter bucketRouter,
            RowType keyType,
            RowType resultReadType,
            RowType mergeReadType) {
        this.realSplits = Collections.unmodifiableList(new ArrayList<>(realSplits));
        this.postponeFileTasks = Collections.unmodifiableList(new ArrayList<>(postponeFileTasks));
        this.bucketRouter = bucketRouter;
        this.keyType = keyType;
        this.resultReadType = resultReadType;
        this.mergeReadType = mergeReadType;
    }

    public List<DataSplit> realSplits() {
        return realSplits;
    }

    public List<PostponeFileReadTask> postponeFileTasks() {
        return postponeFileTasks;
    }

    public PostponeBucketRouter bucketRouter() {
        return bucketRouter;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType resultReadType() {
        return resultReadType;
    }

    public RowType mergeReadType() {
        return mergeReadType;
    }
}
