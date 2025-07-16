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

package org.apache.paimon.flink.clone;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.source.Split;

import java.io.Serializable;

/** Clone split with necessary information. */
public class CloneSplitInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Identifier sourceIdentifier;
    private final Identifier targetidentifier;
    private final Split split;

    public CloneSplitInfo(Identifier sourceIdentifier, Identifier targetidentifier, Split split) {
        this.sourceIdentifier = sourceIdentifier;
        this.targetidentifier = targetidentifier;
        this.split = split;
    }

    public Identifier sourceIdentifier() {
        return sourceIdentifier;
    }

    public Identifier targetidentifier() {
        return targetidentifier;
    }

    public Split split() {
        return split;
    }
}
