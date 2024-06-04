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

package org.apache.paimon.manifest;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageLegacyV2Serializer;

import java.io.IOException;
import java.util.List;

/** A legacy version serializer for {@link ManifestCommittable}. */
public class ManifestCommittableLegacyV2Serializer extends ManifestCommittableSerializer {

    private final CommitMessageLegacyV2Serializer commitMessageLegacyV2Serializer;

    public ManifestCommittableLegacyV2Serializer() {
        this.commitMessageLegacyV2Serializer = new CommitMessageLegacyV2Serializer();
    }

    @Override
    protected List<CommitMessage> deserializeCommitMessage(int version, DataInputView view)
            throws IOException {
        return commitMessageLegacyV2Serializer.deserializeList(version, view);
    }
}
