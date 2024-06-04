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

package org.apache.paimon.table.sink;

import org.apache.paimon.index.IndexFileMetaLegacyV2Serializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMetaLegacyV2Serializer;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.IndexIncrement;

import java.io.IOException;
import java.util.Collections;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** A legacy version serializer for {@link CommitMessage}. */
public class CommitMessageLegacyV2Serializer extends CommitMessageSerializer {

    private DataFileMetaLegacyV2Serializer dataFileSerializer;
    private IndexFileMetaLegacyV2Serializer indexEntrySerializer;

    @Override
    protected CommitMessage deserialize(int version, DataInputView view) throws IOException {
        if (version == 2) {
            if (dataFileSerializer == null) {
                dataFileSerializer = new DataFileMetaLegacyV2Serializer();
                indexEntrySerializer = new IndexFileMetaLegacyV2Serializer();
            }
            return new CommitMessageImpl(
                    deserializeBinaryRow(view),
                    view.readInt(),
                    new DataIncrement(
                            dataFileSerializer.deserializeList(view),
                            Collections.emptyList(),
                            dataFileSerializer.deserializeList(view)),
                    new CompactIncrement(
                            dataFileSerializer.deserializeList(view),
                            dataFileSerializer.deserializeList(view),
                            dataFileSerializer.deserializeList(view)),
                    new IndexIncrement(
                            indexEntrySerializer.deserializeList(view), Collections.emptyList()));
        }
        return super.deserialize(version, view);
    }
}
