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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.BlobReference;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;

/** Type serializer for {@code BLOB_REF}. */
public class BlobRefSerializer extends SerializerSingleton<BlobRef> {

    private static final long serialVersionUID = 1L;

    public static final BlobRefSerializer INSTANCE = new BlobRefSerializer();

    @Override
    public BlobRef copy(BlobRef from) {
        return from;
    }

    @Override
    public void serialize(BlobRef blobRef, DataOutputView target) throws IOException {
        BinarySerializer.INSTANCE.serialize(blobRef.reference().serialize(), target);
    }

    @Override
    public BlobRef deserialize(DataInputView source) throws IOException {
        byte[] bytes = BinarySerializer.INSTANCE.deserialize(source);
        return new BlobRef(BlobReference.deserialize(bytes));
    }
}
