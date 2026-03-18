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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Set;

import static org.apache.paimon.types.DataTypeRoot.BLOB;

/** Context for blob file. */
public class BlobFileContext {

    private final Set<String> blobDescriptorFields;
    private final Set<String> blobExternalStorageFields;
    @Nullable private final String blobExternalStoragePath;

    private @Nullable BlobConsumer blobConsumer;

    private BlobFileContext(
            Set<String> blobDescriptorFields,
            Set<String> blobExternalStorageFields,
            @Nullable String blobExternalStoragePath) {
        this.blobDescriptorFields = blobDescriptorFields;
        this.blobExternalStorageFields = blobExternalStorageFields;
        this.blobExternalStoragePath = blobExternalStoragePath;
    }

    @Nullable
    public static BlobFileContext create(RowType rowType, CoreOptions options) {
        if (rowType.getFieldTypes().stream().noneMatch(t -> t.is(BLOB))) {
            return null;
        }
        Set<String> descriptorFields = options.blobDescriptorField();
        Set<String> externalStorageField = options.blobExternalStorageField();
        String externalStoragePath = options.blobExternalStoragePath();
        boolean requireBlobFile = false;
        for (DataField field : rowType.getFields()) {
            DataTypeRoot type = field.type().getTypeRoot();
            if (type == DataTypeRoot.BLOB
                    && (!descriptorFields.contains(field.name())
                            || externalStorageField.contains(field.name()))) {
                requireBlobFile = true;
                break;
            }
        }
        if (!requireBlobFile) {
            return null;
        }
        return new BlobFileContext(descriptorFields, externalStorageField, externalStoragePath);
    }

    public BlobFileContext withBlobConsumer(BlobConsumer blobConsumer) {
        this.blobConsumer = blobConsumer;
        return this;
    }

    public BlobFileContext withWriteType(RowType writeType) {
        if (writeType.getFieldTypes().stream().noneMatch(t -> t.is(BLOB))) {
            return null;
        }
        return this;
    }

    public Set<String> blobDescriptorFields() {
        return blobDescriptorFields;
    }

    public Set<String> blobExternalStorageFields() {
        return blobExternalStorageFields;
    }

    @Nullable
    public String blobExternalStoragePath() {
        return blobExternalStoragePath;
    }

    @Nullable
    public BlobConsumer blobConsumer() {
        return blobConsumer;
    }
}
