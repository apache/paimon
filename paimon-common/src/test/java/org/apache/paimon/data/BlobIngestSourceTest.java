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

package org.apache.paimon.data;

import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BlobIngestSourceTest {

    @Test
    void classifiesStandardReaderApisWithoutOpeningAStream() {
        assertThat(BlobIngestSource.classify(Blob.fromData(new byte[0])))
                .isEqualTo(BlobIngestSource.INLINE);
        assertThat(BlobIngestSource.classify(Blob.fromHttp("https://example.org/image")))
                .isEqualTo(BlobIngestSource.HTTP);
        assertThat(BlobIngestSource.classify(Blob.fromLocal("/not-opened/image")))
                .isEqualTo(BlobIngestSource.FILE);
    }

    @Test
    void doesNotInferSourceKindFromAUriOrInspectCredentials() {
        Blob custom =
                new BlobRef(
                        uri -> {
                            throw new AssertionError("classification must not open a stream");
                        },
                        new BlobDescriptor("https://example.org/image?signature=test", 0, -1));
        assertThat(BlobIngestSource.classify(custom)).isEqualTo(BlobIngestSource.CUSTOM);
        assertThat(
                        BlobIngestSource.classify(
                                new BlobRef(
                                        UriReader.fromHttp(),
                                        new BlobDescriptor("not a valid URI", 0, -1))))
                .isEqualTo(BlobIngestSource.HTTP);
        assertThat(BlobIngestSource.classify(null)).isEqualTo(BlobIngestSource.UNKNOWN);
        assertThat(BlobIngestSource.classify(new BlobRef(null, null)))
                .isEqualTo(BlobIngestSource.UNKNOWN);
    }

    @Test
    void doesNotInspectDescriptorsOfTransformingSubclasses() {
        BlobRef transformed =
                new BlobRef(null, null) {
                    @Override
                    public BlobDescriptor toDescriptor() {
                        throw new UnsupportedOperationException("descriptor is not available");
                    }
                };
        assertThat(BlobIngestSource.classify(transformed)).isEqualTo(BlobIngestSource.UNKNOWN);
        assertThat(BlobIngestSource.classify(new BlobData(new byte[0]) {}))
                .isEqualTo(BlobIngestSource.UNKNOWN);
    }
}
