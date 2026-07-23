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

package org.apache.paimon.jindo;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JindoFileIO}. */
public class JindoFileIOTest {

    @Test
    public void testCreateBlobPresignedUrlDelegates() throws Exception {
        TestingFileIO delegate = new TestingFileIO();
        JindoFileIO fileIO = new JindoFileIO(delegate);
        CatalogContext context = CatalogContext.create(new Options());
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 10, 20);
        Duration validity = Duration.ofHours(1);

        fileIO.configure(context);

        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, "jpg", validity))
                .isEqualTo("https://bucket.oss.example/blob.jpg");
        assertThat(delegate.context).isSameAs(context);
        assertThat(delegate.tableRoot).isEqualTo(tableRoot);
        assertThat(delegate.descriptor).isEqualTo(descriptor);
        assertThat(delegate.extension).isEqualTo("jpg");
        assertThat(delegate.validity).isEqualTo(validity);
    }

    private static class TestingFileIO extends LocalFileIO {

        private CatalogContext context;
        private Path tableRoot;
        private BlobDescriptor descriptor;
        private String extension;
        private Duration validity;

        @Override
        public void configure(CatalogContext context) {
            this.context = context;
        }

        @Override
        public String createBlobPresignedUrl(
                Path tableRoot, BlobDescriptor descriptor, String extension, Duration validity)
                throws IOException {
            this.tableRoot = tableRoot;
            this.descriptor = descriptor;
            this.extension = extension;
            this.validity = validity;
            return "https://bucket.oss.example/blob.jpg";
        }
    }
}
