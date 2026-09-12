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

package org.apache.paimon.s3;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RenamingTwoPhaseOutputStream;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that a two-phase write routed through {@link ResolvingFileIO} uses S3's native
 * multipart-upload commit end to end against a MinIO backend: the stream is the resolved override
 * (not the rename fallback), its committer survives serialization, and
 * commit/discard/discardStaging work when handed a fresh resolver that has to resolve the scheme
 * before casting to {@link S3FileIO}.
 */
class S3ResolvingTwoPhaseCommitITCase {

    @RegisterExtension private static final MinioTestContainer MINIO = new MinioTestContainer();

    // preferIO resolves the s3 scheme to a native S3FileIO without any ServiceLoader registration.
    private static final FileIOLoader S3_LOADER =
            new FileIOLoader() {
                @Override
                public String getScheme() {
                    return "s3";
                }

                @Override
                public FileIO load(Path path) {
                    return new S3FileIO();
                }
            };

    private ResolvingFileIO newResolver() {
        ResolvingFileIO resolver = new ResolvingFileIO();
        resolver.configure(
                CatalogContext.create(
                        Options.fromMap(MINIO.getS3ConfigOptions()),
                        new Configuration(),
                        S3_LOADER,
                        null));
        return resolver;
    }

    private Path target(String name) {
        return new Path(MINIO.getS3UriForDefaultBucket() + "/two-phase/" + name);
    }

    @Test
    void nativeMultipartCommitThroughFreshResolver() throws Exception {
        Path path = target(UUID.randomUUID() + ".data");
        String payload = "native-multipart-payload";

        TwoPhaseOutputStream out = newResolver().newTwoPhaseOutputStream(path, true);
        // The resolver must forward to S3's native stream, not fall back to a copy-and-rename one.
        assertThat(out).isNotInstanceOf(RenamingTwoPhaseOutputStream.class);
        out.write(payload.getBytes(StandardCharsets.UTF_8));
        TwoPhaseOutputStream.Committer committer = out.closeForCommit();

        // The committer is handed across the commit boundary, so it has to serialize.
        byte[] bytes = InstantiationUtil.serializeObject(committer);
        TwoPhaseOutputStream.Committer restored =
                InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

        // Not visible before commit; committing through a fresh resolver forces the resolve that
        // precedes the (S3FileIO) cast in BaseMultiPartUploadCommitter.
        assertThat(newResolver().exists(path)).isFalse();
        restored.commit(newResolver());

        FileIO reader = newResolver();
        assertThat(reader.exists(path)).isTrue();
        assertThat(reader.readFileUtf8(path)).isEqualTo(payload);
        reader.delete(path, false);
    }

    @Test
    void abortBeforeCompletionLeavesNoObject() throws Exception {
        Path path = target(UUID.randomUUID() + ".data");

        TwoPhaseOutputStream out = newResolver().newTwoPhaseOutputStream(path, true);
        out.write("to-be-aborted".getBytes(StandardCharsets.UTF_8));
        TwoPhaseOutputStream.Committer committer = out.closeForCommit();

        committer.discard(newResolver());
        assertThat(newResolver().exists(path)).isFalse();
    }

    @Test
    void discardStagingAfterCommitPreservesObject() throws Exception {
        Path path = target(UUID.randomUUID() + ".data");

        TwoPhaseOutputStream out = newResolver().newTwoPhaseOutputStream(path, true);
        out.write("committed".getBytes(StandardCharsets.UTF_8));
        TwoPhaseOutputStream.Committer committer = out.closeForCommit();

        committer.commit(newResolver());
        assertThat(newResolver().exists(path)).isTrue();

        // Aborting staged resources after a successful commit must never delete the object.
        committer.discardStaging(newResolver());
        assertThat(newResolver().exists(path)).isTrue();
        newResolver().delete(path, false);
    }
}
