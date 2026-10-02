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
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that the {@code s3a} scheme is served by the S3 plugin. */
public class S3ASchemeTest {

    @RegisterExtension public static final MinioTestContainer MINIO = new MinioTestContainer();

    @Test
    public void testS3aSchemeIsRegistered() {
        Map<String, FileIOLoader> loaders = FileIO.discoverLoaders();
        assertTrue(loaders.containsKey("s3"), "s3 loader missing");
        assertTrue(loaders.containsKey("s3a"), "s3a loader missing");
        assertEquals(S3Loader.class, loaders.get("s3").getClass());
        assertEquals(S3ALoader.class, loaders.get("s3a").getClass());
    }

    @Test
    public void testReadWriteOverS3aScheme() throws Exception {
        String bucket = MINIO.getS3UriForDefaultBucket().substring("s3://".length());
        Path path = new Path("s3a://" + bucket + "/s3a-test/" + UUID.randomUUID() + ".txt");

        Options options = new Options();
        MINIO.getS3ConfigOptions().forEach(options::set);
        FileIO fileIO = FileIO.get(path, CatalogContext.create(options, new Configuration()));

        System.out.println("[s3a] FileIO class = " + fileIO.getClass().getName());
        assertTrue(
                fileIO.getClass().getName().startsWith("org.apache.paimon.s3.S3Loader"),
                "expected the S3 plugin FileIO, got " + fileIO.getClass().getName());

        fileIO.writeFile(path, "hello-s3a", true);
        assertTrue(fileIO.exists(path));
        assertEquals("hello-s3a", fileIO.readFileUtf8(path));
        assertTrue(fileIO.listStatus(path.getParent()).length >= 1);
        assertTrue(fileIO.delete(path, false));
    }

    @Test
    public void testS3AndS3aAreBothUsable() throws Exception {
        String bucket = MINIO.getS3UriForDefaultBucket().substring("s3://".length());
        String name = "both-" + UUID.randomUUID() + ".txt";
        Options options = new Options();
        MINIO.getS3ConfigOptions().forEach(options::set);
        CatalogContext context = CatalogContext.create(options, new Configuration());

        Path s3Path = new Path("s3://" + bucket + "/s3a-test/" + name);
        Path s3aPath = new Path("s3a://" + bucket + "/s3a-test/" + name);

        FileIO.get(s3Path, context).writeFile(s3Path, "written-via-s3", true);
        // the very same object must be visible through the s3a scheme
        assertEquals("written-via-s3", FileIO.get(s3aPath, context).readFileUtf8(s3aPath));
    }

    @Test
    public void testUnderlyingFileSystemIsS3A() throws Exception {
        assertUnderlyingFileSystemIsS3A("s3a");
    }

    @Test
    public void testUnderlyingFileSystemIsS3AForPlainS3Scheme() throws Exception {
        assertUnderlyingFileSystemIsS3A("s3");
    }

    private void assertUnderlyingFileSystemIsS3A(String scheme) throws Exception {
        String bucket = MINIO.getS3UriForDefaultBucket().substring("s3://".length());
        Path path =
                new Path(scheme + "://" + bucket + "/s3a-test/fs-" + UUID.randomUUID() + ".txt");

        Options options = new Options();
        MINIO.getS3ConfigOptions().forEach(options::set);
        FileIO fileIO = FileIO.get(path, CatalogContext.create(options, new Configuration()));
        fileIO.writeFile(path, "probe", true);

        // unwrap PluginFileIO -> S3FileIO (loaded in the plugin classloader)
        Field lazy =
                Class.forName("org.apache.paimon.fs.PluginFileIO").getDeclaredField("lazyFileIO");
        lazy.setAccessible(true);
        Object delegate = lazy.get(fileIO);
        System.out.println("[" + scheme + "] plugin FileIO = " + delegate.getClass().getName());

        // unwrap S3FileIO -> the Hadoop FileSystem it actually created
        Field fsMapField = null;
        for (Class<?> c = delegate.getClass(); c != null; c = c.getSuperclass()) {
            try {
                fsMapField = c.getDeclaredField("fsMap");
                break;
            } catch (NoSuchFieldException ignored) {
                // keep walking
            }
        }
        fsMapField.setAccessible(true);
        Map<?, ?> fsMap = (Map<?, ?>) fsMapField.get(delegate);
        Object fs = fsMap.values().iterator().next();

        Method getScheme = fs.getClass().getMethod("getScheme");
        Method getUri = fs.getClass().getMethod("getUri");
        URI fsUri = (URI) getUri.invoke(fs);
        System.out.println(
                "["
                        + scheme
                        + "] hadoop FileSystem = "
                        + fs.getClass().getName()
                        + ", getScheme() = "
                        + getScheme.invoke(fs)
                        + ", getUri() = "
                        + fsUri);

        assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", fs.getClass().getName());
        assertEquals(scheme, fsUri.getScheme());
        assertEquals(bucket, fsUri.getHost());
        assertEquals("probe", fileIO.readFileUtf8(path));
        assertTrue(fileIO.delete(path, false));
    }
}
