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

package org.apache.paimon.fs.hadoop;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HadoopSecuredFileSystem}. */
public class HadoopSecuredFileSystemTest {
    @TempDir private java.nio.file.Path tmp;

    @Test
    public void testEmptySecurityConfigurationDoesNotWrapFileSystem() throws Exception {
        HadoopFileIO fileIO = createFileIO(new Options());

        assertThat(fileIO.getFileSystem(new org.apache.hadoop.fs.Path("file:///tmp/test")))
                .isNotInstanceOf(HadoopSecuredFileSystem.class);
    }

    @Test
    public void testValidKeytabAndPrincipalWrapsFileSystem() throws Exception {
        File keytabFile = new File(tmp.toFile(), "test-keytab.keytab");
        assertThat(keytabFile.createNewFile()).isTrue();

        Options options = new Options();
        options.set("security.kerberos.login.principal", "test-user");
        options.set("security.kerberos.login.keytab", keytabFile.getAbsolutePath());

        HadoopFileIO fileIO = createFileIO(options);

        assertThat(fileIO.getFileSystem(new org.apache.hadoop.fs.Path("file:///tmp/test")))
                .isInstanceOf(HadoopSecuredFileSystem.class);
    }

    @Test
    public void testPreserveExternalUgiWhenNoKerberosCredentials() throws Exception {
        Options options = new Options();

        HadoopFileIO fileIO = createFileIO(options);
        assertThat(fileIO.getFileSystem(new org.apache.hadoop.fs.Path("file:///tmp/test")))
                .isNotInstanceOf(HadoopSecuredFileSystem.class);
    }

    @Test
    public void testReturnOriginalFileSystemWhenSecurityConfigIsIllegal() throws Exception {
        File keytabFile = new File(tmp.toFile(), "test-keytab.keytab");
        assertThat(keytabFile.createNewFile()).isTrue();

        Options options = new Options();
        options.set("security.kerberos.login.keytab", keytabFile.getAbsolutePath());

        HadoopFileIO fileIO = createFileIO(options);
        assertThat(fileIO.getFileSystem(new org.apache.hadoop.fs.Path("file:///tmp/test")))
                .isNotInstanceOf(HadoopSecuredFileSystem.class);
    }

    @Test
    public void testUnwrapAndCallAsLoginUser() throws Exception {
        // tryAtomicOverwriteViaRename has to reach FileSystem's protected 3-arg rename on the
        // file system under the wrapper, and run the call as the login user.
        HadoopSecuredFileSystem secured = securedFileSystem();

        assertThat(secured.unwrap()).isNotInstanceOf(HadoopSecuredFileSystem.class);
        assertThat(secured.<String>callAsLoginUser(() -> "ran")).isEqualTo("ran");
        assertThatThrownBy(
                        () ->
                                secured.callAsLoginUser(
                                        () -> {
                                            throw new IOException("rename failed");
                                        }))
                .isInstanceOf(IOException.class)
                .hasMessage("rename failed");
    }

    @Test
    public void testAtomicRenameRunsOnTheDelegateAsTheLoginUser() throws Exception {
        File dir = new File(tmp.toFile(), "atomic");
        assertThat(dir.mkdirs()).isTrue();
        Path target = new Path(new File(dir, "LATEST").toURI());

        AtomicRenameFileSystem delegate = new AtomicRenameFileSystem();
        delegate.initialize(target.toUri(), new Configuration());
        HadoopFileIO fileIO = new HadoopFileIO(target);
        Options options = kerberosOptions();
        fileIO.configure(CatalogContext.create(options));
        fileIO.setFileSystem(
                HadoopSecuredFileSystem.trySecureFileSystem(
                        delegate, options, new Configuration()));

        // Reflection only sees public methods, and the wrapper cannot override FileSystem's
        // protected 3-arg rename, so this only works if the lookup goes to the delegate.
        assertThat(fileIO.tryAtomicOverwriteViaRename(target, "content")).isTrue();
        assertThat(delegate.atomicRenames).isEqualTo(1);
        assertThat(delegate.renameUser)
                .isEqualTo(UserGroupInformation.getLoginUser().getUserName());
        assertThat(fileIO.readFileUtf8(target)).isEqualTo("content");
    }

    /** A local file system exposing {@link FileSystem}'s 3-arg rename as public. */
    private static class AtomicRenameFileSystem extends RawLocalFileSystem {

        private int atomicRenames;
        private String renameUser;

        @Override
        public void rename(
                org.apache.hadoop.fs.Path src,
                org.apache.hadoop.fs.Path dst,
                org.apache.hadoop.fs.Options.Rename... options)
                throws IOException {
            atomicRenames++;
            renameUser = UserGroupInformation.getCurrentUser().getUserName();
            if (!rename(src, dst)) {
                throw new IOException("rename failed");
            }
        }
    }

    private Options kerberosOptions() throws IOException {
        File keytabFile = new File(tmp.toFile(), "k.keytab");
        if (!keytabFile.exists()) {
            assertThat(keytabFile.createNewFile()).isTrue();
        }
        Options options = new Options();
        options.set("security.kerberos.login.principal", "test-user");
        options.set("security.kerberos.login.keytab", keytabFile.getAbsolutePath());
        return options;
    }

    private HadoopSecuredFileSystem securedFileSystem() throws Exception {
        File keytabFile = new File(tmp.toFile(), "k.keytab");
        assertThat(keytabFile.createNewFile()).isTrue();
        Options options = new Options();
        options.set("security.kerberos.login.principal", "test-user");
        options.set("security.kerberos.login.keytab", keytabFile.getAbsolutePath());

        org.apache.hadoop.fs.FileSystem fs =
                createFileIO(options).getFileSystem(new org.apache.hadoop.fs.Path("file:///tmp/t"));
        assertThat(fs).isInstanceOf(HadoopSecuredFileSystem.class);
        return (HadoopSecuredFileSystem) fs;
    }

    private HadoopFileIO createFileIO(Options options) {
        HadoopFileIO fileIO = new HadoopFileIO(new Path("file:///tmp/test"));
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }
}
