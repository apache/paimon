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

package org.apache.paimon.fs;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Guards the ownership of every public method declared by {@link FileIO}. */
public class FileIOContractCoverageTest {

    private static final Set<String> PROVIDER_CORE =
            signatures(
                    "isObjectStore()",
                    "newInputStream(Path)",
                    "newOutputStream(Path,boolean)",
                    "getFileStatus(Path)",
                    "listStatus(Path)",
                    "exists(Path)",
                    "delete(Path,boolean)",
                    "mkdirs(Path)",
                    "rename(Path,Path)");

    private static final Set<String> DEFAULT_METHOD =
            signatures(
                    "newTwoPhaseOutputStream(Path,boolean)",
                    "listFiles(Path,boolean)",
                    "listFilesIterative(Path,boolean)",
                    "listDirectories(Path)",
                    "deleteQuietly(Path)",
                    "deleteFilesQuietly(List)",
                    "deleteDirectoryQuietly(Path)",
                    "getFileSize(Path)",
                    "isDir(Path)",
                    "checkOrMkdirs(Path)",
                    "readFileUtf8(Path)",
                    "tryToWriteAtomic(Path,String)",
                    "writeFile(Path,String,boolean)",
                    "overwriteFileUtf8(Path,String)",
                    "overwriteHintFile(Path,String)",
                    "copyFile(Path,Path,boolean)",
                    "copyFiles(Path,Path,boolean)",
                    "readOverwrittenFileUtf8(Path)");

    private static final Set<String> PROVIDER_LIFECYCLE =
            signatures("configure(CatalogContext)", "setRuntimeContext(Map)", "close()");

    private static final Set<String> FACTORY =
            signatures(
                    "get(Path,CatalogContext)",
                    "discoverLoaders()",
                    "checkAccess(FileIOLoader,Path,CatalogContext)");

    private static final Set<String> OPTIONAL_CAPABILITY =
            signatures(
                    "archive(Path,StorageType)",
                    "restoreArchive(Path,Duration)",
                    "unarchive(Path,StorageType)",
                    "createBlobPresignedUrl(Path,BlobDescriptor,Duration)");

    @Test
    public void testEveryDeclaredPublicMethodHasExactlyOneOwner() {
        Set<String> actual =
                Arrays.stream(FileIO.class.getDeclaredMethods())
                        .filter(method -> Modifier.isPublic(method.getModifiers()))
                        .filter(method -> !method.isSynthetic())
                        .map(FileIOContractCoverageTest::signature)
                        .collect(Collectors.toSet());

        List<Set<String>> categories =
                Arrays.asList(
                        PROVIDER_CORE,
                        DEFAULT_METHOD,
                        PROVIDER_LIFECYCLE,
                        FACTORY,
                        OPTIONAL_CAPABILITY);
        Set<String> classified = new HashSet<>();
        for (Set<String> category : categories) {
            assertThat(Collections.disjoint(category, classified)).isTrue();
            classified.addAll(category);
        }

        assertThat(actual).hasSize(37);
        assertThat(classified).containsExactlyInAnyOrderElementsOf(actual);
    }

    private static Set<String> signatures(String... signatures) {
        return new HashSet<>(Arrays.asList(signatures));
    }

    private static String signature(Method method) {
        return method.getName()
                + Arrays.stream(method.getParameterTypes())
                        .map(Class::getSimpleName)
                        .collect(Collectors.joining(",", "(", ")"));
    }
}
