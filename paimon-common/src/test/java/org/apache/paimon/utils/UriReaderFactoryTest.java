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

package org.apache.paimon.utils;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.UriReader.FileUriReader;
import org.apache.paimon.utils.UriReader.HttpUriReader;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link UriReaderFactory}. */
public class UriReaderFactoryTest {

    private final UriReaderFactory factory =
            new UriReaderFactory(CatalogContext.create(new Options()));

    @Test
    public void testCreateHttpUriReader() {
        UriReader reader = factory.create("http://example.com/file.txt");
        assertThat(reader).isInstanceOf(HttpUriReader.class);
    }

    @Test
    public void testCreateHttpsUriReader() {
        UriReader reader = factory.create("https://example.com/file.txt");
        assertThat(reader).isInstanceOf(HttpUriReader.class);
    }

    @Test
    public void testCreateFileUriReader() {
        UriReader reader = factory.create("file:///path/to/file.txt");
        assertThat(reader).isInstanceOf(FileUriReader.class);
    }

    @Test
    public void testCreateUriReaderWithAuthority() {
        UriReader reader1 = factory.create("http://my_bucket1/path/to/file.txt");
        UriReader reader2 = factory.create("http://my_bucket2/path/to/file.txt");
        assertThat(reader1).isNotEqualTo(reader2);
    }

    @Test
    public void testCachedReadersWithSameSchemeAndAuthority() {
        UriReader reader1 = factory.create("http://my_bucket/path/to/file1.txt");
        UriReader reader2 = factory.create("http://my_bucket/path/to/file2.txt");
        assertThat(reader1).isSameAs(reader2);
    }

    @Test
    public void testCachedReadersWithNullAuthority() {
        UriReader reader1 = factory.create("file:///path/to/file1.txt");
        UriReader reader2 = factory.create("file:///path/to/file2.txt");
        assertThat(reader1).isSameAs(reader2);
    }

    @Test
    public void testCreateUriReaderWithLocalPath() {
        UriReader reader = factory.create("/local/path/to/file.txt");
        assertThat(reader).isInstanceOf(FileUriReader.class);
    }
}
