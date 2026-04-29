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

package org.apache.paimon.catalog;

import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CatalogContext} factory logic. */
public class CatalogContextFactoryTest {

    @Test
    public void testBasicContextCreation() {
        Options options = new Options();
        options.set(WAREHOUSE, "file:///tmp/warehouse");

        CatalogContext context = CatalogContext.create(options);

        // Should create basic CatalogContext for local filesystem
        assertThat(context).isInstanceOf(CatalogContext.class);
        assertThat(context).isNotInstanceOf(CatalogHadoopContext.class);
    }

    @Test
    public void testHadoopContextForHiveMetastore() {
        Options options = new Options();
        options.set(WAREHOUSE, "file:///tmp/warehouse");
        options.set(METASTORE, "hive");

        CatalogContext context = CatalogContext.create(options);

        // Should create CatalogHadoopContext for Hive metastore
        assertThat(context).isInstanceOf(CatalogHadoopContext.class);
    }

    @Test
    public void testHadoopContextForHdfsFilesystem() {
        Options options = new Options();
        options.set(WAREHOUSE, "hdfs://namenode:8020/warehouse");

        CatalogContext context = CatalogContext.create(options);

        // Should create CatalogHadoopContext for HDFS
        assertThat(context).isInstanceOf(CatalogHadoopContext.class);
    }

    @Test
    public void testHadoopContextForViewFs() {
        Options options = new Options();
        options.set(WAREHOUSE, "viewfs://cluster/warehouse");

        CatalogContext context = CatalogContext.create(options);

        // Should create CatalogHadoopContext for ViewFS
        assertThat(context).isInstanceOf(CatalogHadoopContext.class);
    }

    @Test
    public void testHadoopContextForKerberos() {
        Options options = new Options();
        options.set(WAREHOUSE, "file:///tmp/warehouse");
        options.set("security.kerberos.login.principal", "user@REALM");
        options.set("security.kerberos.login.keytab", "/path/to/keytab");

        CatalogContext context = CatalogContext.create(options);

        // Should create CatalogHadoopContext for Kerberos
        assertThat(context).isInstanceOf(CatalogHadoopContext.class);
    }

    @Test
    public void testHadoopContextForS3WithoutHadoop() {
        Options options = new Options();
        options.set(WAREHOUSE, "s3://bucket/warehouse");

        CatalogContext context = CatalogContext.create(options);

        // Should create basic CatalogContext for S3 (cloud storage without Hadoop)
        assertThat(context).isInstanceOf(CatalogContext.class);
        assertThat(context).isNotInstanceOf(CatalogHadoopContext.class);
    }

    @Test
    public void testExplicitHadoopContextCreation() {
        Options options = new Options();
        options.set(WAREHOUSE, "file:///tmp/warehouse");

        // Explicitly create CatalogHadoopContext
        CatalogHadoopContext hadoopContext = CatalogHadoopContext.create(options);

        assertThat(hadoopContext).isInstanceOf(CatalogHadoopContext.class);
        assertThat(hadoopContext.hadoopConf()).isNotNull();
    }

    @Test
    public void testCopyPreservesType() {
        Options options1 = new Options();
        options1.set(WAREHOUSE, "hdfs://namenode:8020/warehouse");

        CatalogContext context = CatalogContext.create(options1);
        assertThat(context).isInstanceOf(CatalogHadoopContext.class);

        // Copy with different warehouse
        Options options2 = new Options();
        options2.set(WAREHOUSE, "hdfs://namenode:8020/new_warehouse");

        CatalogContext copiedContext = context.copy(options2);

        // Should preserve Hadoop context type
        assertThat(copiedContext).isInstanceOf(CatalogHadoopContext.class);
    }
}
