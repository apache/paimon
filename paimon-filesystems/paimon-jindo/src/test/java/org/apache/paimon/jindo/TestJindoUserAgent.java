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
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JindoUserAgent}. */
public class TestJindoUserAgent {

    private static final String[] SCHEMES = {"oss", "dls"};

    @Test
    public void testDefault() {
        assertThat(JindoBuildVersions.PAIMON).matches("\\d+\\.\\d+\\S*");
        Options hadoopOptions = configure(new Options());
        for (String scheme : SCHEMES) {
            assertThat(hadoopOptions.get(key(scheme, "features"))).isEqualTo(JindoUserAgent.PAIMON);
            assertThat(hadoopOptions.get(key(scheme, "module"))).isNull();
            assertThat(hadoopOptions.get(key(scheme, "extended"))).isNull();
        }
    }

    @Test
    public void testCatalogWideKeys() {
        Options options = new Options();
        options.set(JindoUserAgent.MODULE, "MyApp/1.0");
        options.set(JindoUserAgent.FEATURES, " Flink  Spark ");
        options.set(JindoUserAgent.EXTENDED, "vvr");
        Options hadoopOptions = configure(options);
        for (String scheme : SCHEMES) {
            assertThat(hadoopOptions.get(key(scheme, "module"))).isEqualTo("MyApp/1.0");
            assertThat(hadoopOptions.get(key(scheme, "features")))
                    .isEqualTo(JindoUserAgent.PAIMON + " Flink Spark");
            assertThat(hadoopOptions.get(key(scheme, "extended"))).isEqualTo("vvr");
        }
    }

    @Test
    public void testSchemeKeysOverrideCatalogWideKeys() {
        Options options = new Options();
        options.set(JindoUserAgent.FEATURES, "Flink");
        options.set(JindoUserAgent.EXTENDED, "vvr");
        options.set(key("oss", "features"), "morax/2.6.0");
        options.set(key("oss", "extended"), "bennett/2.7.0");
        Options hadoopOptions = configure(options);
        assertThat(hadoopOptions.get(key("oss", "features")))
                .isEqualTo(JindoUserAgent.PAIMON + " morax/2.6.0");
        assertThat(hadoopOptions.get(key("oss", "extended"))).isEqualTo("bennett/2.7.0");
        assertThat(hadoopOptions.get(key("dls", "features")))
                .isEqualTo(JindoUserAgent.PAIMON + " Flink");
        assertThat(hadoopOptions.get(key("dls", "extended"))).isEqualTo("vvr");
    }

    @Test
    public void testAccessTrackingIsAppendedToCatalogWideExtended() {
        Options options = new Options();
        options.set(JindoUserAgent.EXTENDED, "vvr");
        options.set(JindoUserAgent.DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123 user/alice");
        Options hadoopOptions = configure(options);
        for (String scheme : SCHEMES) {
            assertThat(hadoopOptions.get(key(scheme, "extended")))
                    .isEqualTo("vvr uid/123 user/alice");
        }
    }

    @Test
    public void testPaimonFeatureIsNotDuplicated() {
        Options options = new Options();
        options.set(JindoUserAgent.FEATURES, "Flink Paimon");
        assertThat(configure(options).get(key("oss", "features"))).isEqualTo("Flink Paimon");
    }

    private static String key(String scheme, String part) {
        return "fs." + scheme + ".user.agent." + part;
    }

    private static Options configure(Options options) {
        options.set("fs.oss.accessKeyId", "testAk");
        options.set("fs.oss.accessKeySecret", "testSk");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));
        try {
            Field field = JindoFileIO.class.getDeclaredField("hadoopOptions");
            field.setAccessible(true);
            return (Options) field.get(fileIO);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
