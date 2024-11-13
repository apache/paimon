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

package org.apache.paimon.format;

import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.options.Options;

import org.junit.Test;

import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

/** test format performance. */
public class FormatPerformanceTest {

    @Test
    public void testFormatPerformance() {
        double objectCacheCost = newFileFormatWithObjectCache();
        double serviceCost = newFileFormatWithServiceLoader();
        double factoryUtilCost = newFileFormatWithFactoryUtil();
        assertThat(objectCacheCost * 30 < serviceCost).isTrue();
        assertThat(objectCacheCost * 30 < factoryUtilCost).isTrue();
    }

    private double newFileFormatWithServiceLoader() {
        for (int i = 0; i < 1000; i++) {
            loadFromIdentifier();
        }
        int times = 10_000;
        long start = System.nanoTime();
        int nothing = 0;
        for (int i = 0; i < times; i++) {
            nothing += loadFromIdentifier();
        }
        nothing = 0;
        return ((double) (System.nanoTime() - start)) / 1000_000 / times + nothing;
    }

    private double newFileFormatWithObjectCache() {
        for (int i = 0; i < 1000; i++) {
            newFileFormat();
        }
        int times = 10_000;
        long start = System.nanoTime();
        for (int i = 0; i < times; i++) {
            newFileFormat();
        }
        return ((double) (System.nanoTime() - start)) / 1000_000 / times;
    }

    private double newFileFormatWithFactoryUtil() {
        for (int i = 0; i < 1000; i++) {
            newFileFormatFromFactoryUtil();
        }
        int times = 10_000;
        long start = System.nanoTime();
        for (int i = 0; i < times; i++) {
            newFileFormatFromFactoryUtil();
        }
        return ((double) (System.nanoTime() - start)) / 1000_000 / times;
    }

    private int loadFromIdentifier() {
        ServiceLoader<FileFormatFactory> serviceLoader =
                ServiceLoader.load(FileFormatFactory.class, FileFormat.class.getClassLoader());
        int i = 0;
        for (FileFormatFactory factory : serviceLoader) {
            i++;
        }
        return i;
    }

    private FileFormat newFileFormat() {
        FileFormat orc = FileFormat.fromIdentifier("orc", new Options());
        return orc;
    }

    @Test
    public void newFileFormatFromFactoryUtil() {
        FileFormatFactory fileFormatFactory =
                FactoryUtil.discoverFactory(
                        FileFormatFactory.class.getClassLoader(), FileFormatFactory.class, "orc");
    }
}
