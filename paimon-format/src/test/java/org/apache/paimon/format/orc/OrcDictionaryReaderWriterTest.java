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

package org.apache.paimon.format.orc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.AbstractDictionaryReaderWriterTest;
import org.apache.paimon.format.DictionaryOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.ReflectionUtils;

import org.apache.orc.OrcFile;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.TreeReaderFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for the read/writer of orc. */
public class OrcDictionaryReaderWriterTest extends AbstractDictionaryReaderWriterTest {
    protected final FileFormat fileFormat;

    public OrcDictionaryReaderWriterTest() {
        Options options = new Options();
        options.set(CoreOptions.FORMAT_FIELDS_DICTIONARY, false);
        // a22 enable dictionary
        options.set("fields.a22.dictionary-enable", "true");
        fileFormat = FileFormat.getFileFormat(options, "orc");
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    @Test
    public void testOrcWriterOpt() {
        OrcWriterFactory writerFactory =
                (OrcWriterFactory) getFileFormat().createWriterFactory(rowType);
        OrcFile.WriterOptions writerOptions = writerFactory.getWriterOptions();
        String[] directEncodingColumns = writerOptions.getDirectEncodingColumns().split(",");
        ArrayList<String> expected =
                Lists.newArrayList(
                        "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11",
                        "a12", "a13", "a14", "a15", "a16", "a17", "a18", "a19", "a20", "a21",
                        "a23");
        Assertions.assertThat(Arrays.asList(directEncodingColumns)).hasSameElementsAs(expected);
    }

    private TreeReaderFactory.TreeReader[] getReaderChild()
            throws IOException, ClassNotFoundException, NoSuchFieldException,
                    IllegalAccessException {
        FormatReaderFactory readerFactory = fileFormat.createReaderFactory(rowType);
        RecordReader<InternalRow> reader = readerFactory.createReader(LocalFileIO.create(), path);

        Class<?> vectorizedReaderClass =
                Class.forName("org.apache.paimon.format.orc.OrcReaderFactory$OrcVectorizedReader");
        RecordReaderImpl orcReader =
                ReflectionUtils.getFeild(vectorizedReaderClass, reader, "orcReader");

        TreeReaderFactory.StructTreeReader structTreeReader =
                ReflectionUtils.getFeild(RecordReaderImpl.class, orcReader, "reader");
        return structTreeReader.getChildReaders();
    }

    @Test
    public void testFormatDictionaryIT()
            throws IOException, NoSuchFieldException, ClassNotFoundException,
                    IllegalAccessException {
        TreeReaderFactory.TreeReader[] child = getReaderChild();

        // DataTypes.STRING()
        {
            TreeReaderFactory.TreeReader reader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, child[4], "reader");
            Assertions.assertThat(reader)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
        }

        // DataTypes.ARRAY(DataTypes.STRING())
        {
            assertDirectEncodeArray(child[6]);
        }

        // DataTypes.CHAR(100)
        {
            TreeReaderFactory.TreeReader reader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, child[7], "reader");
            Assertions.assertThat(reader)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
        }

        // DataTypes.VARCHAR(100)
        {
            TreeReaderFactory.TreeReader reader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, child[8], "reader");
            Assertions.assertThat(reader)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
        }

        // DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
        {
            assertDirectEncodeMap(child[17]);
        }

        // DataTypes.ROW(DataTypes.FIELD(100, "b1", DataTypes.STRING()))
        {
            assertDirectEncodeRow(child[18]);
        }

        // DataTypes.MULTISET(DataTypes.STRING())
        {
            assertDirectEncodeMultiSet(child[21]);
        }

        // Enable string dictionary
        {
            TreeReaderFactory.TreeReader reader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, child[22], "reader");
            Assertions.assertThat(reader)
                    .isInstanceOf(TreeReaderFactory.StringDictionaryTreeReader.class);
        }

        // Complex Nested Feilds:  Row<Row<Map<String,String>,Arrary,Set<String>>>
        {
            TreeReaderFactory.StructTreeReader reader =
                    (TreeReaderFactory.StructTreeReader) child[23];

            TreeReaderFactory.StructTreeReader childReader =
                    (TreeReaderFactory.StructTreeReader) reader.getChildReaders()[0];

            assertDirectEncodeMap(childReader.getChildReaders()[0]);
            assertDirectEncodeArray(childReader.getChildReaders()[1]);
            assertDirectEncodeMultiSet(childReader.getChildReaders()[2]);
        }
    }

    private static void assertDirectEncodeArray(TreeReaderFactory.TreeReader child)
            throws NoSuchFieldException, IllegalAccessException {
        TreeReaderFactory.StringTreeReader elementReader =
                ReflectionUtils.getFeild(
                        TreeReaderFactory.ListTreeReader.class, child, "elementReader");

        TreeReaderFactory.TreeReader reader =
                ReflectionUtils.getFeild(
                        TreeReaderFactory.StringTreeReader.class, elementReader, "reader");
        Assertions.assertThat(reader).isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
    }

    private static void assertDirectEncodeMap(TreeReaderFactory.TreeReader child)
            throws NoSuchFieldException, IllegalAccessException {
        TreeReaderFactory.TreeReader keyReader =
                ReflectionUtils.getFeild(TreeReaderFactory.MapTreeReader.class, child, "keyReader");
        TreeReaderFactory.TreeReader valueReader =
                ReflectionUtils.getFeild(
                        TreeReaderFactory.MapTreeReader.class, child, "valueReader");

        TreeReaderFactory.TreeReader reader1 =
                ReflectionUtils.getFeild(
                        TreeReaderFactory.StringTreeReader.class, keyReader, "reader");

        TreeReaderFactory.TreeReader reader2 =
                ReflectionUtils.getFeild(
                        TreeReaderFactory.StringTreeReader.class, valueReader, "reader");
        Assertions.assertThat(reader1).isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
        Assertions.assertThat(reader2).isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
    }

    private static void assertDirectEncodeRow(TreeReaderFactory.TreeReader child)
            throws NoSuchFieldException, IllegalAccessException {
        TreeReaderFactory.TreeReader[] fields =
                ReflectionUtils.getFeild(TreeReaderFactory.StructTreeReader.class, child, "fields");
        TreeReaderFactory.TreeReader reader =
                ReflectionUtils.getFeild(
                        TreeReaderFactory.StringTreeReader.class, fields[0], "reader");
        Assertions.assertThat(reader).isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
    }

    private static void assertDirectEncodeMultiSet(TreeReaderFactory.TreeReader child)
            throws NoSuchFieldException, IllegalAccessException {
        TreeReaderFactory.TreeReader keyReader =
                ReflectionUtils.getFeild(TreeReaderFactory.MapTreeReader.class, child, "keyReader");
        TreeReaderFactory.TreeReader strReader =
                ReflectionUtils.getFeild(
                        TreeReaderFactory.StringTreeReader.class, keyReader, "reader");

        Assertions.assertThat(strReader)
                .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
    }

    @Test
    public void testDictionaryOpt() {
        {
            Options options = new Options();
            options.set(CoreOptions.FORMAT_FIELDS_DICTIONARY, false);
            CoreOptions coreOptions = new CoreOptions(options);
            List<String> fieldNames =
                    org.apache.paimon.shade.guava30.com.google.common.collect.Lists.newArrayList(
                            "a", "b", "c", "d");
            DictionaryOptions dictionaryOptions = coreOptions.getDictionaryOptions();
            assertThat(dictionaryOptions.mergeFieldsDictionaryOpt(fieldNames).keySet())
                    .hasSameElementsAs(fieldNames);
        }

        {
            Options options = new Options();
            options.set(CoreOptions.FORMAT_FIELDS_DICTIONARY, false);
            options.set("fields.c.dictionary-enable", "true");
            CoreOptions coreOptions = new CoreOptions(options);
            DictionaryOptions dictionaryOptions = coreOptions.getDictionaryOptions();
            List<String> fieldNames =
                    org.apache.paimon.shade.guava30.com.google.common.collect.Lists.newArrayList(
                            "a", "b", "c", "d");
            List<String> disableDictionaryField = dictionaryOptions.getDisableDicFields(fieldNames);
            assertThat(disableDictionaryField)
                    .hasSameElementsAs(
                            org.apache.paimon.shade.guava30.com.google.common.collect.Lists
                                    .newArrayList("a", "b", "d"));
        }

        {
            Options options = new Options();
            options.set(CoreOptions.FORMAT_FIELDS_DICTIONARY, true);
            options.set("fields.c.dictionary-enable", "false");
            CoreOptions coreOptions = new CoreOptions(options);
            DictionaryOptions dictionaryOptions = coreOptions.getDictionaryOptions();
            List<String> fieldNames =
                    org.apache.paimon.shade.guava30.com.google.common.collect.Lists.newArrayList(
                            "a", "b", "c", "d");
            List<String> disableDictionaryField = dictionaryOptions.getDisableDicFields(fieldNames);
            assertThat(disableDictionaryField)
                    .hasSameElementsAs(
                            org.apache.paimon.shade.guava30.com.google.common.collect.Lists
                                    .newArrayList("c"));
        }
    }
}
