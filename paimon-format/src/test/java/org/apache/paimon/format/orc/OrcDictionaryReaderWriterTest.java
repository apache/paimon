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

/** IT Case for the read/writer of orc. */
public class OrcDictionaryReaderWriterTest extends AbstractDictionaryReaderWriterTest {
    protected final FileFormat fileFormat;

    public OrcDictionaryReaderWriterTest() {
        Options options = new Options();
        options.set(CoreOptions.FORMAT_FIELDS_DICTIONARY, false);
        // a22 enable dictionary
        options.set("format.fields-dictionary.a22.enable", "true");
        fileFormat = FileFormat.getFileFormat(options, "orc");
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    @Test
    public void testWriterFormatOpt() {
        OrcWriterFactory writerFactory =
                (OrcWriterFactory) getFileFormat().createWriterFactory(rowType);
        OrcFile.WriterOptions writerOptions = writerFactory.getWriterOptions();
        String[] directEncodingColumns = writerOptions.getDirectEncodingColumns().split(",");
        ArrayList<String> expected =
                Lists.newArrayList(
                        "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11",
                        "a12", "a13", "a14", "a15", "a16", "a17", "a18", "a19", "a20", "a21");
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
    public void testReadFormatDictionary()
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
            TreeReaderFactory.StringTreeReader elementReader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.ListTreeReader.class, child[6], "elementReader");

            TreeReaderFactory.TreeReader reader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, elementReader, "reader");
            Assertions.assertThat(reader)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
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
            TreeReaderFactory.TreeReader keyReader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.MapTreeReader.class, child[17], "keyReader");
            TreeReaderFactory.TreeReader valueReader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.MapTreeReader.class, child[17], "valueReader");

            TreeReaderFactory.TreeReader reader1 =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, keyReader, "reader");

            TreeReaderFactory.TreeReader reader2 =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, valueReader, "reader");
            Assertions.assertThat(reader1)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
            Assertions.assertThat(reader2)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
        }

        //        DataTypes.ROW(DataTypes.FIELD(100, "b1", DataTypes.STRING()))
        {
            TreeReaderFactory.TreeReader[] fields =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StructTreeReader.class, child[18], "fields");
            TreeReaderFactory.TreeReader reader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, fields[0], "reader");
            Assertions.assertThat(reader)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
        }

        //        DataTypes.MULTISET(DataTypes.STRING())
        {
            TreeReaderFactory.TreeReader keyReader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.MapTreeReader.class, child[21], "keyReader");
            TreeReaderFactory.TreeReader strReader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, keyReader, "reader");

            Assertions.assertThat(strReader)
                    .isInstanceOf(TreeReaderFactory.StringDirectTreeReader.class);
        }
        // enable dictionary
        {
            TreeReaderFactory.TreeReader reader =
                    ReflectionUtils.getFeild(
                            TreeReaderFactory.StringTreeReader.class, child[22], "reader");
            Assertions.assertThat(reader)
                    .isInstanceOf(TreeReaderFactory.StringDictionaryTreeReader.class);
        }
    }
}
