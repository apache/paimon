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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Abstract test base for serializers. */
public abstract class SerializerTestBase<T> {

    protected abstract Serializer<T> createSerializer();

    protected abstract boolean deepEquals(T t1, T t2);

    protected abstract T[] getTestData();

    protected T[] getSerializableTestData() {
        return getTestData();
    }

    protected List<Pair<T, String>> getSerializableToStringTestData() {
        return Collections.emptyList();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    protected void testCopy() {
        try {
            Serializer<T> serializer = getSerializer();
            T[] testData = getData();

            for (T datum : testData) {
                T copy = serializer.copy(datum);
                checkToString(copy);
                deepEquals("Copied element is not equal to the original element.", datum, copy);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializeIndividually() {
        try {
            Serializer<T> serializer = getSerializer();
            T[] testData = getData();

            for (T value : testData) {
                TestOutputView out = new TestOutputView();
                serializer.serialize(value, out);
                TestInputView in = out.getInputView();

                assertThat(in.available() > 0)
                        .withFailMessage("No data available during deserialization.")
                        .isTrue();

                T deserialized = serializer.deserialize(in);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", value, deserialized);

                assertThat(in.available())
                        .withFailMessage("Trailing data available after deserialization.")
                        .isEqualTo(0);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializeAsSequenceNoReuse() {
        try {
            Serializer<T> serializer = getSerializer();
            T[] testData = getData();

            TestOutputView out = new TestOutputView();
            for (T value : testData) {
                serializer.serialize(value, out);
            }

            TestInputView in = out.getInputView();

            int num = 0;
            while (in.available() > 0) {
                T deserialized = serializer.deserialize(in);
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", testData[num], deserialized);
                num++;
            }

            assertThat(num)
                    .withFailMessage("Wrong number of elements deserialized.")
                    .isEqualTo(testData.length);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testSerializabilityAndEquals() {
        try {
            Serializer<T> ser1 = getSerializer();
            Serializer<T> ser2;
            try {
                ser2 = InstantiationUtil.clone(ser1);
            } catch (Throwable e) {
                fail("The serializer is not serializable: " + e);
                return;
            }

            assertThat(ser2)
                    .withFailMessage("The copy of the serializer is not equal to the original one.")
                    .isEqualTo(ser1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    void testNullability() {
        Serializer<T> serializer = getSerializer();
        try {
            checkIfNullSupported(serializer);
        } catch (Throwable t) {
            System.err.println(t.getMessage());
            t.printStackTrace();
            fail("Unexpected failure of null value handling: " + t.getMessage());
        }
    }

    @Test
    void testDuplicate() throws Exception {
        final int numThreads = 10;
        final Serializer<T> serializer = getSerializer();
        final CyclicBarrier startLatch = new CyclicBarrier(numThreads);
        final List<SerializerRunner> concurrentRunners = new ArrayList<>(numThreads);
        assertThat(serializer.duplicate()).isEqualTo(serializer);

        T[] testData = getData();

        for (int i = 0; i < numThreads; ++i) {
            SerializerRunner runner =
                    new SerializerRunner(startLatch, serializer.duplicate(), testData, 120L);

            runner.start();
            concurrentRunners.add(runner);
        }

        for (SerializerRunner concurrentRunner : concurrentRunners) {
            concurrentRunner.join();
            concurrentRunner.checkResult();
        }
    }

    @Test
    void testJavaSerializable() {
        try {
            T[] testData = getSerializableTestData();

            for (T value : testData) {
                byte[] bytes = InstantiationUtil.serializeObject(value);
                assertThat(bytes.length > 0)
                        .withFailMessage("No data available during deserialization.")
                        .isTrue();

                T deserialized =
                        InstantiationUtil.deserializeObject(
                                bytes, this.getClass().getClassLoader());
                checkToString(deserialized);

                deepEquals("Deserialized value if wrong.", value, deserialized);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    @Test
    protected void testSerializeToString() {
        try {
            Serializer<T> serializer = getSerializer();
            List<Pair<T, String>> serializableToStringTestData = getSerializableToStringTestData();
            for (Pair<T, String> datumToString : serializableToStringTestData) {
                String s = serializer.serializeToString(datumToString.getLeft());
                Assertions.assertEquals(datumToString.getRight(), s);
                T deserialized = serializer.deserializeFromString(s);
                deepEquals("Deserialized value if wrong.", datumToString.getLeft(), deserialized);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Exception in test: " + e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    private void deepEquals(String message, T should, T is) {
        assertThat(deepEquals(should, is)).withFailMessage(message).isTrue();
    }

    // --------------------------------------------------------------------------------------------

    protected Serializer<T> getSerializer() {
        Serializer<T> serializer = createSerializer();
        if (serializer == null) {
            throw new RuntimeException("Test case corrupt. Returns null as serializer.");
        }
        return serializer;
    }

    private T[] getData() {
        T[] data = getTestData();
        if (data == null) {
            throw new RuntimeException("Test case corrupt. Returns null as test data.");
        }
        return data;
    }

    // --------------------------------------------------------------------------------------------

    private static final class TestOutputView extends DataOutputStream implements DataOutputView {

        public TestOutputView() {
            super(new ByteArrayOutputStream(4096));
        }

        public TestInputView getInputView() {
            ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
            return new TestInputView(baos.toByteArray());
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
            for (int i = 0; i < numBytes; i++) {
                write(0);
            }
        }

        @Override
        public void write(DataInputView source, int numBytes) throws IOException {
            byte[] buffer = new byte[numBytes];
            source.readFully(buffer);
            write(buffer);
        }
    }

    /** Runner to test serializer duplication via concurrency. */
    class SerializerRunner extends Thread {
        final CyclicBarrier allReadyBarrier;
        final Serializer<T> serializer;
        final T[] testData;
        final long durationLimitMillis;
        Throwable failure;

        SerializerRunner(
                CyclicBarrier allReadyBarrier,
                Serializer<T> serializer,
                T[] testData,
                long testTargetDurationMillis) {

            this.allReadyBarrier = allReadyBarrier;
            this.serializer = serializer;
            this.testData = testData;
            this.durationLimitMillis = testTargetDurationMillis;
            this.failure = null;
        }

        @Override
        public void run() {
            DataInputDeserializer dataInputDeserializer = new DataInputDeserializer();
            DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
            try {
                allReadyBarrier.await();
                final long endTimeNanos = System.nanoTime() + durationLimitMillis * 1_000_000L;
                while (true) {
                    for (T testItem : testData) {
                        serializer.serialize(testItem, dataOutputSerializer);
                        dataInputDeserializer.setBuffer(
                                dataOutputSerializer.getSharedBuffer(),
                                0,
                                dataOutputSerializer.length());
                        T serdeTestItem = serializer.deserialize(dataInputDeserializer);
                        T copySerdeTestItem = serializer.copy(serdeTestItem);
                        dataOutputSerializer.clear();

                        assertThat(deepEquals(copySerdeTestItem, testItem))
                                .withFailMessage(
                                        "Serialization/Deserialization cycle resulted in an object that are not equal to the original.")
                                .isTrue();

                        // try to enforce some upper bound to the test time
                        if (System.nanoTime() >= endTimeNanos) {
                            return;
                        }
                    }
                }
            } catch (Throwable ex) {
                failure = ex;
            }
        }

        void checkResult() throws Exception {
            if (failure != null) {
                if (failure instanceof AssertionError) {
                    throw (AssertionError) failure;
                } else {
                    throw (Exception) failure;
                }
            }
        }
    }

    private static final class TestInputView extends DataInputStream implements DataInputView {

        public TestInputView(byte[] data) {
            super(new ByteArrayInputStream(data));
        }

        @Override
        public void skipBytesToRead(int numBytes) throws IOException {
            while (numBytes > 0) {
                int skipped = skipBytes(numBytes);
                numBytes -= skipped;
            }
        }
    }

    private static <T> void checkToString(T value) {
        if (value != null) {
            value.toString();
        }
    }

    private static <T> boolean checkIfNullSupported(@Nonnull Serializer<T> serializer) {
        DataOutputSerializer dos = new DataOutputSerializer(20);
        try {
            serializer.serialize(null, dos);
        } catch (IOException | RuntimeException e) {
            return false;
        }
        DataInputDeserializer dis = new DataInputDeserializer(dos.getSharedBuffer());
        try {
            checkArgument(serializer.deserialize(dis) == null);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Unexpected failure to deserialize just serialized null value with %s",
                            serializer.getClass().getName()),
                    e);
        }
        checkArgument(
                serializer.copy(null) == null,
                "Serializer %s has to be able properly copy null value if it can serialize it",
                serializer.getClass().getName());
        return true;
    }
}
