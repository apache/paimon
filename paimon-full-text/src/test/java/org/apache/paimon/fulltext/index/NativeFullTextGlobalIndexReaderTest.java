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

package org.apache.paimon.fulltext.index;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.index.fulltext.FullTextIndexReader;
import org.apache.paimon.predicate.FullTextSearch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CompletionException;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for resource lifecycle of {@link NativeFullTextGlobalIndexReader}. */
public class NativeFullTextGlobalIndexReaderTest {

    @Test
    public void testInitializationFailurePreservesOriginalExceptionWhenCleanupThrowsUnchecked() {
        RuntimeException closeFailure = new RuntimeException("input close failure");
        TestingSeekableInputStream input =
                new TestingSeekableInputStream(new byte[64], closeFailure);
        NativeFullTextGlobalIndexReader reader = createReader(input);

        Throwable failure =
                catchThrowable(
                        () ->
                                reader.visitFullTextSearch(new FullTextSearch("text", "{}", 1))
                                        .join());

        assertThat(failure).isInstanceOf(CompletionException.class);
        Throwable initializationFailure = failure.getCause();
        assertThat(initializationFailure)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("invalid storage format");
        assertThat(initializationFailure.getSuppressed()).containsExactly(closeFailure);
        assertThat(input.closeCalls).isEqualTo(1);

        assertThatCode(reader::close).doesNotThrowAnyException();
        assertThat(input.closeCalls).isEqualTo(1);
    }

    @Test
    public void testCloseAttemptsAllResourcesWhenReaderCloseThrowsError() throws Exception {
        NativeFullTextGlobalIndexReader reader =
                createReader(new TestingSeekableInputStream(new byte[1], null));
        FullTextIndexReader nativeReader = mock(FullTextIndexReader.class);
        UnsatisfiedLinkError readerCloseFailure = new UnsatisfiedLinkError("reader close failure");
        doThrow(readerCloseFailure).when(nativeReader).close();
        IOException inputCloseFailure = new IOException("input close failure");
        TestingSeekableInputStream input =
                new TestingSeekableInputStream(new byte[1], inputCloseFailure);
        setResource(reader, "reader", nativeReader);
        setResource(reader, "inputStream", input);

        Throwable failure = catchThrowable(reader::close);

        assertThat(failure).isSameAs(readerCloseFailure);
        assertThat(failure.getSuppressed()).containsExactly(inputCloseFailure);
        assertThat(input.closeCalls).isEqualTo(1);

        assertThatCode(reader::close).doesNotThrowAnyException();
        verify(nativeReader, times(1)).close();
        assertThat(input.closeCalls).isEqualTo(1);
    }

    private NativeFullTextGlobalIndexReader createReader(TestingSeekableInputStream input) {
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(new Path("file:///unused"), 64L, new byte[0]);
        return new NativeFullTextGlobalIndexReader(
                ignored -> input, Collections.singletonList(meta), newDirectExecutorService());
    }

    private static void setResource(
            NativeFullTextGlobalIndexReader reader, String fieldName, Object resource)
            throws ReflectiveOperationException {
        Field field = NativeFullTextGlobalIndexReader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(reader, resource);
    }

    private static class TestingSeekableInputStream extends ByteArraySeekableStream {

        private final Throwable closeFailure;
        private int closeCalls;

        private TestingSeekableInputStream(byte[] bytes, Throwable closeFailure) {
            super(bytes);
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            if (closeFailure instanceof IOException) {
                throw (IOException) closeFailure;
            } else if (closeFailure instanceof RuntimeException) {
                throw (RuntimeException) closeFailure;
            } else if (closeFailure instanceof Error) {
                throw (Error) closeFailure;
            }
            super.close();
        }
    }
}
