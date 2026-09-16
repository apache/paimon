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

package org.apache.paimon.rest;

import org.apache.paimon.rest.interceptor.LoggingInterceptor;
import org.apache.paimon.rest.interceptor.TimingInterceptor;
import org.apache.paimon.utils.SensitiveConfigUtils;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.entity.DecompressingEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.TruncatedChunkException;
import org.apache.hc.core5.reactor.ssl.SSLBufferMode;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utils for {@link HttpClientBuilder}. */
public class HttpClientUtils {

    private static final int MAX_BODY_RESUME_ATTEMPTS = 5;
    private static final Pattern CONTENT_RANGE_PATTERN =
            Pattern.compile("bytes\\s+(\\d+)-(\\d+)/(\\d+|\\*)", Pattern.CASE_INSENSITIVE);
    private static final RequestConfig DEFAULT_REQUEST_CONFIG =
            RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofMinutes(3))
                    .setResponseTimeout(Timeout.ofMinutes(3))
                    .build();

    public static final CloseableHttpClient DEFAULT_HTTP_CLIENT = createLoggingBuilder().build();

    public static HttpClientBuilder createLoggingBuilder() {
        HttpClientBuilder clientBuilder = createBuilder();
        clientBuilder
                .addRequestInterceptorFirst(new TimingInterceptor())
                .addResponseInterceptorLast(new LoggingInterceptor());
        return clientBuilder;
    }

    public static HttpClientBuilder createBuilder() {
        HttpClientBuilder clientBuilder = HttpClients.custom();
        clientBuilder.setDefaultRequestConfig(DEFAULT_REQUEST_CONFIG);

        clientBuilder.setConnectionManager(configureConnectionManager());
        clientBuilder.setRetryStrategy(new ExponentialHttpRequestRetryStrategy(5));
        return clientBuilder;
    }

    private static HttpClientConnectionManager configureConnectionManager() {
        PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
                PoolingHttpClientConnectionManagerBuilder.create();
        connectionManagerBuilder.useSystemProperties().setMaxConnTotal(100).setMaxConnPerRoute(100);

        // support TLS
        String[] tlsProtocols = {"TLSv1.2", "TLSv1.3"};
        connectionManagerBuilder.setTlsSocketStrategy(
                new DefaultClientTlsStrategy(
                        SSLContexts.createDefault(),
                        tlsProtocols,
                        null,
                        SSLBufferMode.STATIC,
                        HttpsSupport.getDefaultHostnameVerifier()));

        return connectionManagerBuilder.build();
    }

    public static InputStream getAsInputStream(String uri) throws IOException {
        return new ResumableHttpInputStream(uri);
    }

    /**
     * Checks whether an HTTP resource exists. HEAD is attempted first; when HEAD does not return
     * 200, a lightweight GET with {@code Range: bytes=0-0} is used to verify readability. This
     * avoids treating signed or GET-only URLs as missing when HEAD is rejected or returns a
     * different status than GET.
     */
    public static boolean exists(String uri) throws IOException {
        int headStatusCode = headStatusCode(uri);
        if (headStatusCode == HttpStatus.SC_OK) {
            return true;
        }
        int rangeStatusCode = getRangeStatusCode(uri);
        if (rangeStatusCode == HttpStatus.SC_OK
                || rangeStatusCode == HttpStatus.SC_PARTIAL_CONTENT
                || rangeStatusCode == HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE) {
            return true;
        }
        if (rangeStatusCode == HttpStatus.SC_NOT_FOUND) {
            return false;
        }
        throw new IOException(
                "Unexpected HTTP status code: "
                        + rangeStatusCode
                        + " for uri: "
                        + SensitiveConfigUtils.sanitizeUri(uri));
    }

    public static boolean isNotFoundError(Throwable throwable) {
        Integer statusCode = getHttpStatusCode(throwable);
        return statusCode != null && statusCode == HttpStatus.SC_NOT_FOUND;
    }

    public static boolean isInvalidUriException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof java.net.URISyntaxException) {
                return true;
            }
            if (current instanceof IllegalArgumentException
                    && current.getMessage() != null
                    && (current.getMessage().contains("Illegal character")
                            || current.getMessage()
                                    .startsWith(SensitiveConfigUtils.INVALID_URI_MESSAGE_PREFIX))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    public static Integer getHttpStatusCode(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                Integer statusCode = parseHttpStatusCode(current.getMessage());
                if (statusCode != null) {
                    return statusCode;
                }
            }
            current = current.getCause();
        }
        return null;
    }

    private static Integer parseHttpStatusCode(String message) {
        if (message.startsWith("HTTP error code: ")) {
            return parseStatusCodeSuffix(message.substring("HTTP error code: ".length()));
        }
        if (message.startsWith("Unexpected HTTP status code: ")) {
            int end = message.indexOf(' ', "Unexpected HTTP status code: ".length());
            String statusText =
                    end < 0
                            ? message.substring("Unexpected HTTP status code: ".length())
                            : message.substring("Unexpected HTTP status code: ".length(), end);
            return parseStatusCodeSuffix(statusText);
        }
        return null;
    }

    private static Integer parseStatusCodeSuffix(String statusText) {
        try {
            return Integer.parseInt(statusText.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int headStatusCode(String uri) throws IOException {
        HttpHead httpHead = newHttpHead(uri);
        try (CloseableHttpResponse response = execute(httpHead, uri)) {
            return response.getCode();
        }
    }

    private static int getRangeStatusCode(String uri) throws IOException {
        HttpGet httpGet = newHttpGet(uri);
        httpGet.addHeader("Range", "bytes=0-0");
        try (CloseableHttpResponse response = execute(httpGet, uri)) {
            return response.getCode();
        }
    }

    /**
     * Executes a request, converting any execute-stage failure into an exception that carries
     * neither the original message nor cause. Redirect and protocol errors (e.g. "Circular redirect
     * to &lt;Location&gt;") echo the target URL, which for a signed URL is a credential; only the
     * sanitized request URI is reported.
     */
    private static CloseableHttpResponse execute(ClassicHttpRequest request, String uri)
            throws IOException {
        try {
            return DEFAULT_HTTP_CLIENT.execute(request);
        } catch (IOException | RuntimeException e) {
            throw new IOException(
                    "HTTP request failed for uri: " + SensitiveConfigUtils.sanitizeUri(uri));
        }
    }

    public static HttpGet newHttpGet(String uri) {
        return newRequest(uri, HttpGet::new);
    }

    public static HttpHead newHttpHead(String uri) {
        return newRequest(uri, HttpHead::new);
    }

    public static HttpPost newHttpPost(String uri) {
        return newRequest(uri, HttpPost::new);
    }

    public static HttpPut newHttpPut(String uri) {
        return newRequest(uri, HttpPut::new);
    }

    public static HttpDelete newHttpDelete(String uri) {
        return newRequest(uri, HttpDelete::new);
    }

    /** A malformed URL leaks the raw URL from the constructor; sanitize it. */
    private static <T> T newRequest(String uri, Function<String, T> constructor) {
        try {
            return constructor.apply(uri);
        } catch (RuntimeException e) {
            throw SensitiveConfigUtils.invalidUri(uri);
        }
    }

    private static RuntimeException httpError(int statusCode) {
        return new RuntimeException("HTTP error code: " + statusCode);
    }

    /**
     * An HTTP stream which resumes a prematurely closed response body from the last byte already
     * returned to the caller.
     *
     * <p>The request retry strategy only covers failures before response headers are returned. A
     * {@link ConnectionClosedException} or {@link TruncatedChunkException} can instead be raised
     * while the entity stream is consumed. Replaying the whole response would duplicate bytes
     * already written by the caller. A strong ETag allows a byte range continuation. Without one, a
     * complete response is replayed and its already-delivered prefix is verified before reading
     * continues.
     */
    private static class ResumableHttpInputStream extends InputStream {

        private final String uri;
        private final byte[] singleByte = new byte[1];

        private CloseableHttpResponse response;
        private InputStream stream;
        private long position;
        private long contentLength = -1L;
        private long currentResponseEndExclusive = Long.MAX_VALUE;
        private String strongEtag;
        private boolean identityEncoded;
        private int resumeAttempts;
        private boolean closed;
        private IOException terminalFailure;
        private final MessageDigest deliveredDigest = sha256();

        private ResumableHttpInputStream(String uri) throws IOException {
            this.uri = uri;
            openInitialResponse();
        }

        @Override
        public int read() throws IOException {
            int bytesRead = read(singleByte, 0, 1);
            return bytesRead < 0 ? -1 : singleByte[0] & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            if (closed) {
                throw new IOException("HTTP response stream is closed.");
            }
            if (terminalFailure != null) {
                throw new IOException(terminalFailure.getMessage());
            }
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            if (offset < 0 || length < 0 || length > bytes.length - offset) {
                throw new IndexOutOfBoundsException();
            }
            if (length == 0) {
                return 0;
            }

            while (true) {
                try {
                    if (position == currentResponseEndExclusive) {
                        if (contentLength >= 0 && position < contentLength) {
                            resumeOrFail("range response ended before the complete resource");
                            continue;
                        }
                        return -1;
                    }

                    int readLength =
                            (int)
                                    Math.min(
                                            length,
                                            Math.min(
                                                    Integer.MAX_VALUE,
                                                    currentResponseEndExclusive - position));
                    int bytesRead = stream.read(bytes, offset, readLength);
                    if (bytesRead > 0) {
                        if (strongEtag == null) {
                            deliveredDigest.update(bytes, offset, bytesRead);
                        }
                        position += bytesRead;
                        if (contentLength >= 0 && position > contentLength) {
                            throw fail("response body exceeded its declared length");
                        }
                        return bytesRead;
                    }
                    if (bytesRead < 0 && contentLength >= 0 && position < contentLength) {
                        resumeOrFail("response body ended before its declared length");
                        continue;
                    }
                    return bytesRead;
                } catch (ConnectionClosedException | TruncatedChunkException e) {
                    resumeOrFail("response body was closed before it was fully consumed");
                }
            }
        }

        @Override
        public int available() throws IOException {
            if (closed) {
                return 0;
            }
            if (terminalFailure != null) {
                throw new IOException(terminalFailure.getMessage());
            }
            return stream == null ? 0 : stream.available();
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                closeCurrentResponse();
            }
        }

        private void openInitialResponse() throws IOException {
            HttpGet request = newBodyGet(uri);
            CloseableHttpResponse newResponse = execute(request, uri);
            boolean accepted = false;
            try {
                if (newResponse.getCode() == HttpStatus.SC_NOT_ACCEPTABLE) {
                    closeQuietly(newResponse);
                    accepted = true;
                    openContentDecodedResponse();
                    return;
                }
                if (newResponse.getCode() != HttpStatus.SC_OK) {
                    throw httpError(newResponse.getCode());
                }

                HttpEntity entity = requireEntity(newResponse);
                if (entity instanceof DecompressingEntity || !isIdentityEncoded(newResponse)) {
                    response = newResponse;
                    stream = entity.getContent();
                    contentLength =
                            entity instanceof DecompressingEntity ? -1L : entity.getContentLength();
                    currentResponseEndExclusive =
                            contentLength < 0 ? Long.MAX_VALUE : contentLength;
                    strongEtag = null;
                    identityEncoded = false;
                    accepted = true;
                    return;
                }

                response = newResponse;
                stream = entity.getContent();
                contentLength = entity.getContentLength();
                currentResponseEndExclusive = contentLength < 0 ? Long.MAX_VALUE : contentLength;
                strongEtag = responseStrongEtag(newResponse);
                identityEncoded = true;
                accepted = true;
            } finally {
                if (!accepted) {
                    closeQuietly(newResponse);
                }
            }
        }

        private void resumeOrFail(String reason) throws IOException {
            try {
                resume(reason);
            } catch (IOException e) {
                terminalFailure = e;
                discardCurrentResponse();
                throw e;
            } catch (RuntimeException e) {
                Integer statusCode = getHttpStatusCode(e);
                terminalFailure =
                        readFailure(
                                statusCode == null
                                        ? "response restart failed"
                                        : "server returned HTTP "
                                                + statusCode
                                                + " while restarting the response");
                discardCurrentResponse();
                throw terminalFailure;
            }
        }

        private void resume(String reason) throws IOException {
            if (resumeAttempts >= MAX_BODY_RESUME_ATTEMPTS) {
                throw readFailure(
                        reason + " after " + MAX_BODY_RESUME_ATTEMPTS + " resume attempts");
            }
            resumeAttempts++;
            discardCurrentResponse();

            if (position == 0) {
                openInitialResponse();
                return;
            }
            if (!identityEncoded) {
                throw readFailure("encoded response bodies cannot be resumed safely");
            }
            if (strongEtag == null) {
                replayFromStart();
                return;
            }

            HttpGet request = newBodyGet(uri);
            request.addHeader(HttpHeaders.RANGE, "bytes=" + position + "-");
            request.addHeader(HttpHeaders.IF_RANGE, strongEtag);

            CloseableHttpResponse newResponse = execute(request, uri);
            boolean accepted = false;
            try {
                if (newResponse.getCode() != HttpStatus.SC_PARTIAL_CONTENT) {
                    throw readFailure(
                            "server did not honor the range request (HTTP "
                                    + newResponse.getCode()
                                    + ")");
                }

                Range range = parseContentRange(newResponse);
                if (range.start != position) {
                    throw readFailure(
                            "server resumed at byte " + range.start + " instead of " + position);
                }
                if (contentLength >= 0 && range.total != contentLength) {
                    throw readFailure(
                            "resource length changed from " + contentLength + " to " + range.total);
                }
                if (contentLength < 0) {
                    contentLength = range.total;
                }

                HttpEntity entity = requireEntity(newResponse);
                long rangeLength = range.end - range.start + 1;
                if (entity instanceof DecompressingEntity || !isIdentityEncoded(newResponse)) {
                    throw readFailure("range response uses a content encoding");
                }
                if (entity.getContentLength() >= 0 && entity.getContentLength() != rangeLength) {
                    throw readFailure("range response length does not match Content-Range");
                }

                if (hasDifferentStrongEtag(newResponse, strongEtag)) {
                    throw readFailure("strong ETag changed while resuming");
                }

                response = newResponse;
                stream = entity.getContent();
                currentResponseEndExclusive = range.end + 1;
                accepted = true;
            } finally {
                if (!accepted) {
                    closeQuietly(newResponse);
                }
            }
        }

        /**
         * Replays a response without a strong ETag from byte zero and verifies that its prefix is
         * identical to the bytes already returned. Once the prefix matches, continuing with the
         * same response cannot combine bytes from two different representations.
         */
        private void replayFromStart() throws IOException {
            byte[] expectedPrefixDigest = digestSnapshot(deliveredDigest);
            while (true) {
                HttpGet request = newBodyGet(uri);
                CloseableHttpResponse newResponse = execute(request, uri);
                boolean accepted = false;
                try {
                    if (newResponse.getCode() != HttpStatus.SC_OK) {
                        throw readFailure(
                                "server returned HTTP "
                                        + newResponse.getCode()
                                        + " while replaying the response");
                    }
                    HttpEntity entity = requireEntity(newResponse);
                    if (entity instanceof DecompressingEntity || !isIdentityEncoded(newResponse)) {
                        throw readFailure("replayed response uses a content encoding");
                    }
                    long replayedLength = entity.getContentLength();
                    if (contentLength >= 0
                            && replayedLength >= 0
                            && contentLength != replayedLength) {
                        throw readFailure(
                                "resource length changed from "
                                        + contentLength
                                        + " to "
                                        + replayedLength);
                    }

                    InputStream newStream = entity.getContent();
                    verifyReplayedPrefix(newStream, expectedPrefixDigest);
                    contentLength = replayedLength;
                    response = newResponse;
                    stream = newStream;
                    currentResponseEndExclusive =
                            replayedLength < 0 ? Long.MAX_VALUE : replayedLength;
                    strongEtag = responseStrongEtag(newResponse);
                    identityEncoded = true;
                    accepted = true;
                    return;
                } catch (ConnectionClosedException | TruncatedChunkException e) {
                    if (resumeAttempts >= MAX_BODY_RESUME_ATTEMPTS) {
                        throw readFailure(
                                "response replay failed after "
                                        + MAX_BODY_RESUME_ATTEMPTS
                                        + " resume attempts");
                    }
                    resumeAttempts++;
                } finally {
                    if (!accepted) {
                        closeQuietly(newResponse);
                    }
                }
            }
        }

        private void verifyReplayedPrefix(InputStream newStream, byte[] expectedPrefixDigest)
                throws IOException {
            MessageDigest replayedDigest = sha256();
            byte[] buffer = new byte[8192];
            long remaining = position;
            while (remaining > 0) {
                int bytesRead = newStream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (bytesRead < 0) {
                    throw new ConnectionClosedException(
                            "Response ended before the previously delivered prefix.");
                }
                if (bytesRead == 0) {
                    throw new IOException("HTTP response returned zero bytes while replaying.");
                }
                replayedDigest.update(buffer, 0, bytesRead);
                remaining -= bytesRead;
            }
            if (!MessageDigest.isEqual(expectedPrefixDigest, replayedDigest.digest())) {
                throw readFailure("resource content changed while replaying the response");
            }
        }

        /**
         * Preserves the old transparent content-decoding behavior for a server which ignores the
         * identity request. Encoded response bodies cannot use byte-offset recovery because their
         * decoded positions do not match wire byte ranges.
         */
        private void openContentDecodedResponse() throws IOException {
            HttpGet request = newHttpGet(uri);
            CloseableHttpResponse newResponse = execute(request, uri);
            boolean accepted = false;
            try {
                if (newResponse.getCode() != HttpStatus.SC_OK) {
                    throw httpError(newResponse.getCode());
                }

                HttpEntity entity = requireEntity(newResponse);
                response = newResponse;
                stream = entity.getContent();
                contentLength = -1L;
                currentResponseEndExclusive = Long.MAX_VALUE;
                strongEtag = null;
                identityEncoded = false;
                accepted = true;
            } finally {
                if (!accepted) {
                    closeQuietly(newResponse);
                }
            }
        }

        private void closeCurrentResponse() throws IOException {
            stream = null;
            if (response != null) {
                try {
                    response.close();
                } finally {
                    response = null;
                }
            }
        }

        private void discardCurrentResponse() {
            try {
                closeCurrentResponse();
            } catch (IOException ignored) {
                // The response is being discarded precisely because its body is incomplete.
            }
        }

        private IOException readFailure(String reason) {
            return new IOException(
                    "Failed to resume HTTP response for uri: "
                            + SensitiveConfigUtils.sanitizeUri(uri)
                            + "; position="
                            + position
                            + ", contentLength="
                            + contentLength
                            + ", recoveryAttempts="
                            + resumeAttempts
                            + "; "
                            + reason);
        }

        private IOException fail(String reason) {
            terminalFailure = readFailure(reason);
            discardCurrentResponse();
            return terminalFailure;
        }
    }

    private static HttpEntity requireEntity(CloseableHttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new IOException("HTTP response has no entity.");
        }
        return entity;
    }

    private static HttpGet newBodyGet(String uri) {
        HttpGet request = newHttpGet(uri);
        request.addHeader(HttpHeaders.ACCEPT_ENCODING, "identity");
        return request;
    }

    private static String responseStrongEtag(CloseableHttpResponse response) {
        Header etag = response.getFirstHeader(HttpHeaders.ETAG);
        if (etag == null || etag.getValue() == null) {
            return null;
        }

        String value = etag.getValue().trim();
        return value.length() >= 2
                        && value.charAt(0) == '"'
                        && value.charAt(value.length() - 1) == '"'
                        && value.indexOf('"', 1) == value.length() - 1
                ? value
                : null;
    }

    private static boolean hasDifferentStrongEtag(
            CloseableHttpResponse response, String expectedStrongEtag) {
        Header etag = response.getFirstHeader(HttpHeaders.ETAG);
        return etag != null && !expectedStrongEtag.equals(responseStrongEtag(response));
    }

    private static boolean isIdentityEncoded(CloseableHttpResponse response) {
        Header contentEncoding = response.getFirstHeader(HttpHeaders.CONTENT_ENCODING);
        return contentEncoding == null
                || "identity".equalsIgnoreCase(contentEncoding.getValue().trim());
    }

    private static void closeQuietly(CloseableHttpResponse response) {
        try {
            response.close();
        } catch (IOException ignored) {
            // Best effort cleanup while preserving the original failure.
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    private static byte[] digestSnapshot(MessageDigest digest) throws IOException {
        try {
            return ((MessageDigest) digest.clone()).digest();
        } catch (CloneNotSupportedException e) {
            throw new IOException("SHA-256 digest cannot be cloned.");
        }
    }

    private static Range parseContentRange(CloseableHttpResponse response) throws IOException {
        Header header = response.getFirstHeader(HttpHeaders.CONTENT_RANGE);
        Matcher matcher =
                header == null ? null : CONTENT_RANGE_PATTERN.matcher(header.getValue().trim());
        if (matcher == null || !matcher.matches() || "*".equals(matcher.group(3))) {
            throw new IOException("Invalid Content-Range in HTTP resume response.");
        }
        try {
            long start = Long.parseLong(matcher.group(1));
            long end = Long.parseLong(matcher.group(2));
            long total = Long.parseLong(matcher.group(3));
            if (start < 0 || end < start || total <= end) {
                throw new IOException("Invalid Content-Range in HTTP resume response.");
            }
            return new Range(start, end, total);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid Content-Range in HTTP resume response.");
        }
    }

    private static class Range {

        private final long start;
        private final long end;
        private final long total;

        private Range(long start, long end, long total) {
            this.start = start;
            this.end = end;
            this.total = total;
        }
    }
}
