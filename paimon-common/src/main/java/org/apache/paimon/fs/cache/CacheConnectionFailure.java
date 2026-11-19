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

package org.apache.paimon.fs.cache;

import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Identifies transport failures without depending on a filesystem SDK or its class loader. */
final class CacheConnectionFailure {

    private CacheConnectionFailure() {}

    static boolean isConnectionFailure(Throwable failure) {
        Set<Throwable> visited =
                Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        if (hasLogicalFailure(failure, visited)) {
            return false;
        }
        return isConnectionFailure(
                failure, Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>()));
    }

    private static boolean hasLogicalFailure(Throwable failure, Set<Throwable> visited) {
        if (failure == null || !visited.add(failure)) {
            return false;
        }
        if (failure instanceof FileNotFoundException
                || failure instanceof NoSuchFileException
                || failure instanceof FileAlreadyExistsException
                || failure instanceof AccessDeniedException
                || failure instanceof SecurityException
                || failure instanceof InterruptedException
                || (failure instanceof InterruptedIOException
                        && !(failure instanceof SocketTimeoutException)
                        && !isConnectTimeout(failure))) {
            return true;
        }
        return hasLogicalFailure(failure.getCause(), visited);
    }

    private static boolean isConnectTimeout(Throwable failure) {
        // Apache HTTP's connect timeout extends InterruptedIOException, not SocketTimeoutException.
        return failure.getClass().getName().endsWith(".ConnectTimeoutException");
    }

    private static boolean isConnectionFailure(Throwable failure, Set<Throwable> visited) {
        if (failure == null || !visited.add(failure)) {
            return false;
        }
        if (failure instanceof SocketException
                || failure instanceof SocketTimeoutException
                || failure instanceof UnknownHostException
                || isConnectTimeout(failure)) {
            return true;
        }
        // Jindo's JNI bridge loses the socket exception and exposes this native error text.
        // Match its transport diagnostic, not arbitrary strings such as "timeout" or "failed".
        String message = failure.getMessage();
        if (message != null
                && (message.startsWith("Failed to connect Http service:")
                        || (message.startsWith("ErrorCode : 2001, ErrorMessage :")
                                && message.contains("Failed to connect Http service:")))) {
            return true;
        }
        if (isConnectionFailure(failure.getCause(), visited)) {
            return true;
        }
        // FileIO.get retains failed loader probes as suppressed exceptions.
        for (Throwable suppressed : failure.getSuppressed()) {
            if (isConnectionFailure(suppressed, visited)) {
                return true;
            }
        }
        return false;
    }
}
