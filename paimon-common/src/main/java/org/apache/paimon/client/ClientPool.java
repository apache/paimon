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

package org.apache.paimon.client;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/** Client pool for using multiple clients to execute actions. */
public interface ClientPool<C, E extends Exception> {
    /** Action interface with return object for client. */
    interface Action<R, C, E extends Exception> {
        R run(C client) throws E;
    }

    /** Action interface with return void for client. */
    interface ExecuteAction<C, E extends Exception> {
        void run(C client) throws E;
    }

    <R> R run(Action<R, C, E> action) throws E, InterruptedException;

    void execute(ExecuteAction<C, E> action) throws E, InterruptedException;

    /** Default implementation for {@link ClientPool}. */
    abstract class ClientPoolImpl<C, E extends Exception> implements Closeable, ClientPool<C, E> {

        private volatile LinkedBlockingQueue<C> clients;

        protected ClientPoolImpl(int poolSize, Supplier<C> supplier) {
            this.clients = new LinkedBlockingQueue<>();
            for (int i = 0; i < poolSize; i++) {
                this.clients.add(supplier.get());
            }
        }

        @Override
        public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
            C client = this.clients.take();
            try {
                return action.run(client);
            } finally {
                this.clients.add(client);
            }
        }

        @Override
        public void execute(ExecuteAction<C, E> action) throws E, InterruptedException {
            run(
                    (Action<Void, C, E>)
                            client -> {
                                action.run(client);
                                return null;
                            });
        }

        protected abstract void close(C client);

        @Override
        public void close() {
            LinkedBlockingQueue<C> clients = this.clients;
            this.clients = null;
            if (clients != null) {
                List<C> drain = new ArrayList<>();
                clients.drainTo(drain);
                drain.forEach(this::close);
            }
        }
    }
}
