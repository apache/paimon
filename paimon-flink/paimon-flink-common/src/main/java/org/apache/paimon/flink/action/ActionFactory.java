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

package org.apache.paimon.flink.action;

import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Factory to create {@link Action}. */
public interface ActionFactory extends Factory {

    Logger LOG = LoggerFactory.getLogger(ActionFactory.class);

    Optional<Action> create(MultipleParameterTool params);

    static Optional<Action> createAction(String[] args) {
        String action = args[0].toLowerCase();
        String[] actionArgs = Arrays.copyOfRange(args, 1, args.length);
        ActionFactory actionFactory;
        try {
            actionFactory =
                    FactoryUtil.discoverFactory(
                            Thread.currentThread().getContextClassLoader(),
                            ActionFactory.class,
                            action);
        } catch (FactoryException e) {
            printDefaultHelp();
            throw new UnsupportedOperationException("Unknown action \"" + action + "\".");
        }

        LOG.info("{} job args: {}", actionFactory.identifier(), String.join(" ", actionArgs));

        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        if (params.has("help")) {
            actionFactory.printHelp();
            return Optional.empty();
        }

        return actionFactory.create(params);
    }

    void printHelp();

    static void printDefaultHelp() {
        System.out.println("Usage: <action> [OPTIONS]");
        System.out.println();

        System.out.println("Available actions:");
        List<String> identifiers =
                FactoryUtil.discoverIdentifiers(
                        Thread.currentThread().getContextClassLoader(), ActionFactory.class);
        identifiers.forEach(action -> System.out.println("  " + action));
        System.out.println("For detailed options of each action, run <action> --help");
    }
}
