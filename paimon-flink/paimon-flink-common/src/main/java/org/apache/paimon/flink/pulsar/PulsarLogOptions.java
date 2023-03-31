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

package org.apache.paimon.flink.pulsar;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/** Options for Pulsar log. */
public class PulsarLogOptions {

    // Pulsar client API config prefix.
    public static final String CLIENT_CONFIG_PREFIX = "pulsar.client.";
    // Pulsar admin API config prefix.
    public static final String ADMIN_CONFIG_PREFIX = "pulsar.admin.";
    public static final String CONSUMER_CONFIG_PREFIX = "pulsar.consumer.";
    public static final String SOURCE_CONFIG_PREFIX = "pulsar.source.";

    public static final ConfigOption<String> PULSAR_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "serviceUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Service URL provider for Pulsar service.");

    public static final ConfigOption<String> PULSAR_ADMIN_URL =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "adminUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Pulsar service HTTP URL for the admin endpoint.");

    public static final ConfigOption<String> TOPIC =
            ConfigOptions.key("pulsar.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Topic of this Pulsar table.");


    public static final ConfigOption<String> SUBSCRIPTION_NAME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specify the subscription name for Paimon consumer.");


}
