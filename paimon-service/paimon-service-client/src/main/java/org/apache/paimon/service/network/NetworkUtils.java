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

package org.apache.paimon.service.network;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.LinkedList;

/** Utils for network. */
public class NetworkUtils {

    public static String findHostAddress() throws SocketException, UnknownHostException {
        return findLocalAddress().getHostAddress();
    }

    public static InetAddress findLocalAddress() throws UnknownHostException, SocketException {
        InetAddress localAddress = InetAddress.getLocalHost();
        if (localAddress.isLoopbackAddress()) {
            Enumeration<NetworkInterface> activeNetworkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            LinkedList<NetworkInterface> reversedNetworkInterfaces = new LinkedList<>();
            // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
            // on unix-like system.
            while (activeNetworkInterfaces.hasMoreElements()) {
                reversedNetworkInterfaces.addFirst(activeNetworkInterfaces.nextElement());
            }

            for (NetworkInterface networkInterface : reversedNetworkInterfaces) {
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isLinkLocalAddress() || address.isLoopbackAddress()) {
                        continue;
                    }

                    // because of Inet6Address.toHostName may add interface at the end if it knows
                    // about it
                    if (!(address instanceof Inet4Address)) {
                        continue;
                    }

                    return InetAddress.getByAddress(address.getAddress());
                }
            }
        }
        return localAddress;
    }
}
