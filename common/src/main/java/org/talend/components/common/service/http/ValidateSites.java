/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.common.service.http;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidateSites {

    public final static boolean CAN_ACCESS_LOCAL = Boolean.valueOf(System.getProperty("connectors.enable_local_network_access",
            System.getenv().getOrDefault("CONNECTORS_ENABLE_LOCAL_NETWORK_ACCESS", "true")));

    public final static boolean ENABLE_MULTICAST_ACCESS = Boolean
            .valueOf(System.getProperty("connectors.enable_multicast_network_access",
                    System.getenv().getOrDefault("CONNECTORS_ENABLE_MULTICAST_NETWORK_ACCESS", "true")));

    private final static List<String> ADDITIONAL_LOCAL_HOSTS = Arrays.asList(new String[] { "224.0.0." // local multicast : from
            // 224.0.0.0 to 224.0.0.255
    });

    private ValidateSites() {
    }

    public static boolean isValidSite(final String base) {
        return isValidSite(base, CAN_ACCESS_LOCAL, ENABLE_MULTICAST_ACCESS);
    }

    /**
     * This method returns if the given url is valid depending of paremeter.
     * We can make local sites and multicast class of addresses invalid.
     *
     * @param surl
     * @param can_access_local
     * @param enable_multicast_access
     * @return
     */
    public static boolean isValidSite(final String surl, final boolean can_access_local, final boolean enable_multicast_access) {
        try {
            final URL url = new URL(surl);
            final String host = url.getHost();
            final InetAddress inetAddress = InetAddress.getByName(host);

            if (!enable_multicast_access && inetAddress.isMulticastAddress()) {
                // Multicast addresses are forbidden
                return false;
            }

            if (can_access_local) {
                // we can access local sites
                return true;
            }

            return !inetAddress.isSiteLocalAddress() && !inetAddress.isLoopbackAddress()
                    && !ADDITIONAL_LOCAL_HOSTS.stream().filter(h -> host.contains(h)).findFirst().isPresent();
        } catch (MalformedURLException e) {
            log.error(e.getMessage(), e);
            return false;
        } catch (UnknownHostException e) {
            return true;
        }
    }
}
