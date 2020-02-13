/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RedirectService {

    // Doesn't exist in java.net.HttpUrlConnection
    public final static int TEMPORARY_REDIRECT = 307;

    public final static int PERMANENT_REDIRECT = 308;

    public final static String LOCATION_HEADER = "Location";

    public RedirectContext call(final RedirectContext context) {
        final int status = context.getResponse().status();

        boolean redirect = false;
        if (status != HttpURLConnection.HTTP_OK && ((status == HttpURLConnection.HTTP_MOVED_TEMP
                || status == HttpURLConnection.HTTP_MOVED_PERM || status == HttpURLConnection.HTTP_SEE_OTHER
                || status == TEMPORARY_REDIRECT || status == PERMANENT_REDIRECT))) {
            redirect = true;
        }

        if (!redirect) {
            context.setNextUrl(null);
            return context;
        }

        // Retrieve redirection url
        Map<String, List<String>> headers = context.getResponse().headers();
        String location = Optional.ofNullable(headers).map(m -> m.get(LOCATION_HEADER)).filter(l -> !l.isEmpty())
                .map(l -> l.get(0)).orElseThrow(() -> new IllegalArgumentException(LOCATION_HEADER
                        + " header is not available after redirection code '" + status + "':\n" + redirectioHistory(context)));

        if (location.isEmpty()) {
            throw new IllegalArgumentException(
                    LOCATION_HEADER + " header is empty after redirection code '" + status + "':\n" + redirectioHistory(context));
        }

        String rawLocation = location;
        if (location.charAt(0) == '/') {
            // is relative
            location = context.getBase() + location;
            rawLocation = "base: '" + context.getBase() + "', location: '" + rawLocation + "'";
        }

        if (!isValidUrl(location)) {
            throw new IllegalArgumentException(LOCATION_HEADER + " header is not valid after redirection code '" + status + "',  "
                    + rawLocation + "':\n" + redirectioHistory(context));
        }

        context.setNewUrlAndIncreaseNbRedirection(location);

        // Force redirect of 303 & 302 with GET method if option is selected
        if (status == HttpURLConnection.HTTP_SEE_OTHER
                || (context.isForceGETOn302() && status == HttpURLConnection.HTTP_MOVED_TEMP)) {
            context.setForceGETMethod();
        }

        // If changing domain is forbidden we check redirection host
        if (context.isOnlySameHost()) {
            try {
                String currentHost = new URL(context.getHistory().get(0).getBase()).getHost();
                String redirectDomain = new URL(context.getNextUrl()).getHost();
                if (!currentHost.equals(redirectDomain)) {
                    throw new IllegalArgumentException("Redirect to another domain is forbidden from '" + currentHost + "' to '"
                            + redirectDomain + "':\nLast one has not been followed:\n" + redirectioHistory(context));
                }
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Can't check if redirect to another domain : " + e.getMessage()
                        + "\nLast one has not been followed:\n" + redirectioHistory(context));
            }
        }

        // Check max redirection (0 no redirection, -1 no bound redirection)
        if (context.getMaxRedirect() >= 0 && context.getNbRedirect() >= context.getMaxRedirect()) {
            throw new IllegalArgumentException("Max redirection reached '" + context.getNbRedirect()
                    + "':\nLast one has not been followed:\n" + redirectioHistory(context));
        }

        return context;
    }

    private boolean isValidUrl(String url) {
        try {
            new URL(url);
        } catch (MalformedURLException e) {
            return false;
        }
        return true;
    }

    private String redirectioHistory(RedirectContext context) {
        StringBuilder sb = new StringBuilder();

        context.getHistory().stream().forEach(r -> {
            sb.append("\tLocation[").append(r.getNbRedirect()).append("] : ").append(r.getMethod()).append(" ")
                    .append(r.getNextUrl()).append("\n");
        });

        return sb.toString();
    }

}
