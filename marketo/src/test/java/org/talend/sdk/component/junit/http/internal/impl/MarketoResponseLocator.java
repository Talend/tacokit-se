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
package org.talend.sdk.component.junit.http.internal.impl;

import java.util.function.Predicate;

import org.talend.sdk.component.junit.http.api.Request;

import lombok.extern.slf4j.Slf4j;

import static java.util.Optional.ofNullable;

@Slf4j
public class MarketoResponseLocator extends DefaultResponseLocator {

    public static final String MARKETO_FIND_PATTERN_IDENTITY_OAUTH_TOKEN = ".*/identity/oauth/token.*";

    public static final String MARKTO_REPLACE_IDENTITY_OAUTH_TOKEN = "/identity/oauth/token.*";

    public static final String PREFIX = "talend/testing/http/";

    private String test;

    public MarketoResponseLocator() {
        super(PREFIX, "");
    }

    @Override
    protected boolean matches(final Request request, final RequestModel model, final boolean exact,
            final Predicate<String> headerFilter) {
        final String method = ofNullable(model.getMethod()).orElse("GET");
        final String requestUri = request.uri();
        boolean uriMatches;
        if (requestUri.matches(MARKETO_FIND_PATTERN_IDENTITY_OAUTH_TOKEN)) {
            uriMatches = true;
            log.debug("[MarketoResponseLocator#matches] [{}] Checking URI: {}.", uriMatches, requestUri);
        } else {
            uriMatches = requestUri.equals(model.getUri());
        }

        boolean headLineMatches = uriMatches && request.method().equalsIgnoreCase(method);
        final String payload = request.payload();
        final boolean headersMatch = doesHeadersMatch(request, model, headerFilter);
        if (headLineMatches && headersMatch && (model.getPayload() == null || model.getPayload().equals(payload))) {
            return true;
        } else if (exact) {
            return false;
        }

        if (log.isDebugEnabled()) {
            log.debug("Matching test: {} for {}", request, model);
        }

        if (!headLineMatches && requestUri.contains("?")) { // strip the query
            headLineMatches = requestUri.substring(0, requestUri.indexOf('?')).equals(model.getUri())
                    && request.method().equalsIgnoreCase(method);
        }

        return headLineMatches && headersMatch && (model.getPayload() == null
                || (payload != null && (payload.matches(model.getPayload()) || payload.equals(model.getPayload()))));
    }

    @Override
    public void addModel(final Model model) {
        if (model.getRequest().getUri().matches(MARKETO_FIND_PATTERN_IDENTITY_OAUTH_TOKEN)) {
            model.getRequest()
                    .setUri(model.getRequest().getUri().replaceAll(MARKTO_REPLACE_IDENTITY_OAUTH_TOKEN, "/identity/oauth/token"));
        }
        getCapturingBuffer().add(model);
    }

}
