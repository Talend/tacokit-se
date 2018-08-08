// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.sdk.component.junit.http.internal.impl;

import static java.util.Optional.ofNullable;

import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.junit.http.api.Request;

public class MarketoResponseLocator extends DefaultResponseLocator {

    public static final String MARKETO_FIND_PATTERN_IDENTITY_OAUTH_TOKEN = ".*/identity/oauth/token.*";

    public static final String MARKTO_REPLACE_IDENTITY_OAUTH_TOKEN = "/identity/oauth/token.*";

    public static final String PREFIX = "talend/testing/http/";

    private transient static final Logger LOG = LoggerFactory.getLogger(MarketoResponseLocator.class);

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
            LOG.debug("[MarketoResponseLocator#matches] [{}] Checking URI: {}.", uriMatches, requestUri);
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("Matching test: {} for {}", request, model);
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
