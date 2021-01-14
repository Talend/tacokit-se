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
package org.talend.components.rest.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.talend.components.common.service.http.RedirectContext;
import org.talend.components.common.service.http.RedirectService;
import org.talend.components.common.service.http.ValidateSites;
import org.talend.components.common.service.http.common.UserNamePassword;
import org.talend.components.common.service.http.digest.DigestAuthContext;
import org.talend.components.common.service.http.digest.DigestAuthService;
import org.talend.components.common.text.Substitutor;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.service.client.Body;
import org.talend.components.rest.service.client.Client;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointerFactory;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toMap;

@Slf4j
@Data
@Service
public class RestService {

    private final static String PARAMETERS_SUBSTITUTOR_PREFIX = System
            .getProperty("org.talend.components.rest.parameters_substitutor_prefix", "{");

    private final static String PARAMETERS_SUBSTITUTOR_SUFFIX = System
            .getProperty("org.talend.components.rest.parameters_substitutor_suffix", "}");

    private final static String BODY_SUBSTITUTOR_PREFIX = System.getProperty("org.talend.components.rest.body_substitutor_prefix",
            "${");

    private final static String BODY_SUBSTITUTOR_SUFFIX = System.getProperty("org.talend.components.rest.body_substitutor_suffix",
            "}");

    private final Substitutor.KeyFinder parameterFinder = new Substitutor.KeyFinder(RestService.PARAMETERS_SUBSTITUTOR_PREFIX,
            RestService.PARAMETERS_SUBSTITUTOR_SUFFIX);

    private final Substitutor.KeyFinder bodyFinder = new Substitutor.KeyFinder(RestService.BODY_SUBSTITUTOR_PREFIX,
            RestService.BODY_SUBSTITUTOR_SUFFIX);

    private final Substitutor.KeyFinder pathParamFinder = new Substitutor.KeyFinder("{", "}");

    @Service
    Client client;

    @Service
    private I18n i18n;

    @Service
    private RecordPointerFactory recordPointerFactory;

    public Response<InputStream> execute(final RequestConfig config, final Record record) {
        return _execute(config, record);
    }

    public Response<InputStream> execute(final RequestConfig config) {
        return _execute(config, null);
    }

    private Response<InputStream> _execute(final RequestConfig config, final Record record) {
        final RecordDictionary dictionary = new RecordDictionary(record, recordPointerFactory);
        final Substitutor substitutor = new Substitutor(parameterFinder, dictionary);

        // Check if there are some duplicate keys in given parameters
        if (!hasNoDuplicates(config.getDataset().getHeaders())) {
            throw new IllegalStateException(i18n.duplicateKeys(i18n.headers()));
        }
        if (!hasNoDuplicates(config.getDataset().getQueryParams())) {
            throw new IllegalStateException(i18n.duplicateKeys(i18n.queryParameters()));
        }
        if (!hasNoDuplicates(config.getDataset().getPathParams())) {
            throw new IllegalStateException(i18n.duplicateKeys(i18n.pathParameters()));
        }
        if (config.getDataset().getBody() != null && !hasNoDuplicates(config.getDataset().getBody().getParams())) {
            throw new IllegalStateException(i18n.duplicateKeys(i18n.bodyParameters()));
        }

        final Map<String, String> headers = updateParamsFromRecord(config.headers(), substitutor);
        final Map<String, String> queryParams = updateParamsFromRecord(config.queryParams(), substitutor);
        final Map<String, String> pathParams = updateParamsFromRecord(config.pathParams(), substitutor);

        // I set another prefix '${' to have placeholder in a json body without having to
        // escape all normal '{' of the json
        final Substitutor bodySubstitutor = new Substitutor(bodyFinder, substitutor.getPlaceholderProvider());

        // Has body has to be checked here to set body = null if needed, the body encoder should not return null
        Body body = config.getDataset().isHasBody() ? new Body(config, bodySubstitutor) : null;

        RedirectContext redirectContext = new RedirectContext(config.getDataset().getDatastore().getBase(),
                config.getDataset().getMaxRedirect(), config.getDataset().isForce_302_redirect(),
                config.getDataset().getMethodType().name(), config.getDataset().isOnly_same_host());

        return this.call(config, headers, queryParams, body, this.buildUrl(config, pathParams), redirectContext);
    }

    private Response<InputStream> call(final RequestConfig config, final Map<String, String> headers,
            final Map<String, String> queryParams, final Body body, final String surl,
            final RedirectContext previousRedirectContext) {

        Response<InputStream> resp = null;

        final Authentication authentication = config.getDataset().getDatastore().getAuthentication();
        log.info(i18n.request(config.getDataset().getMethodType().name(), surl, authentication.getType().toString()));

        try {
            if (authentication.getType() == Authorization.AuthorizationType.Digest) {
                try {
                    URL url = new URL(surl);
                    DigestAuthService das = new DigestAuthService();
                    DigestAuthContext context = new DigestAuthContext(url.getPath(), config.getDataset().getMethodType().name(),
                            url.getHost(), url.getPort(), body == null ? null : body.getContent(), new UserNamePassword(
                                    authentication.getBasic().getUsername(), authentication.getBasic().getPassword()));
                    resp = das.call(context, () -> client.executeWithDigestAuth(i18n, context, config, client,
                            previousRedirectContext.getMethod(), surl, headers, queryParams, body));
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(i18n.malformedURL(surl, e.getMessage()));
                }
            } else if (authentication.getType() == Authorization.AuthorizationType.Basic) {
                UserNamePassword credential = new UserNamePassword(authentication.getBasic().getUsername(),
                        authentication.getBasic().getPassword());
                resp = client.executeWithBasicAuth(i18n, credential, config, client, previousRedirectContext.getMethod(), surl,
                        headers, queryParams, body);
            } else if (authentication.getType() == Authorization.AuthorizationType.Bearer) {
                String token = authentication.getBearerToken();
                resp = client.executeWithBearerAuth(i18n, token, config, client, previousRedirectContext.getMethod(), surl,
                        headers, queryParams, body);
            } else {
                resp = client.execute(i18n, config, client, previousRedirectContext.getMethod(), surl, headers, queryParams,
                        body);
            }

            if (config.getDataset().supportRedirect()) {
                // Redirection is managed by RedirectService only if it is not supported by underlying http client implementation
                RedirectContext rctx = new RedirectContext(resp, previousRedirectContext);
                RedirectService rs = new RedirectService();
                rctx = rs.call(rctx);

                if (rctx.isRedirect()) {
                    log.debug(i18n.redirect(rctx.getNbRedirect(), rctx.getNextUrl()));
                    resp = this.call(config, headers, queryParams, body, rctx.getNextUrl(), rctx);
                }
            }
        } catch (IllegalStateException e) {
            if (SocketTimeoutException.class.isInstance(e.getCause())) {
                log.error(i18n.timeout(surl, e.getCause().getMessage()));
                throw new IllegalStateException(i18n.timeout(surl, e.getCause().getMessage()), e.getCause());
            } else {
                throw e;
            }
        }

        log.info(i18n.requestStatus(resp.status()));

        return resp;
    }

    public String buildUrl(final RequestConfig config, final Map<String, String> params) {
        String base = config.getDataset().getDatastore().getBase().trim();
        String segments = this.setPathParams(config.getDataset().getResource().trim(), config.getDataset().isHasPathParams(),
                params);

        if (segments.isEmpty()) {
            return base;
        }

        if (base.charAt(base.length() - 1) != '/' && segments.charAt(0) != '/') {
            return base + '/' + segments;
        }

        return base + segments;
    }

    public String setPathParams(final String resource, final boolean hasPathParams, final Map<String, String> params) {
        if (!hasPathParams) {
            return resource;
        }

        return new Substitutor(pathParamFinder, params::get).replace(resource);
    }

    public Map<String, String> updateParamsFromRecord(final Map<String, String> params, final Substitutor substitutor) {
        return params.entrySet().stream().collect(toMap(e -> e.getKey(), e -> substitute(e.getValue(), substitutor)));
    }

    private String substitute(final String value, final Substitutor substitutor) {
        return substitutor.replace(value);
    }

    public void checkBaseURL(final String base) {
        if (!ValidateSites.isValidSite(base)) {
            throw new RuntimeException(
                    i18n.notValidAddress(ValidateSites.CAN_ACCESS_LOCAL, ValidateSites.ENABLE_MULTICAST_ACCESS));
        }
    }

    public String getHost(final String baseUrl) throws MalformedURLException {
        final URL url = new URL(baseUrl);
        return url.getProtocol() + "://" + url.getHost() + (url.getPort() > -1 ? ":" + url.getPort() : "");
    }

    /**
     * @param params
     * @return true is no duplicates, false if any duplicates
     */
    public boolean hasNoDuplicates(List<Param> params) {
        if (params == null) {
            return true;
        }

        return params.stream().map(Param::getKey).distinct().count() >= params.size();
    }

}
