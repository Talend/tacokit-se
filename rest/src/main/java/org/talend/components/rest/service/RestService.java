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
package org.talend.components.rest.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.common.collections.IteratorComposer;
import org.talend.components.common.collections.IteratorMap;
import org.talend.components.common.service.http.RedirectContext;
import org.talend.components.common.service.http.RedirectService;
import org.talend.components.common.service.http.common.UserNamePassword;
import org.talend.components.common.service.http.digest.DigestAuthContext;
import org.talend.components.common.service.http.digest.DigestAuthService;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.common.stream.format.rawtext.ExtendedRawTextConfiguration;
import org.talend.components.common.stream.format.rawtext.RawTextConfiguration;
import org.talend.components.common.text.Substitutor;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.Format;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.service.client.Body;
import org.talend.components.rest.service.client.Client;
import org.talend.components.rest.service.client.ContentType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointerFactory;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonReaderFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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

    public final static String HEALTHCHECK = "healthcheck";

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

    @Service
    private RecordIORepository ioRepository;

    @Service
    private JsonReaderFactory jsonReaderFactory;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

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

        log.info(i18n.request(config.getDataset().getMethodType().name(), surl,
                config.getDataset().getDatastore().getAuthentication().getType().toString()));

        try {
            if (config.getDataset().getDatastore().getAuthentication().getType() == Authorization.AuthorizationType.Digest) {
                try {
                    URL url = new URL(surl);
                    DigestAuthService das = new DigestAuthService();
                    DigestAuthContext context = new DigestAuthContext(url.getPath(), config.getDataset().getMethodType().name(),
                            url.getHost(), url.getPort(), body == null ? null : body.getContent(),
                            new UserNamePassword(config.getDataset().getDatastore().getAuthentication().getBasic().getUsername(),
                                    config.getDataset().getDatastore().getAuthentication().getBasic().getPassword()));
                    resp = das.call(context, () -> client.executeWithDigestAuth(i18n, context, config, client,
                            previousRedirectContext.getMethod(), surl, headers, queryParams, body));
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(i18n.malformedURL(surl, e.getMessage()));
                }
            } else if (config.getDataset().getDatastore().getAuthentication()
                    .getType() == Authorization.AuthorizationType.Basic) {
                UserNamePassword credential = new UserNamePassword(
                        config.getDataset().getDatastore().getAuthentication().getBasic().getUsername(),
                        config.getDataset().getDatastore().getAuthentication().getBasic().getPassword());
                resp = client.executeWithBasicAuth(i18n, credential, config, client, previousRedirectContext.getMethod(), surl,
                        headers, queryParams, body);
            } else if (config.getDataset().getDatastore().getAuthentication()
                    .getType() == Authorization.AuthorizationType.Bearer) {
                String token = config.getDataset().getDatastore().getAuthentication().getBearerToken();
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

    public Iterator<Record> buildFixedRecord(final Response<InputStream> resp, final boolean isCompletePayload,
            final Format format) {
        int status = resp.status();
        log.info(i18n.requestStatus(status));

        Map<String, String> headers = Optional.ofNullable(resp.headers()).orElseGet(Collections::emptyMap).entrySet().stream()
                .collect(toMap((Map.Entry<String, List<String>> e) -> e.getKey(), e -> String.join(",", e.getValue())));

        final String encoding = ContentType.getCharsetName(resp.headers());

        final ContentFormat contentFormat = findFormat(headers, format);
        final RecordReaderSupplier recordReaderSupplier = this.ioRepository.findReader(contentFormat.getClass());
        final RecordReader reader = recordReaderSupplier.getReader(recordBuilderFactory, contentFormat,
                new ExtendedRawTextConfiguration(encoding, isCompletePayload));

        final Schema.Entry headersEntry = this.recordBuilderFactory.newEntryBuilder().withName("headers")
                .withType(Schema.Type.ARRAY)
                .withElementSchema(this.recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                        .withEntry(newEntry("key", Schema.Type.STRING)).withEntry(newEntry("value", Schema.Type.STRING)).build())
                .build();

        Schema.Type bodyType = Schema.Type.RECORD;
        if (RawTextConfiguration.class.equals(contentFormat.getClass())) {
            bodyType = Schema.Type.STRING;
        }

        final Schema.Entry statusEntry = this.recordBuilderFactory.newEntryBuilder().withName("status").withType(Schema.Type.INT)
                .build();
        final Schema.Entry.Builder bodyBuilder = this.recordBuilderFactory.newEntryBuilder().withName("body").withType(bodyType)
                .withNullable(true);

        if (Schema.Type.RECORD.equals(bodyType)) {
            // Need a schema, it will be replace by the real one later
            bodyBuilder.withElementSchema(this.recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                    .withEntry(newEntry("tmp", Schema.Type.STRING)).build());
        }

        final Schema.Entry bodyEntry = bodyBuilder.build();
        final Schema.Builder builder = this.recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        if (isCompletePayload) {
            builder.withEntry(statusEntry).withEntry(headersEntry);
        }
        final Schema schema = builder.withEntry(bodyEntry).build();

        final List<Record> headerRecords = headers.entrySet().stream().map(this::convertHeadersToRecords)
                .collect(Collectors.toList());

        return new IteratorMap<Record, Record>(reader.read(resp.body()),
                r -> this.buildRecord(schema, headersEntry, r, status, headerRecords, isCompletePayload), true);
    }

    private Record convertHeadersToRecords(final Map.Entry<String, String> header) {
        return this.recordBuilderFactory.newRecordBuilder().withString("key", header.getKey())
                .withString("value", header.getValue()).build();
    }

    private Record buildRecord(final Schema schema, final Schema.Entry headersEntry, final Record body, final int status,
            final List<Record> headers, final boolean isCompletePayload) {

        final boolean isRawText = schema.getEntries().stream().filter(e -> "body".equals(e.getName())).findFirst().get()
                .getType() == Schema.Type.STRING;

        final Record.Builder bodyBuilder;
        if (isRawText && isCompletePayload) {
            String v = body == null ? null : body.getString("content");
            bodyBuilder = this.recordBuilderFactory.newRecordBuilder(schema).withString("body", v);
            return bodyBuilder.withInt("status", status).withArray(headersEntry, headers).build();

        } else if (!isRawText && isCompletePayload) {
            bodyBuilder = this.recordBuilderFactory.newRecordBuilder(schema).withRecord("body", body);
            return bodyBuilder.withInt("status", status).withArray(headersEntry, headers).build();
        } else if (isRawText && !isCompletePayload) {
            String v = body == null ? null : body.getString("content");
            return this.recordBuilderFactory.newRecordBuilder(schema).withString("body", v).build();
        } else if (!isRawText && !isCompletePayload) {
            return body;
        }

        throw new IllegalStateException("Unsuported record build.");
    }

    private Schema.Entry newEntry(String name, Schema.Type type) {
        return this.recordBuilderFactory.newEntryBuilder().withName(name).withType(type).build();
    }

    private ContentFormat findFormat(final Map<String, String> headers, final Format format) {
        if (format == Format.JSON) {
            JsonConfiguration jsonConfiguration = new JsonConfiguration();
            jsonConfiguration.setJsonPointer("/");
            return jsonConfiguration;
        }

        return new RawTextConfiguration();
    }

    private String substitute(final String value, final Substitutor substitutor) {
        String substitute = substitutor.replace(value);
        return substitute;
    }

    @HealthCheck(HEALTHCHECK)
    public HealthCheckStatus healthCheck(@Option final Datastore datastore) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(datastore.getBase()).openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(datastore.getConnectionTimeout());
            conn.setReadTimeout(datastore.getReadTimeout());
            conn.connect();
            final int status = conn.getResponseCode();
            log.info(i18n.healthCheckStatus(datastore.getBase(), status));
            if (status == HttpURLConnection.HTTP_OK) {
                return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.healthCheckOk());
            }

        } catch (IOException e) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            log.debug(i18n.healthChecException(sw.toString()));
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.healthCheckFailed(datastore.getBase()));
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
