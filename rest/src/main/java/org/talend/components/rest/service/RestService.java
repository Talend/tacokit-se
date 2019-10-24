/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
import org.talend.components.common.service.http.RedirectContext;
import org.talend.components.common.service.http.RedirectService;
import org.talend.components.common.service.http.digest.DigestAuthContext;
import org.talend.components.common.service.http.digest.DigestAuthService;
import org.talend.components.common.service.http.UserNamePassword;
import org.talend.components.common.text.Substitutor;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointerFactory;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.spi.JsonProvider;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Slf4j
@Data
@Service
public class RestService {

    final static String DEFAULT_ENCODING = System.getProperty("org.talend.components.rest.default_encoding");

    public final static String HEALTHCHECK = "healthcheck";

    @Service
    Client client;

    @Service
    RecordBuilderFactory recordBuilderFactory;

    @Service
    JsonProvider jsonProvider;

    @Service
    private RecordPointerFactory recordPointerFactory;

    public Record execute(final RequestConfig config, final Record record) {
        return _execute(config, record);
    }

    public Record execute(final RequestConfig config) {
        return _execute(config, null);
    }

    private Record _execute(final RequestConfig config, final Record record) {
        final Substitutor substitutor = new RecordSubstitutor("{", "}", record, recordPointerFactory);

        final Map<String, String> headers = updateParamsFromRecord(config.headers(), substitutor);
        final Map<String, String> queryParams = updateParamsFromRecord(config.queryParams(), substitutor);
        final Map<String, String> pathParams = updateParamsFromRecord(config.pathParams(), substitutor);

        // I set another prefix '${' to have placeholder in a json body without having to
        // escape all normal '{' of the json
        final Substitutor bodySubstitutor = new RecordSubstitutor("${", "}", record, recordPointerFactory,
                substitutor.getCache());

        // Has body has to be check here to set body = null if needed, the body encoder should not return null
        Body body = config.getDataset().isHasBody() ? new Body(config, bodySubstitutor) : null;

        RedirectContext redirectContext = new RedirectContext(config.getDataset().getDatastore().getBase(),
                config.getDataset().getMaxRedirect(), config.getDataset().getForce_302_redirect(),
                config.getDataset().getMethodType().name(), config.getDataset().getOnly_same_host());

        Response<byte[]> resp = this.call(config, headers, queryParams, body, this.buildUrl(config, pathParams), redirectContext);

        return this.buildRecord(resp);
    }

    private Response<byte[]> call(final RequestConfig config, final Map<String, String> headers,
            final Map<String, String> queryParams, final Body body, final String surl,
            final RedirectContext previousRedirectContext) {

        Response<byte[]> resp = null;

        if (config.getDataset().getDatastore().getAuthentication().getType() == Authorization.AuthorizationType.Digest) {
            try {
                URL url = new URL(surl);
                DigestAuthService das = new DigestAuthService();
                DigestAuthContext context = new DigestAuthContext(url.getPath(), config.getDataset().getMethodType().name(),
                        url.getHost(), url.getPort(), body == null ? null : body.getContent(),
                        new UserNamePassword(config.getDataset().getDatastore().getAuthentication().getBasic().getUsername(),
                                config.getDataset().getDatastore().getAuthentication().getBasic().getPassword()));
                resp = das.call(context, () -> client.executeWithDigestAuth(context, config, client,
                        previousRedirectContext.getMethod(), surl, headers, queryParams, body));
            } catch (MalformedURLException e) {
                log.error("Given url '" + surl + "' is malformed.", e);
            }
        } else if (config.getDataset().getDatastore().getAuthentication().getType() == Authorization.AuthorizationType.Basic) {
            UserNamePassword credential = new UserNamePassword(
                    config.getDataset().getDatastore().getAuthentication().getBasic().getUsername(),
                    config.getDataset().getDatastore().getAuthentication().getBasic().getPassword());
            resp = client.executeWithBasicAuth(credential, config, client, previousRedirectContext.getMethod(), surl, headers,
                    queryParams, body);
        } else if (config.getDataset().getDatastore().getAuthentication().getType() == Authorization.AuthorizationType.Bearer) {
            String token = config.getDataset().getDatastore().getAuthentication().getBearerToken();
            resp = client.executeWithBearerAuth(token, config, client, previousRedirectContext.getMethod(), surl, headers,
                    queryParams, body);
        } else {
            resp = client.execute(config, client, previousRedirectContext.getMethod(), surl, headers, queryParams, body);
        }

        if (config.getDataset().supportRedirect()) {
            // Redirection is managed by RedirectService only if it is not supported by underlying http client implementation
            RedirectContext rctx = new RedirectContext(resp, previousRedirectContext);
            RedirectService rs = new RedirectService();
            rctx = rs.call(rctx);

            if (rctx.isRedirect()) {
                resp = this.call(config, headers, queryParams, body, rctx.getNextUrl(), rctx);
            }
        }

        return resp;
    }

    private String buildUrl(final RequestConfig config, final Map<String, String> params) {
        return config.getDataset().getDatastore().getBase() + '/'
                + this.setPathParams(config.getDataset().getResource(), config.getDataset().getHasPathParams(), params);
    }

    public String setPathParams(String resource, boolean hasPathParams, Map<String, String> params) {
        if (!hasPathParams) {
            return resource;
        }

        return new Substitutor("{", "}", params::get).replace(resource);
    }

    public Map<String, String> updateParamsFromRecord(final Map<String, String> params, final Substitutor substitutor) {
        return params.entrySet().stream().collect(toMap(e -> e.getKey(), e -> substitute(e.getValue(), substitutor)));
    }

    private Record buildRecord(Response<byte[]> resp) {
        Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();

        builder.withInt("status", resp.status());

        if (resp.headers() == null) {
            builder.withString("headers", "{}");
        } else {
            Schema.Entry headerKeyEntry = recordBuilderFactory.newEntryBuilder().withName("key").withType(Schema.Type.STRING)
                    .withNullable(false).build();
            Schema.Entry headerValueEntry = recordBuilderFactory.newEntryBuilder().withName("value").withType(Schema.Type.STRING)
                    .withNullable(false).build();
            Schema.Builder schemaBuilderHeader = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
            Schema headerElementSchema = schemaBuilderHeader.withEntry(headerKeyEntry).withEntry(headerValueEntry).build();
            List<Record> headers = resp
                    .headers().entrySet().stream().map(e -> recordBuilderFactory.newRecordBuilder(headerElementSchema)
                            .withString("key", e.getKey()).withString("value", String.join(",", e.getValue())).build())
                    .collect(Collectors.toList());

            Schema.Entry.Builder arrayEntryBuilder = recordBuilderFactory.newEntryBuilder();
            builder.withArray(arrayEntryBuilder.withName("headers").withType(Schema.Type.ARRAY).withNullable(false)
                    .withElementSchema(headerElementSchema).build(), headers);
        }

        String encoding = this.getCharsetName(resp);
        String receivedBody = (encoding == null) ? //
                new String(Optional.ofNullable(resp.body()).orElse(new byte[0])) : //
                new String(Optional.ofNullable(resp.body()).orElse(new byte[0]), Charset.forName(encoding));
        builder.withString("body", receivedBody);

        return builder.build();
    }

    private String substitute(final String value, final Substitutor substitutor) {
        return !value.contains(substitutor.getPrefix()) ? value : substitutor.replace(value);
    }

    @HealthCheck(HEALTHCHECK)
    public HealthCheckStatus healthCheck(@Option final Datastore datastore) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(datastore.getBase()).openConnection();
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                return new HealthCheckStatus(HealthCheckStatus.Status.OK, "url is accessible");
            }

        } catch (IOException e) {
            // do nothing
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.KO, "Can't access url");
    }

    public static String getCharsetName(final Response<byte[]> resp) {
        return getCharsetName(resp.headers());
    }

    public static String getCharsetName(final Map<String, List<String>> headers) {
        return getCharsetName(headers, DEFAULT_ENCODING);
    }

    public static String getCharsetName(final Map<String, List<String>> headers, final String defaultCharsetName) {
        String contentType = Optional.ofNullable(headers.get(ContentType.HEADER_KEY)).filter(h -> !h.isEmpty()).map(h -> h.get(0))
                .orElse(defaultCharsetName);

        if (contentType == null) {
            // can happen if defaultCharsetName == null && ContentType.HEADER_KEY is not present in headers
            return null;
        }

        List<String> values = new ArrayList<>();
        int split = contentType.indexOf(';');
        int previous = 0;
        while (split > 0) {
            values.add(contentType.substring(previous, split).trim());
            previous = split + 1;
            split = contentType.indexOf(';', previous);
        }

        if (previous == 0) {
            values.add(contentType);
        } else {
            values.add(contentType.substring(previous + 1, contentType.length()));
        }

        String encoding = values.stream().filter(h -> h.startsWith(ContentType.CHARSET_KEY))
                .map(h -> h.substring(ContentType.CHARSET_KEY.length())).findFirst().orElse(defaultCharsetName);

        return encoding;
    }

}
