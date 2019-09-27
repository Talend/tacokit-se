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
import org.talend.component.common.service.http.UserNamePassword;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonObject;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

@Slf4j
@Data
@Service
public class RestService {

    public final static String HEALTHCHECK = "healthcheck";

    /*
     * private final static int[] REDIRECT_CODES = new int[]{HttpURLConnection.HTTP_MOVED_TEMP, HttpURLConnection.HTTP_MOVED_PERM,
     * HttpURLConnection.HTTP_SEE_OTHER};
     * static{
     * Arrays.sort(REDIRECT_CODES);
     * }
     */

    @Service
    Client client;

    @Service
    RecordBuilderFactory recordBuilderFactory;

    // Pattern to retrieve {xxxx} in a non-greddy way
    private final Pattern pathPattern = Pattern.compile("\\{.+?\\}");

    private final Pattern queryPattern = Pattern.compile("\\$\\{.+?\\}");

    public Record execute(final RequestConfig config, final Record record) {
        return _execute(config, record);
    }

    public Record execute(final RequestConfig config) {
        return _execute(config, null);
    }

    private Record _execute(final RequestConfig config, final Record record) {
        log.info("Execute");
        final Map<String, String> headers = updateParamsFromRecord(config.headers(), record);
        final Map<String, String> queryParams = updateParamsFromRecord(config.queryParams(), record);
        final Map<String, String> pathParams = updateParamsFromRecord(config.pathParams(), record);
        RequestBody body = config.body();

        Response<JsonObject> resp = null;
        if(config.getDataset().getAuthentication().getType() == Authorization.AuthorizationType.Digest){
            DigestAuthService das = new DigestAuthService();
            das.call(new UserNamePassword(config.getDataset().getAuthentication().getBasic().getUsername(), config.getDataset().getAuthentication().getBasic().getPassword()),() -> client.executeWithDigestAuth(config, client, config.getDataset().getMethodType().name(),
                    this.buildUrl(config, pathParams), headers, queryParams, body));
        }
        else {
            resp = client.execute(config, client, config.getDataset().getMethodType().name(),
                    this.buildUrl(config, pathParams), headers, queryParams, body);
        }

        return this.handleResponse(resp, config);
    }

    private Record handleResponse(final Response<JsonObject> firstResp, final RequestConfig config) {
        Record rec = null;

        Response<JsonObject> finalResp = redirect(firstResp, config);

        if (!config.isStopIfNotOk() || (config.isStopIfNotOk() && isSuccess(finalResp.status()))) {
            rec = buildRecord(finalResp);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("Rest response error [").append(finalResp.status()).append("]");
            String msg = sb.toString();
            log.debug(msg);
            throw new RuntimeException(msg);
        }

        return rec;
    }

    private Response<JsonObject> redirect(final Response<JsonObject> previousResp, final RequestConfig config) {
        // @TODO check https://www.mkyong.com/java/java-httpurlconnection-follow-redirect-example/
        // https://github.com/Talend/tdi-studio-se/blob/master/main/plugins/org.talend.designer.components.localprovider/components/tFileFetch/tFileFetch_main.javajet#L337

        // HTTP client auto-redirect

        /*
         * boolean redirect = Arrays.binarySearch(REDIRECT_CODES, previousResp.status()) != -1;
         *
         * if(redirect){
         * String location = previousResp.headers().get("Location").get(0);
         * }
         */

        return previousResp;
    }

    private String buildUrl(final RequestConfig config, Map<String, String> params) {
        return config.getDataset().getDatastore().getBase() + '/'
                + this.setPathParams(config.getDataset().getResource(), config.getDataset().getHasPathParams(), params);
    }

    public String setPathParams(String resource, boolean hasPathParams, Map<String, String> params) {
        if (!hasPathParams) {
            return resource;
        }

        return replaceAll(resource, pathPattern, 1, params);
    }

    public Map<String, String> updateParamsFromRecord(final Map<String, String> params, final Record record) {
        Map<String, String> updated = new HashMap<>();
        params.forEach((k, v) -> updated.put(k, updateParamFromRecord(v, record)));

        return updated;
    }

    private String updateParamFromRecord(String param, final Record record) {
        if(record == null){
            return param;
        }

        return replaceAll(param, queryPattern, 2, RecordToMap(record));
    }

    private String replaceAll(String input, final Pattern search, final int substrStart, Map<String, String> params) {
        // Find parameters
        final List<String> found = new ArrayList<>();
        Matcher matcher = search.matcher(input);

        while (matcher.find()) {
            found.add(matcher.group());
        }

        for (String param : found) {
            String name = param.substring(substrStart).substring(0, param.length() - substrStart - 1);
            if (params.containsKey(name)) {
                // We have to transform {name} to \{name\} and then replace all occurences with desired value
                input = input.replace(param, params.get(name));
            }
        }

        return input;
    }

    private boolean isSuccess(int code) {
        return code == HttpURLConnection.HTTP_OK;
    }

    private Record buildRecord(Response<JsonObject> resp) {
        Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();

        ContentType.ContentTypeEnum contentType = null;
        if (resp.headers().containsKey(ContentType.headerValue)) {
            String contentTypeStr = resp.headers().get(ContentType.headerValue).get(0);

            String encoding = "UTF-8";
            if (contentTypeStr.indexOf(';') > 1) {
                String[] split = contentTypeStr.split(";");
                contentTypeStr = split[0];
                encoding = split[1];
            }

            log.debug("[buildRecord] Retrieve " + ContentType.headerValue + " : " + contentTypeStr + ", with encoding : "
                    + encoding);
            contentType = ContentType.ContentTypeEnum.get(contentTypeStr);
            // @TODO better manager content/type
            if (contentType == null) {
                log.warn("Unsupported " + ContentType.headerValue + " : " + contentTypeStr);
            }
        } else {
            log.warn("No header content type " + ContentType.headerValue);
        }

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

        if (resp.body() == null) {
            builder.withString("body", "{}");
        } else {
            builder.withString("body", resp.body().toString());
        }

        return builder.build();
    }

    private static Map<String, String> RecordToMap(Record record) {
        Map<String, String> recordAsMap = new HashMap<>();
        Set<String> keys = record.getSchema().getEntries().stream().map(Schema.Entry::getName).collect(toSet());

        for (Schema.Entry e : record.getSchema().getEntries()) {
            String value = "undefined";
            switch (e.getType()) {
            case RECORD:
                // Maybe convert to json
                throw new RuntimeException("Unsuported");
            case ARRAY:
                // Maybe convert to string
                throw new RuntimeException("Unsuported");
            case STRING:
                value = record.getString(e.getName());
                break;
            case BYTES:
                value = new String(record.getBytes(e.getName()));
                break;
            case INT:
                value = "" + record.getInt(e.getName());
                break;
            case LONG:
                value = "" + record.getLong(e.getName());
                break;
            case FLOAT:
                value = "" + record.getFloat(e.getName());
                break;
            case DOUBLE:
                value = "" + record.getDouble(e.getName());
                break;
            case BOOLEAN:
                value = "" + record.getBoolean(e.getName());
                break;
            case DATETIME:
                break;
            default:
                throw new RuntimeException("Unknown type");
            }
            recordAsMap.put(e.getName(), value);
        }

        return recordAsMap;
    }

    @HealthCheck(HEALTHCHECK)
    public HealthCheckStatus healthCheck(@Option final Datastore datastore) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(datastore.getBase()).openConnection();
            conn.setRequestMethod("HEAD");
            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                return new HealthCheckStatus(HealthCheckStatus.Status.OK, "url is accessible");
            }

        } catch (IOException e) {
            // do nothing
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.KO, "Can't access url");
    }

}
