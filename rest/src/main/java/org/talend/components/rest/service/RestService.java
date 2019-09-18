// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.rest.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonObject;
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

    private final Pattern queryPattern = Pattern.compile("$\\{.+?\\}");

    public Record execute(final RequestConfig config, final Record record) {
        return execute(config, record);
    }

    public Record execute(final RequestConfig config) {
        return _execute(config, null);
    }

    private Record _execute(final RequestConfig config, final Record record) {
        log.info("Execute");
        Map<String, String> headers = config.headers();
        Map<String, String> queryParams = config.queryParams();
        Map<String, String> pathParams = config.pathParams();
        RequestBody body = config.body();

        if (record != null) {
            headers = updateParamsFromRecord(headers, record);
            queryParams = updateParamsFromRecord(queryParams, record);
            pathParams = updateParamsFromRecord(pathParams, record);
        }

        Response<JsonObject> resp = client.execute(config, client, config.getDataset().getMethodType().name(),
                this.buildUrl(config, pathParams), headers, queryParams, body);
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

        return replaceAll(resource, pathPattern, params);
    }

    public Map<String, String> updateParamsFromRecord(final Map<String, String> params, final Record record) {
        Map<String, String> updated = new HashMap<>();
        params.forEach((k, v) -> updated.put(k, updateParamFromRecord(v, record)));

        return updated;
    }

    private String updateParamFromRecord(String param, final Record record) {
        return replaceAll(param, queryPattern, RecordToMap(record));
    }

    private String replaceAll(String input, final Pattern search, Map<String, String> params) {
        // Find parameters
        final List<String> found = new ArrayList<>();
        Matcher matcher = search.matcher(input);

        while (matcher.find()) {
            found.add(matcher.group());
        }

        for (String param : found) {
            String name = param.substring(2).substring(0, param.length() - 3);
            if (!params.containsKey(name)) {
                throw new RuntimeException("No value found for " + param);
            }

            // We have to transform {name} to \{name\} and then replace all occurences with desired value
            input = input.replace(param, params.get(name));
        }

        return input;
    }

    private boolean isSuccess(int code) {
        return code == sun.net.www.protocol.http.HttpURLConnection.HTTP_OK;
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
                // value = record.
                break;
            case ARRAY:
                break;
            case DATETIME:
                break;
            default:
                value = record.getString(e.getName());
            }
            recordAsMap.put(e.getName(), value);
        }

        return recordAsMap;
    }

}
