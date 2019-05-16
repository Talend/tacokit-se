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
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import sun.net.www.protocol.http.HttpURLConnection;

import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    public Record execute(final RequestConfig config) {
        Response<JsonObject> resp = client.execute(config, client, config.getDataset().getMethodType().name(),
                this.buildUrl(config), config.headers(), config.queryParams(), config.body());
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

    private String buildUrl(final RequestConfig config) {
        return config.getDataset().getDatastore().getBase() + '/' + this.setPathParams(config.getDataset().getResource(),
                config.getDataset().getHasPathParams(), config.pathParams());
    }

    public String setPathParams(String resource, boolean hasPathParams, Map<String, String> params) {
        if (!hasPathParams) {
            return resource;
        }

        // Find parameters
        final List<String> found = new ArrayList<>();
        Matcher matcher = pathPattern.matcher(resource);

        while (matcher.find()) {
            found.add(matcher.group());
        }

        for (String param : found) {
            String name = param.substring(1).substring(0, param.length() - 2);
            if (!params.containsKey(name)) {
                throw new RuntimeException("No value found for path param " + param);
            }

            // We have to transform {name} to \{name\} and then replace all occurences with desired value
            resource = resource.replaceAll("\\" + param.substring(0, param.length() - 1) + "\\}", params.get(name));
        }

        return resource;
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

}
