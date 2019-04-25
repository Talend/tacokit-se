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

import javax.json.JsonObject;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Data
@Service
public class RestService {

    @Service
    Client client;

    @Service
    RecordBuilderFactory recordBuilderFactory;

    public Record execute(final RequestConfig config) {
        Response<JsonObject> resp = client.execute(config, client, config.getDataset().getMethodType().name(),
                this.buildUrl(config), config.headers(), config.queryParams(), config.body());
        return this.handleResponse(resp, config);
    }

    private Record handleResponse(final Response<JsonObject> firstResp, final RequestConfig config) {
        Record rec = null;
        Response<JsonObject> finalResp = redirect(firstResp, config);
        if (isSuccess(finalResp.status())) {
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

    private Response<JsonObject> redirect(final Response<JsonObject> firstResp, final RequestConfig config) {
        // @TODO check https://www.mkyong.com/java/java-httpurlconnection-follow-redirect-example/
        // https://github.com/Talend/tdi-studio-se/blob/master/main/plugins/org.talend.designer.components.localprovider/components/tFileFetch/tFileFetch_main.javajet#L337
        return firstResp;
    }

    private String buildUrl(final RequestConfig config) {
        return config.getDataset().getDatastore().getBase() + '/' + config.getDataset().getResource();
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
            log.debug("[buildRecord] Retrieve " + ContentType.headerValue + " : " + contentTypeStr);
            contentType = ContentType.ContentTypeEnum.get(contentTypeStr);
            // @TODO better manager content/type
            if (contentType == null) {
                log.warn("Unsupported " + ContentType.headerValue + " : " + contentTypeStr);
            }
        } else {
            log.warn("No header content type " + ContentType.headerValue);
        }

        builder.withInt("status", resp.status());

        /*
         * Record.Builder headerBuilder = recordBuilderFactory.newRecordBuilder();
         * resp.headers().forEach((k, v) -> headerBuilder.withString(k, String.join(",", v)));
         * 
         * Record r = headerBuilder.build();
         * Schema.Entry e = entryBuilder.withName("headers").withType(Schema.Type.RECORD).withNullable(false)
         * .withElementSchema(r.getSchema()).build();
         * builder.withRecord(e, r);
         */

        Schema.Entry headerKeyEntry = recordBuilderFactory.newEntryBuilder().withName("key").withType(Schema.Type.STRING)
                .withNullable(false).build();
        Schema.Entry headerValueEntry = recordBuilderFactory.newEntryBuilder().withName("value").withType(Schema.Type.STRING)
                .withNullable(false).build();
        Schema.Builder schemaBuilderHeader = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        Schema headerElementSchema = schemaBuilderHeader.withEntry(headerKeyEntry).withEntry(headerValueEntry).build();
        Record.Builder arrayHeaderBuilder = recordBuilderFactory.newRecordBuilder(headerElementSchema);

        // List<Record.Builder> headers = Arrays.asList(arrayHeaderBuilder.withString("key", "TOTOT").withString("value",
        // "TITIT"));
        // resp.headers().forEach((k, v) -> headerBuilder.withString(k, String.join(",", v)));
        List<Record> headers = resp.headers().entrySet().stream().map(
                e -> arrayHeaderBuilder.withString("key", e.getKey()).withString("value", String.join(",", e.getValue())).build())
                .collect(Collectors.toList());

        Schema.Entry.Builder arrayEntryBuilder = recordBuilderFactory.newEntryBuilder();
        builder.withArray(arrayEntryBuilder.withName("headers").withType(Schema.Type.ARRAY).withNullable(false)
                .withElementSchema(headerElementSchema).build(), headers);

        builder.withString("body", resp.body().toString());

        return builder.build();
    }

}
