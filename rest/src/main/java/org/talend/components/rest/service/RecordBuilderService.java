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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.common.collections.IteratorMap;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.common.stream.format.rawtext.ExtendedRawTextConfiguration;
import org.talend.components.common.stream.format.rawtext.RawTextConfiguration;
import org.talend.components.rest.configuration.Format;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.client.ContentType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Slf4j
@Service
public class RecordBuilderService {

    @Service
    private I18n i18n;

    @Service
    private RecordIORepository ioRepository;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    public Iterator<Record> buildFixedRecord(final Response<InputStream> resp, final RequestConfig config) {
        final Format format = config.getDataset().getFormat();
        final boolean isCompletePayload = config.getDataset().isCompletePayload();

        Map<String, String> headers = Optional.ofNullable(resp.headers()).orElseGet(Collections::emptyMap).entrySet().stream()
                .collect(toMap((Map.Entry<String, List<String>> e) -> e.getKey(), e -> String.join(",", e.getValue())));

        final String encoding = ContentType.getCharsetName(resp.headers());

        final ContentFormat contentFormat = findFormat(config);
        final RecordReaderSupplier recordReaderSupplier = this.ioRepository.findReader(contentFormat.getClass());
        final RecordReader reader = recordReaderSupplier.getReader(recordBuilderFactory, contentFormat,
                new ExtendedRawTextConfiguration(encoding, isCompletePayload));

        final List<Record> headerRecords = headers.entrySet().stream().map(this::convertHeadersToRecords)
                .collect(Collectors.toList());

        final Iterator<Record> readIterator;

        try {
            readIterator = reader.read(resp.body());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    i18n.invalideBodyContent(format == Format.RAW_TEXT ? i18n.formatText() : i18n.formatJSON(), e.getMessage()));
        }

        return new IteratorMap<Record, Record>(readIterator,
                r -> this.buildRecord(r, resp.status(), headerRecords, isCompletePayload, format), true);
    }

    private ContentFormat findFormat(final RequestConfig config) {
        if (config.getDataset().getFormat() == Format.JSON) {
            JsonConfiguration jsonConfiguration = new JsonConfiguration();
            jsonConfiguration.setJsonPointer("/");
            jsonConfiguration.setForceDouble(config.getDataset().isJsonForceDouble());
            return jsonConfiguration;
        }

        return new RawTextConfiguration();
    }

    private Schema.Entry newEntry(String name, Schema.Type type) {
        return this.recordBuilderFactory.newEntryBuilder().withName(name).withType(type).build();
    }

    private Record convertHeadersToRecords(final Map.Entry<String, String> header) {
        return this.recordBuilderFactory.newRecordBuilder().withString("key", header.getKey())
                .withString("value", header.getValue()).build();
    }

    private Record buildRecord(final Record body, final int status, final List<Record> headers, final boolean isCompletePayload,
            final Format format) {

        final SchemaContainer schema = buildShema(body, isCompletePayload, format);

        final boolean isRawText = schema.getRecordSchema().getEntries().stream().filter(e -> "body".equals(e.getName()))
                .findFirst().get().getType() == Schema.Type.STRING;

        final Record.Builder bodyBuilder;
        if (isRawText && isCompletePayload) {
            String v = body == null ? null : body.getString("content");
            bodyBuilder = this.recordBuilderFactory.newRecordBuilder(schema.getRecordSchema()).withString("body", v);
            return bodyBuilder.withInt("status", status).withArray(schema.getHeadersEntry(), headers).build();
        } else if (!isRawText && isCompletePayload) {
            bodyBuilder = this.recordBuilderFactory.newRecordBuilder(schema.getRecordSchema()).withRecord("body", body);
            return bodyBuilder.withInt("status", status).withArray(schema.getHeadersEntry(), headers).build();
        } else if (isRawText && !isCompletePayload) {
            String v = body == null ? null : body.getString("content");
            return this.recordBuilderFactory.newRecordBuilder(schema.getRecordSchema()).withString("body", v).build();
        } else if (!isRawText && !isCompletePayload) {
            return body;
        }

        throw new IllegalStateException("Unsupported record build.");
    }

    private SchemaContainer buildShema(final Record body, final boolean isCompletePayload, final Format format) {
        final Schema.Entry headersEntry = this.recordBuilderFactory.newEntryBuilder().withName("headers")
                .withType(Schema.Type.ARRAY)
                .withElementSchema(this.recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                        .withEntry(newEntry("key", Schema.Type.STRING)).withEntry(newEntry("value", Schema.Type.STRING)).build())
                .build();

        final Schema.Entry statusEntry = this.recordBuilderFactory.newEntryBuilder().withName("status").withType(Schema.Type.INT)
                .build();

        final Schema.Entry.Builder bodyBuilder = this.recordBuilderFactory.newEntryBuilder().withName("body");
        // If body is null we always return same schema as a RAW_TEXT
        if (format == Format.RAW_TEXT || body == null) {
            bodyBuilder.withType(Schema.Type.STRING).withNullable(true);
        } else {
            final Schema.Builder bodySchema = this.recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
            body.getSchema().getEntries().forEach(e -> bodySchema.withEntry(e));
            bodyBuilder.withType(Schema.Type.RECORD).withElementSchema(bodySchema.build());
        }
        final Schema.Entry bodyEntry = bodyBuilder.build();

        final Schema.Builder builder = this.recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        if (isCompletePayload) {
            builder.withEntry(statusEntry).withEntry(headersEntry);
        }

        final Schema schema = builder.withEntry(bodyEntry).build();
        return new SchemaContainer(headersEntry, schema);
    }

    @Data
    @AllArgsConstructor
    private static class SchemaContainer {

        final private Schema.Entry headersEntry;

        final private Schema recordSchema;

    }

}
