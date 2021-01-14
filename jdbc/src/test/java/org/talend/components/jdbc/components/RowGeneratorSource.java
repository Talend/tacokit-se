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
package org.talend.components.jdbc.components;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static java.util.Optional.ofNullable;
import static org.talend.sdk.component.api.record.Schema.Type.BOOLEAN;
import static org.talend.sdk.component.api.record.Schema.Type.BYTES;
import static org.talend.sdk.component.api.record.Schema.Type.DATETIME;
import static org.talend.sdk.component.api.record.Schema.Type.DOUBLE;
import static org.talend.sdk.component.api.record.Schema.Type.FLOAT;
import static org.talend.sdk.component.api.record.Schema.Type.INT;
import static org.talend.sdk.component.api.record.Schema.Type.LONG;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

public class RowGeneratorSource implements Serializable {

    private final Config config;

    private int currentCount;

    private RecordBuilderFactory recordBuilderFactory;

    private transient Schema schema;

    public RowGeneratorSource(final Config config, final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @PostConstruct
    public void init() {
        currentCount = config.start + 1;
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("id").withType(INT).withNullable(false).build())
                .withEntry(
                        recordBuilderFactory.newEntryBuilder().withName("string_id").withType(STRING).withNullable(false).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_string").withType(STRING)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_string2").withType(STRING)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_text").withType(STRING)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_date").withType(DATETIME)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_datetime").withType(DATETIME)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_time").withType(DATETIME)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_double").withType(DOUBLE)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_long").withType(LONG)
                        .withNullable(config.withNullValues).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("t_float").withType(FLOAT)
                        .withNullable(config.withNullValues).build());
        if (config.withBytes) {
            schemaBuilder.withEntry(recordBuilderFactory.newEntryBuilder().withName("t_bytes").withType(BYTES)
                    .withNullable(config.withNullValues).build());
        }

        if (config.withBoolean) {
            schemaBuilder.withEntry(recordBuilderFactory.newEntryBuilder().withName("t_boolean").withType(BOOLEAN)
                    .withNullable(config.withNullValues).build());
        }

        schema = schemaBuilder.build();
    }

    @Producer
    public Record next() throws ParseException {
        if (currentCount > config.rowCount) {
            return null;
        }

        final Record.Builder builder = recordBuilderFactory.newRecordBuilder(schema);
        builder.withInt("id", currentCount);
        builder.withString("string_id", String.valueOf(currentCount));
        if (!config.withNullValues) {
            final Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-6").getTime());
            final Date datetime = new Date();
            final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39
            builder.withString("t_string", ofNullable(config.stringPrefix).orElse("customer") + currentCount);
            builder.withString("t_string2", RandomStringUtils.randomAlphabetic(50));
            builder.withString("t_text", RandomStringUtils.randomAlphabetic(300));
            if (config.withBoolean) {
                builder.withBoolean("t_boolean", currentCount % 3 != 0);
            }
            builder.withLong("t_long", 10000000000L);
            builder.withDouble("t_double", 1000.85d);
            builder.withFloat("t_float", 15.50f);
            builder.withDateTime("t_date", date);
            builder.withDateTime("t_datetime", datetime);
            builder.withDateTime("t_time", time);
        }

        if (config.withBytes && !config.withNullValues) {
            builder.withBytes("t_bytes", "some data in bytes".getBytes(StandardCharsets.UTF_8));
        }

        currentCount++;
        return builder.build();
    }

    @Data
    public static class Config implements Serializable {

        @Option
        private int start;

        @Option
        private int rowCount;

        @Option
        private String stringPrefix;

        @Option
        private boolean withNullValues;

        @Option
        private boolean withBytes = true;

        @Option
        private boolean withBoolean = true;

    }
}