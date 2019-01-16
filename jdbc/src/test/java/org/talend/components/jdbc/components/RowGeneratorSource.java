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
package org.talend.components.jdbc.components;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static java.util.Optional.ofNullable;

public class RowGeneratorSource implements Serializable {

    private final Config config;

    private int currentCount;

    private RecordBuilderFactory recordBuilderFactory;

    public RowGeneratorSource(final Config config, final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @PostConstruct
    public void init() {
        currentCount = config.start + 1;
    }

    @Producer
    public Record next() throws ParseException {
        if (currentCount > config.rowCount) {
            return null;
        }

        final Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.withMissingIdEvery <= 0 || currentCount % config.withMissingIdEvery != 0) {
            builder.withInt("id", currentCount);
            final String uuid = UUID.randomUUID().toString();
            builder.withString("string_id", uuid.substring(0, Math.min(255, uuid.length())));
        }

        if (config.withNullValues) {
            builder.withString("t_string", null);
            builder.withString("t_string2", null);
            builder.withString("t_text", null);
            builder.withDateTime("t_date", (Date) null);
            builder.withDateTime("t_datetime", (Date) null);
            builder.withDateTime("t_time", (Date) null);

        } else {
            final Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-6").getTime());
            final Date datetime = new Date();
            final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39
            builder.withString("t_string", ofNullable(config.stringPrefix).orElse("customer") + currentCount);
            builder.withString("t_string2", RandomStringUtils.randomAlphabetic(50));
            builder.withString("t_text", RandomStringUtils.randomAlphabetic(300));
            builder.withBoolean("t_boolean", true);
            builder.withLong("t_long", 10000000000L);
            builder.withDouble("t_double", 1000.85d);
            builder.withFloat("t_float", 15.50f);
            builder.withDateTime("t_date", date);
            builder.withDateTime("t_datetime", datetime);
            builder.withDateTime("t_time", time);
        }

        if (config.withBytes) {
            if (config.withNullValues) {
                builder.withBytes("t_bytes", null);
            } else {
                builder.withBytes("t_bytes", "some data in bytes".getBytes(StandardCharsets.UTF_8));
            }
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
        private int withMissingIdEvery;

        @Option
        private boolean withBytes = true;

    }
}