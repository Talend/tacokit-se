/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Date;

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
    public Record next() {
        if (currentCount > config.rowCount) {
            return null;
        }

        final Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.withMissingIdEvery <= 0 || currentCount % config.withMissingIdEvery != 0) {
            builder.withInt("id", currentCount);
        }

        if (config.withNullValues) {
            builder.withString("t_string", null);
            builder.withDateTime("t_date", (Date) null);
            builder.withBytes("t_bytes", null);
        } else {
            builder.withString("t_string", ofNullable(config.stringPrefix).orElse("customer") + currentCount);
            builder.withBoolean("t_boolean", true);
            builder.withLong("t_long", 10000000000L);
            builder.withDouble("t_double", 1000.85d);
            builder.withFloat("t_float", 15.50f);
            builder.withDateTime("t_date", new Date());
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
        private int withMissingIdEvery;

    }
}