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

import static java.util.Optional.ofNullable;

import java.io.Serializable;

import javax.annotation.PostConstruct;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;

public class UserGeneratorSource implements Serializable {

    private final Config config;

    private RecordBuilderFactory recordBuilderFactory;

    private int currentCount;

    public UserGeneratorSource(final Config config, final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
    }

    @PostConstruct
    public void init() {
        currentCount = config.startIndex + 1;
    }

    @Producer
    public Record next() {
        if (currentCount > config.rowCount) {
            return null;
        }

        final Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        if (config.nullEvery != -1 && currentCount % config.nullEvery == 0) {
            if (!config.withNullIds) {
                builder.withInt("id", currentCount);
            }
            if (config.withNullNames) {
                builder.withString("name", null);
            } else {
                builder.withString("name", ofNullable(config.namePrefix).orElse("user") + currentCount);
            }
        } else {
            builder.withInt("id", currentCount);
            builder.withString("name", ofNullable(config.namePrefix).orElse("user") + currentCount);
        }

        currentCount++;
        return builder.build();
    }

    @Data
    public static class Config implements Serializable {

        @Option
        private int startIndex;

        @Option
        private int rowCount;

        @Option
        private String namePrefix;

        @Option
        private boolean withNullNames = false;

        @Option
        private boolean withNullIds = false;

        @Option
        private int nullEvery = -1;
    }
}