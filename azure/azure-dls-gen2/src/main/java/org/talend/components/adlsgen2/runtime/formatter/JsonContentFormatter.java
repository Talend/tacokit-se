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
package org.talend.components.adlsgen2.runtime.formatter;

import java.util.List;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;

import org.talend.components.common.converters.JsonConverter;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonContentFormatter extends AbstractContentFormatter {

    private final RecordBuilderFactory recordBuilderFactory;

    private final OutputConfiguration configuration;

    private final JsonConverter converter;

    private final JsonBuilderFactory jsonBuilderFactory;

    private boolean hasAlreadyItems;

    public JsonContentFormatter(@Option("configuration") final OutputConfiguration configuration,
            final RecordBuilderFactory recordBuilderFactory, final JsonBuilderFactory jsonBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.configuration = configuration;
        converter = JsonConverter.of(recordBuilderFactory, jsonBuilderFactory, configuration.getDataSet().getJsonConfiguration());
    }

    @Override
    public byte[] feedContent(List<Record> records) {
        JsonArrayBuilder b = jsonBuilderFactory.createArrayBuilder();
        for (Record record : records) {

            b.add(converter.fromRecord(record));
        }
        return b.build().toString().getBytes();
    }

    @Override
    public boolean hasHeader() {
        return true;
    }

    @Override
    public boolean hasFooter() {
        return true;
    }

    @Override
    public byte[] initializeContent() {
        return "[".getBytes();
    }

    @Override
    public byte[] finalizeContent() {
        return "]".getBytes();
    }
}
