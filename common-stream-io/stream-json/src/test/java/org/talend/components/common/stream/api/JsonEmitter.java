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
package org.talend.components.common.stream.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;

import javax.json.JsonReaderFactory;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.common.stream.input.json.JsonReaderSupplier;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Icon(Icon.IconType.STAR)
@Emitter(name = "jsonInput")
@Documentation("")
public class JsonEmitter implements Serializable {

    private JsonReaderFactory jsonReaderFactory;

    private RecordBuilderFactory recordBuilderFactory;

    private final Config config;

    private transient boolean done = false;

    private transient Iterator<Record> records = null;

    public JsonEmitter(@Option("configuration") final Config config, JsonReaderFactory jsonReaderFactory,
            RecordBuilderFactory recordBuilderFactory) {
        this.config = config;
        this.jsonReaderFactory = jsonReaderFactory;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @Producer
    public Record next() {
        if (done) {
            return null;
        }

        if (records == null) {
            try (InputStream in = getClass().getClassLoader().getResourceAsStream(config.getJsonFile())) {
                final RecordReaderSupplier recordReaderSupplier = new JsonReaderSupplier();
                final RecordReader reader = recordReaderSupplier.getReader(recordBuilderFactory, config);
                records = reader.read(in);
            } catch (IOException e) {
                log.error("Can't read json file.", e);
            }
        }

        if (!records.hasNext()) {
            records = null;
            done = true;
            return null;
        }

        final Record next = records.next();
        return next;
    }

    @Data
    public static class Config extends JsonConfiguration {

        @Option
        String jsonFile;
    }

}
