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
package org.talend.components.common.stream.input.json;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderRepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.api.output.RecordWriterRepository;
import org.talend.components.common.stream.format.CSVConfiguration;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.JsonConfiguration;
import org.talend.components.common.stream.format.json.JsonPointerParser;
import org.talend.components.common.stream.output.json.JsonWriterSupplier;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class JsonReaderSupplier implements RecordReaderSupplier {

    static {
        RecordReaderRepository.getInstance().put(JsonConfiguration.class, new JsonReaderSupplier());
    }

    @Override
    public RecordReader getReader(RecordBuilderFactory factory, ContentFormat config) {
        assert config instanceof JsonConfiguration : "json reader not with json config";
        JsonConfiguration jsonConfig = (JsonConfiguration) config;
        final JsonPointerParser parser = JsonPointerParser.of(jsonConfig.getJsonPointer());
        return new JsonRecordReader(parser, factory);
    }
}
