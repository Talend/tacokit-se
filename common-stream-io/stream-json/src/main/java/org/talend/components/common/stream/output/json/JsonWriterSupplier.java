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
package org.talend.components.common.stream.output.json;

import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterRepository;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.api.output.WritableTarget;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.JsonConfiguration;

public class JsonWriterSupplier implements RecordWriterSupplier<byte[]> {

    static {
        RecordWriterRepository.getInstance().put(JsonConfiguration.class, new JsonWriterSupplier());
    }

    @Override
    public RecordWriter getWriter(WritableTarget<byte[]> target, ContentFormat config) {
        assert config instanceof JsonConfiguration : "json reader not with json config";
        final JsonConfiguration jsonConfig = (JsonConfiguration) config;

        return new JsonWriter(jsonConfig, target);
    }
}
