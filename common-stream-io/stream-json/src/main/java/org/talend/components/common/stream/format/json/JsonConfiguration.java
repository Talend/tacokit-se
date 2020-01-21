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
package org.talend.components.common.stream.format.json;

import java.io.Serializable;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("jsonPointer") })
@Documentation("Json Configuration with json pointer rules")
public class JsonConfiguration implements Serializable, RecordReaderSupplier {

    @Option
    @Documentation("json pointer expression")
    private String jsonPointer;

    @Override
    public RecordReader getReader(RecordBuilderFactory factory) {
        final JsonPointerParser parser = JsonPointerParser.of(this.jsonPointer);
        return new JsonRecordReader(parser, factory);
    }
}
