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
package org.talend.components.common.stream.input.fixed;

import java.io.Serializable;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.FixedConfiguration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;

@Data
public class FixedRecordReader implements Serializable {

    private final FixedConfiguration configuration;

    public FixedRecordReader(FixedConfiguration configuration) {
        this.configuration = configuration;
    }

    public RecordReader getReader(RecordBuilderFactory factory) {
        return new FixedReaderSupplier().getReader(factory, configuration);
    }

}
