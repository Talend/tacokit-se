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
package org.talend.components.extension.internal.builtin.test.component.virtual;

import java.io.Serializable;
import java.util.Collection;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointer;
import org.talend.sdk.component.api.record.RecordPointerFactory;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Processor(name = "flatmapjson")
public class FlatMapJson implements Serializable {

    private final Configuration configuration;

    private final RecordPointerFactory recordPointerFactory;

    @ElementListener
    public void onElement(final Record record, @Output final OutputEmitter<Record> recordOutputEmitter) {
        final RecordPointer recordPointer = recordPointerFactory.apply(configuration.getPointer());
        final Collection<Record> nested = recordPointer.getValue(record, Collection.class);
        if (nested != null) {
            nested.forEach(recordOutputEmitter::emit);
        }
    }

    @Data
    public static class Configuration {

        @Option
        @Required
        private String pointer;
    }
}
