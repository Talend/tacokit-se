/**
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.components.preparation.column;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.output.Processor;

@WithComponents("org.talend.components.preparation")
class ConcatTest {

    @Injected
    private ComponentsHandler handler;

    @Service
    private RecordBuilderFactory records;

    @Test
    void concatenation() {
        final Concat.Configuration configuration = new Concat.Configuration();
        configuration.setFirstColumn("first");
        configuration.setSecondColumn("second");
        configuration.setPrefix("^");
        configuration.setSeparator("|");
        configuration.setSuffix("$");
        configuration.setOutputColumnName("concatenated");

        final Processor processor = handler.createProcessor(Concat.class, configuration);

        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(
                records.newRecordBuilder().withString("first", "1").withString("second", "2").withString("third", "3").build(),
                records.newRecordBuilder().withString("first", "a").withString("second", "b").withString("third", "c").build()));

        final List<Record> output = handler.collect(processor, joinInputFactory).get(Record.class, "__default__");
        assertEquals(asList("^1|2$", "^a|b$"), output.stream().map(it -> it.getString("concatenated")).collect(toList()));
    }

    @Test
    void defaultValue() {
        final Concat.Configuration configuration = new Concat.Configuration();
        configuration.setFirstColumn("first");
        configuration.setSecondColumn("second");
        configuration.setFirstDefault("premier");
        configuration.setSecondDefault("deuxieme");
        configuration.setPrefix("^");
        configuration.setSeparator("|");
        configuration.setSuffix("$");
        configuration.setOutputColumnName("concatenated");

        final Processor processor = handler.createProcessor(Concat.class, configuration);

        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(records.newRecordBuilder().withString("first", "1").withString("third", "3").build(),
                        records.newRecordBuilder().withString("second", "b").withString("third", "c").build()));

        final List<Record> output = handler.collect(processor, joinInputFactory).get(Record.class, "__default__");
        assertEquals(asList("^1|deuxieme$", "^premier|b$"),
                output.stream().map(it -> it.getString("concatenated")).collect(toList()));
    }
}
