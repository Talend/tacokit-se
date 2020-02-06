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
package org.talend.components.extension.internal.builtin.virtual;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.components.extension.internal.builtin.test.component.virtual.FlatMapJson;
import org.talend.components.extension.internal.builtin.test.component.virtual.StaticSource;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithComponents("org.talend.components.extension.internal.builtin.test.component.virtual")
class VirtualComponentTest {

    @Injected
    private ComponentsHandler handler;

    @Test
    void runVirtualComponent() {
        final StaticSource.Configuration inputConfig = new StaticSource.Configuration();
        inputConfig.setValues(asList(
                new StaticSource.Person(asList(StaticSource.Name.builder().shortName("one").longName("first").build(),
                        StaticSource.Name.builder().shortName("un").longName("premier").build()), 1),
                new StaticSource.Person(asList(StaticSource.Name.builder().shortName("two").longName("second").build(),
                        StaticSource.Name.builder().shortName("deux").longName("deuxieme").build()), 2)));

        final FlatMapJson.Configuration processorConfig = new FlatMapJson.Configuration();
        processorConfig.setPointer("/names");

        final String staticSourceConfig = configurationByExample().withPrefix("configuration.Source.configuration")
                .forInstance(inputConfig).configured().toQueryString();

        final String flatMapConfig = configurationByExample().withPrefix("configuration.flatmapjson.configuration")
                .forInstance(processorConfig).configured().toQueryString();

        Job.components().component("from", "TestVirual://SourceWithFlatMapJson?" + staticSourceConfig + '&' + flatMapConfig)
                .component("to", "test://collector").connections().from("from").to("to").build().run();

        final List<Record> outputs = handler.getCollectedData(Record.class);
        assertEquals(4, outputs.size());
        assertEquals(asList("one:first", "un:premier", "two:second", "deux:deuxieme"),
                outputs.stream().map(r -> r.getString("shortName") + ":" + r.getString("longName")).collect(toList()));
    }
}
