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
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.output.Branches;
import org.talend.sdk.component.runtime.output.InputFactory;
import org.talend.sdk.component.runtime.output.OutputFactory;
import org.talend.sdk.component.runtime.output.Processor;

import lombok.Data;

class VirtualMapperTest {

    @Test
    void makeItRun() {
        final Collection<String> events = new ArrayList<>();
        final Mapper mapper = new VirtualMapper("whatever", new Mapper() {

            @Override
            public long assess() {
                return 1234;
            }

            @Override
            public List<Mapper> split(final long desiredSize) {
                return singletonList(this);
            }

            @Override
            public Input create() {
                return new Input() {

                    private Iterator<Person> people = asList(new Person("first"), new Person("second")).iterator();

                    @Override
                    public Object next() {
                        return people.hasNext() ? people.next() : null;
                    }

                    @Override
                    public String plugin() {
                        return "test";
                    }

                    @Override
                    public String rootName() {
                        return "Test";
                    }

                    @Override
                    public String name() {
                        return "root";
                    }

                    @Override
                    public void start() {
                        events.add("start_input");
                    }

                    @Override
                    public void stop() {
                        events.add("stop_input");
                    }
                };
            }

            @Override
            public boolean isStream() {
                return false;
            }

            @Override
            public String plugin() {
                return "test";
            }

            @Override
            public String rootName() {
                return "Test";
            }

            @Override
            public String name() {
                return "root";
            }

            @Override
            public void start() {
                events.add("start_mapper");
            }

            @Override
            public void stop() {
                events.add("stop_mapper");
            }
        }, singletonList(new Processor() {

            @Override
            public void beforeGroup() {
                events.add("beforeGroup");
            }

            @Override
            public void afterGroup(final OutputFactory output) {
                events.add("afterGroup");
            }

            @Override
            public void onNext(final InputFactory input, final OutputFactory output) {
                events.add("onNext");
                final OutputEmitter emitter = output.create(Branches.DEFAULT_BRANCH);
                // emit twice
                emitter.emit(input.read(Branches.DEFAULT_BRANCH));
                emitter.emit(input.read(Branches.DEFAULT_BRANCH));
            }

            @Override
            public String plugin() {
                return "test";
            }

            @Override
            public String rootName() {
                return "Test";
            }

            @Override
            public String name() {
                return "flatmap";
            }

            @Override
            public void start() {
                events.add("start_processor");
            }

            @Override
            public void stop() {
                events.add("stop_processor");
            }
        }));

        mapper.start();
        final List<Mapper> newMappers = mapper.split(1);
        assertEquals(1, newMappers.size());
        mapper.stop();

        final Input input = newMappers.iterator().next().create();
        input.start();
        Object record;
        final Collection<Object> outs = new ArrayList<>();
        while ((record = input.next()) != null) {
            outs.add(record);
        }
        input.stop();
        assertEquals(4, outs.size());
        assertEquals(asList("start_mapper", "stop_mapper", "start_input", "start_processor", "beforeGroup", "onNext",
                "afterGroup", "beforeGroup", "onNext", "afterGroup", "stop_processor", "stop_input"), events);
    }

    @Data
    public static class Person {

        private final String age;
    }
}
