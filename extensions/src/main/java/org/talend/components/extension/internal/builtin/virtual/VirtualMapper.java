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

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.List;

import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.output.Processor;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class VirtualMapper implements Mapper, Serializable {

    private final String virtualName;

    private final Mapper mapper;

    private final List<Processor> processors;

    @Override
    public long assess() {
        return mapper.assess();
    }

    @Override
    public List<Mapper> split(final long desiredSize) {
        return mapper.split(desiredSize).stream().map(mapper -> new VirtualMapper(virtualName, mapper, processors))
                .collect(toList());
    }

    @Override
    public Input create() {
        return new VirtualInput(mapper.create(), processors);
    }

    @Override
    public String name() {
        return virtualName;
    }

    @Override
    public void start() {
        mapper.start();
    }

    @Override
    public void stop() {
        mapper.stop();
    }

    @Override
    public boolean isStream() {
        return mapper.isStream();
    }

    @Override
    public String plugin() {
        return mapper.plugin();
    }

    @Override
    public String rootName() {
        return mapper.rootName();
    }
}
