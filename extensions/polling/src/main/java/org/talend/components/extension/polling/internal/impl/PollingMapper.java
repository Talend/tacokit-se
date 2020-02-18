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
package org.talend.components.extension.polling.internal.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class PollingMapper implements Mapper, Serializable {

    private final PollingConfiguration pollingConfiguration;

    private final Mapper batchMapper;

    @Override
    public long assess() {
        return batchMapper.assess();
    }

    @Override
    public List<Mapper> split(long desiredSize) {
        return batchMapper.split(desiredSize).stream()
                .map(it -> new PollingMapper(pollingConfiguration, it))
                .collect(toList());
    }

    @Override
    public void start() {
        batchMapper.start();
    }

    @Override
    public void stop() {
        batchMapper.stop();
    }

    @Override
    public Input create() {
        final Input input = batchMapper.create();
        return new PollingInput(pollingConfiguration, input);
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Override
    public String plugin() {
        return batchMapper.plugin();
    }

    @Override
    public String rootName() {
        return batchMapper.rootName();
    }

    @Override
    public String name() {
        return batchMapper.name();
    }
}
