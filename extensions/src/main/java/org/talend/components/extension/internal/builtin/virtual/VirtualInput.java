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

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.manager.chain.AutoChunkProcessor;
import org.talend.sdk.component.runtime.output.Branches;
import org.talend.sdk.component.runtime.output.OutputFactory;
import org.talend.sdk.component.runtime.output.Processor;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class VirtualInput implements Input, Serializable {

    private final Input delegate;

    private final List<Processor> processors;

    private volatile List<AutoChunkProcessor> runtimeProcessors;

    private volatile List<Object> queue = new ArrayList<>();

    @Override
    public void start() {
        delegate.start();
        runtimeProcessors = processors.stream().map(p -> new AutoChunkProcessor(1 /* todo: configure chunk size */, p))
                .peek(AutoChunkProcessor::start).collect(toList());
    }

    @Override
    public void stop() {
        if (runtimeProcessors != null) {
            Collections.reverse(runtimeProcessors);
            runtimeProcessors.forEach(AutoChunkProcessor::stop);
        }
        delegate.stop();
    }

    @Override
    public Object next() {
        if (!queue.isEmpty()) { // un stack
            return queue.remove(0);
        }

        while (true) {
            Object record = delegate.next();
            if (record == null) { // input data are all read, exit
                return null;
            }

            // go through all processors
            List<Object> records = new ArrayList<>(singletonList(record));
            for (final AutoChunkProcessor processor : runtimeProcessors) {
                records = processWith(records, processor);
            }
            switch (records.size()) {
            case 0:
                break; // continue
            case 1:
                return records.get(0);
            default: // more than one, return the first one after having stacked the others
                queue.addAll(records.subList(1, records.size()));
                return records.get(0);
            }
        }
    }

    @Override
    public String plugin() {
        return delegate.plugin();
    }

    @Override
    public String rootName() {
        return delegate.rootName();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    private List<Object> processWith(final List<Object> inputs, final AutoChunkProcessor processor) {
        final List<Object> outputs = new ArrayList<>();
        inputs.forEach(input -> {
            final OutputFactory outputFactory = name -> {
                if (!Branches.DEFAULT_BRANCH.equals(name)) {
                    throw new IllegalArgumentException("Virtual components only support flatmap processors");
                }
                return outputs::add;
            };
            processor.onElement(name -> {
                if (!Branches.DEFAULT_BRANCH.equals(name)) {
                    throw new IllegalArgumentException("Virtual components only support flatmap processors");
                }
                return input;
            }, outputFactory);
            processor.flush(outputFactory);
        });
        return outputs;
    }
}
