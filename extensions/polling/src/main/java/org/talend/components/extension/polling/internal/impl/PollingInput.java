/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
import lombok.extern.slf4j.Slf4j;
import org.talend.components.extension.polling.api.Resumable;
import org.talend.sdk.component.runtime.base.Delegated;
import org.talend.sdk.component.runtime.input.Input;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@RequiredArgsConstructor
public class PollingInput implements Input, Serializable {

    private final PollingConfiguration pollingConfiguration;

    private final Input input;

    private final AtomicLong lastExec = new AtomicLong(0);

    @Override
    public Object next() {
        long current = System.currentTimeMillis();

        final long duration = current - this.lastExec.get();
        if (duration < pollingConfiguration.getDelay()) {
            return null; // currenty resume not supported, just to know that we will recall the input
        }

        Resumable.class.cast(Delegated.class.cast(input).getDelegate()).resume(null);

        log.info("Call batch input from polling after {} ms.", duration);
        this.lastExec.set(current);
        return input.next();
    }

    @Override
    public String plugin() {
        return input.plugin();
    }

    @Override
    public String rootName() {
        return input.rootName();
    }

    @Override
    public String name() {
        return input.name();
    }

    @Override
    public void start() {
        input.start();
    }

    @Override
    public void stop() {
        input.stop();
    }
}