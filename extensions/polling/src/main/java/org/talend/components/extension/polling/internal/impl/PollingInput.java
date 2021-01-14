/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
import lombok.extern.slf4j.Slf4j;
import org.talend.components.extension.polling.api.Pollable;
import org.talend.sdk.component.runtime.base.Delegated;
import org.talend.sdk.component.runtime.input.Input;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@RequiredArgsConstructor
public class PollingInput implements Input, Serializable {

    private final PollingConfiguration pollingConfiguration;

    private final Input input;

    private final AtomicLong lastExec = new AtomicLong(0);

    private String resumeMethodName;

    private Method resumeMethod;

    @Override
    public Object next() {
        long current = System.currentTimeMillis();

        final long duration = current - this.lastExec.get();
        if (duration < pollingConfiguration.getDelay()) {
            return null; // currenty resume not supported, just to know that we will recall the input
        }

        final Object delegate = Delegated.class.cast(input).getDelegate();
        if (resumeMethodName == null) {
            resumeMethodName = delegate.getClass().getAnnotation(Pollable.class).resumeMethod();
        }

        // Resumable.class.cast(Delegated.class.cast(input).getDelegate()).resume(null);
        resume(delegate, resumeMethodName, null);

        log.info("Call batch input from polling after {} ms.", duration);
        this.lastExec.set(current);
        return input.next();
    }

    private void resume(final Object delegate, final String resumeMethodName, final Object configuration) {
        if (resumeMethodName.isEmpty()) {
            return; // No resume method defined
        }

        try {
            if (resumeMethod == null) {
                Class clazz = configuration == null ? Object.class : configuration.getClass();
                resumeMethod = delegate.getClass().getMethod(resumeMethodName, clazz);
            }
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can't find resume method for delegate", e);
        }

        try {
            resumeMethod.invoke(delegate, configuration);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Can't invoke resume method for delegate", e);
        }
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