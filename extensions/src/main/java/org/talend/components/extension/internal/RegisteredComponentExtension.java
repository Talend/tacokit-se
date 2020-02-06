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
package org.talend.components.extension.internal;

import static java.util.Arrays.asList;
import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import javax.annotation.Priority;

import org.talend.components.extension.api.extension.CustomComponentExtension;
import org.talend.components.extension.internal.builtin.ExcludedComponentExtension;
import org.talend.components.extension.internal.builtin.VirtualComponentExtension;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.manager.spi.ContainerListenerExtension;

import lombok.RequiredArgsConstructor;

public class RegisteredComponentExtension implements ContainerListenerExtension {

    private final Collection<CustomComponentExtension> builtInExtensions = asList(new ExcludedComponentExtension(),
            new VirtualComponentExtension());

    // @Override 1.1.17 API
    public int order() {
        return -1; // before other potential extensions since we can veto some components
    }

    @Override
    public void onCreate(final Container container) {
        final List<CustomComponentExtension> extensions = loadExtensions(container);
        container.set(Closeables.class, new Closeables(extensions.stream().map(it -> it.onCreate(container))
                .filter(Optional::isPresent).flatMap(Optional::get).collect(toList())));
    }

    @Override
    public void onClose(final Container container) {
        final IllegalStateException ise = new IllegalStateException("Invalid undeployment");
        ofNullable(container.get(Closeables.class)).ifPresent(c -> c.tasks.forEach(it -> {
            try {
                it.run();
            } catch (final RuntimeException re) {
                ise.addSuppressed(re);
            }
        }));
        if (ise.getSuppressed().length > 0) {
            throw ise;
        }
    }

    private List<CustomComponentExtension> loadExtensions(final Container container) {
        final List<CustomComponentExtension> extensions = new ArrayList<>(builtInExtensions);
        StreamSupport.stream(ServiceLoader.load(CustomComponentExtension.class, container.getLoader()).spliterator(), false)
                .filter(it -> !it.getClass().getName().startsWith("org.talend.components.extension.internal.builtin."))
                .forEach(extensions::add);
        extensions
                .sort(comparing(it -> ofNullable(it.getClass().getAnnotation(Priority.class)).map(Priority::value).orElse(1000)));
        return extensions;
    }

    @RequiredArgsConstructor
    private static class Closeables {

        private final Collection<Runnable> tasks;
    }
}
