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
package org.talend.components.extension.api.extension;

import static java.util.Optional.ofNullable;

import java.util.Optional;
import java.util.stream.Stream;

import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.manager.ComponentFamilyMeta;
import org.talend.sdk.component.runtime.manager.ContainerComponentRegistry;

/**
 * An extension which modifies the deployed components before the actual runtime.
 * They can be sorted using @{@link javax.annotation.Priority}. Default value if not set is 1000.
 */
public interface CustomComponentExtension {

    /**
     * @param container the family currently deployed.
     * @return an optional list of close tasks to execute when the family is undeployed.
     */
    default Optional<Stream<Runnable>> onCreate(final Container container) {
        return Optional.empty();
    }

    /**
     * @param container container to process.
     * @return an optional stream of families in this container.
     */
    default Optional<Stream<ComponentFamilyMeta>> getFamilies(final Container container) {
        return ofNullable(container.get(ContainerComponentRegistry.class))
                .map(registry -> registry.getComponents().values().stream());
    }
}
