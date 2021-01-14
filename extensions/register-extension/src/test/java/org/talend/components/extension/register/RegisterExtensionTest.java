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
package org.talend.components.extension.register;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.manager.ComponentManager;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class RegisterExtensionTest {

    @Test
    void run() throws ClassNotFoundException {
        try (final ComponentManager manager = ComponentManager.instance()) {
            final String plugin = manager.addPlugin("target/test-classes");
            final Container anyContainer = manager.findPlugin(plugin)
                    .orElseThrow(() -> new IllegalStateException("No container so test"));

            // Needed since the TestExtensionState.class in the anyContainer class loader
            // is not the same as the current one.
            final Class<?> extensionClass = anyContainer.getLoader().loadClass(TestExtension.TestExtensionState.class.getName());

            assertNotNull(anyContainer.get(extensionClass));
        }
    }
}
