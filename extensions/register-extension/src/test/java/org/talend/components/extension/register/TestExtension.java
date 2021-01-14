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

import org.talend.components.extension.register.api.CustomComponentExtension;
import org.talend.sdk.component.container.Container;

import java.util.Optional;
import java.util.stream.Stream;

public class TestExtension implements CustomComponentExtension {

    @Override
    public Optional<Stream<Runnable>> onCreate(final Container container) {
        container.set(TestExtensionState.class, new TestExtensionState());
        return Optional.empty();
    }

    public static class TestExtensionState {
    }
}
