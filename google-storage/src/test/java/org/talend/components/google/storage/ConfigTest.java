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
package org.talend.components.google.storage;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.talend.sdk.component.junit.base.junit5.TemporaryFolder;
import org.talend.sdk.component.tools.ComponentValidator;

import lombok.extern.slf4j.Slf4j;

public class ConfigTest {

    @Slf4j
    public static class TestLog implements org.talend.sdk.component.tools.Log {

        private final Collection<String> messages = new ArrayList<>();

        @Override
        public void debug(final String s) {
            log.info(s);
            messages.add("[DEBUG] " + s);
        }

        @Override
        public void error(final String s) {
            log.error(s);
            messages.add("[ERROR] " + s);
        }

        @Override
        public void info(final String s) {
            log.info(s);
            messages.add("[INFO] " + s);
        }
    }

    ComponentValidator validator;

    TestLog log = new TestLog();

    @Test
    void test() {
        // copy ComponentValidatorTest

    }

    public void beforeEach() {
        // final ComponentPackage config = context.getElement().get().getAnnotation(ComponentPackage.class);
        // final ExtensionContext.Store store = context.getStore(NAMESPACE);
        // final File pluginDir =
        // new File(TemporaryFolder.class.cast(store.get(TemporaryFolder.class.getName())).getRoot() + "/"
        // + context.getRequiredTestMethod().getName());
        final ComponentValidator.Configuration cfg = new ComponentValidator.Configuration();
        cfg.setValidateFamily(true);
        cfg.setValidateSerializable(true);
        cfg.setValidateMetadata(true);
        cfg.setValidateInternationalization(true);
        cfg.setValidateDataSet(true);
        cfg.setValidateActions(true);
        cfg.setValidateComponent(true);
        cfg.setValidateModel(true);
        cfg.setValidateDataStore(true);
        cfg.setValidateLayout(true);
        cfg.setValidateOptionNames(true);
        cfg.setValidateLocalConfiguration(true);
        cfg.setValidateOutputConnection(true);
        cfg.setValidatePlaceholder(true);
        cfg.setValidateSvg(true);
        cfg.setValidateNoFinalOption(true);
        cfg.setValidateDocumentation(true);
        // cfg.setPluginId();
        // Optional.of(config.pluginId()).filter(it -> !it.isEmpty()).ifPresent(cfg::setPluginId);
        // validator = new ComponentValidator(cfg, new File[] { pluginDir }, log);

    }
}
