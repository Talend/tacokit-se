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
package org.talend.components.migration.source;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.migration.conf.AbstractConfig;
import org.talend.components.migration.conf.SourceConfig;
import org.talend.components.migration.migration.SourceMigrationHandler;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Slf4j
@Version(value = AbstractConfig.VERSION, migrationHandler = SourceMigrationHandler.class)
@Icon(Icon.IconType.STAR)
@org.talend.sdk.component.api.input.Emitter(name = "dummySource")
@Documentation("Dummy connector to support migration handler.")
public class DummySource implements Serializable {

    private final SourceConfig config;

    private boolean done = false;

    public DummySource(@Option("configuration") final SourceConfig config) {
        this.config = config;
    }

    @Producer
    public SourceConfig next() {
        try {
            config.getDse().getDso().getDso_shouldNotBeEmpty().trim();
        } catch (Exception e) {
            throw new IllegalArgumentException("The dso_shouldNotBeEmpty property is not set !", e);
        }

        if (done) {
            return null;
        }
        done = true;
        return config;
    }

}
