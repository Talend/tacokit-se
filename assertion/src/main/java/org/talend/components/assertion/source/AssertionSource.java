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
package org.talend.components.assertion.source;

import org.talend.components.assertion.conf.Config;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;

@Version(1)
@Icon(Icon.IconType.STAR)
@Emitter(name = "AssertSource")
@Documentation("Should not exist, Here only to respect the rule : 1 dataset => 1 input for sampling")
public class AssertionSource implements Serializable {

    private final Config.AssertDSE config;

    public AssertionSource(@Option("configuration") final Config.AssertDSE dse) {
        this.config = dse;
    }

    @Producer
    public Record next() {
        return null;
    }
}
