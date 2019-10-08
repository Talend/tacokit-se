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
package org.talend.components.rest.configuration;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "rawValue" }) })
@Documentation("")
public class RequestBody implements Serializable {

    @Option
    @Documentation("")
    private Type type = Type.RAW;

    @Option
    @TextArea
    @ActiveIf(target = "type", value = "RAW")
    @Documentation("")
    private String rawValue;

    @Option
    @ActiveIf(target = "type", value = { "FORM_DATA", "X_WWW_FORM_URLENCODED" })
    @Documentation("")
    private Set<Param> params = new HashSet<>();

    @Option
    @ActiveIf(target = "type", value = "BINARY")
    @Documentation("")
    private String binaryPath;

    public enum Type {
        RAW,
        FORM_DATA,
        X_WWW_FORM_URLENCODED,
        BINARY
    }
}
