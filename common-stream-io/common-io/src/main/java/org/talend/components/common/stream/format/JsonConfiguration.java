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
package org.talend.components.common.stream.format;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("jsonPointer") })
@Documentation("Json Configuration with json pointer rules")
public class JsonConfiguration implements ContentFormat {

    static {
        try {
            Class.forName("org.talend.components.common.stream.format.json.JsonPointerParser");
        } catch (ClassNotFoundException e) {
            // not exist if no dependencies to stream-json, json format is not used.
        }
    }

    @Option
    @Documentation("json pointer expression")
    private String jsonPointer;

}
