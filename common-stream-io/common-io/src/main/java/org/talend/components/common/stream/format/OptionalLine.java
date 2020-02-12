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
package org.talend.components.common.stream.format;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout(@GridLayout.Row({ "activ", "size" }))
public class OptionalLine implements Serializable {

    private static final long serialVersionUID = -5243288997978197551L;

    @Option
    @Documentation("Activ optional lines.")
    private boolean activ;

    @Option
    @ActiveIf(target = "activ", value = "true")
    @Documentation("Number of optional lines.")
    private int size;

    public int getSize() {
        if (!this.activ) {
            return 0;
        }
        return this.size;
    }
}
