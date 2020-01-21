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
package org.talend.components.common.stream.format.line;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@GridLayout(@GridLayout.Row({ "lineSeparatorType", "lineSeparator" }))
public class LineConfiguration implements Serializable {

    private static final long serialVersionUID = 6614704115891739018L;

    @AllArgsConstructor
    public enum Type {
        LF("\n"),
        CRLF("\r\n"),
        OTHER("");

        private final String separator;
    }

    @Option
    @Documentation("Type of symbol(s) used to separate lines")
    private Type lineSeparatorType = Type.LF;

    @Option
    @ActiveIf(target = "lineSeparatorType", value = "OTHER")
    @Documentation("Symbol(s) used to separate lines")
    private String lineSeparator = "\n";

    public String getLineSeparator() {
        if (this.lineSeparatorType == Type.OTHER) {
            return this.lineSeparator;
        }
        return this.lineSeparatorType.separator;
    }
}
