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
package org.talend.components.common.stream.format;

import java.util.Optional;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "lineSeparatorType", "lineSeparator" }), //
        @GridLayout.Row({ "encoding" }), //
        @GridLayout.Row({ "header" }) })
public class LineConfiguration implements ContentFormat {

    private static final long serialVersionUID = 6614704115891739018L;

    @AllArgsConstructor
    public enum LineSeparatorType {
        LF("\n"),
        CRLF("\r\n"),
        OTHER("");

        private final String separator;
    }

    @Option
    @Documentation("Type of symbol(s) used to separate lines.")
    private LineSeparatorType lineSeparatorType = LineSeparatorType.LF;

    @Option
    @ActiveIf(target = "lineSeparatorType", value = "OTHER")
    @Documentation("Symbol(s) used to separate lines.")
    private String lineSeparator = "\n";

    public String getLineSeparator() {
        if (this.lineSeparatorType == LineSeparatorType.OTHER) {
            return this.lineSeparator;
        }
        return this.lineSeparatorType.separator;
    }

    @Option
    @Documentation("Encoding.")
    private Encoding encoding = new Encoding();

    @Option
    @Documentation("Header.")
    private OptionalLine header;

    public int calcHeader() {
        return Optional.ofNullable(this.header).map(OptionalLine::getSize).orElse(0);
    }

}
