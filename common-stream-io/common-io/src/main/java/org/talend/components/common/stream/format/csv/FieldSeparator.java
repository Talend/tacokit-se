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
package org.talend.components.common.stream.format.csv;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "fieldSeparatorType", "fieldSeparator" }), //
})
public class FieldSeparator implements Serializable {

    private static final long serialVersionUID = 6180133661517097088L;

    public enum Type {
        SEMICOLON(';'),
        COMMA(','),
        TABULATION('\t'),
        SPACE(' '),
        OTHER((char) 0);

        private char delimiter;

        Type(char delimiter) {
            this.delimiter = delimiter;
        }
    }

    @Option
    @Documentation("Field delimiter type.")
    private Type fieldSeparatorType = FieldSeparator.Type.COMMA;

    @Option
    @ActiveIf(target = "fieldSeparatorType", value = "OTHER")
    @Documentation("Custom field delimiter.")
    private Character fieldSeparator;

    public Character findFieldSeparator() {
        if (this.fieldSeparatorType != Type.OTHER) {
            return this.fieldSeparatorType.delimiter;
        }
        return this.fieldSeparator;
    }
}
