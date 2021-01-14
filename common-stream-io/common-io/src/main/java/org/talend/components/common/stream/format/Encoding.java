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

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout(@GridLayout.Row({ "encodingType", "encoding" }))
public class Encoding implements Serializable {

    private static final long serialVersionUID = -332572169846419496L;

    public enum Type {
        UTF8("UTF-8"), //
        ISO_8859_15("ISO-8859-15"), //
        OTHER("OTHER");

        Type(String encodingCharsetValue) {
            this.encodingCharsetValue = encodingCharsetValue;
        }

        private String encodingCharsetValue;

        public String getEncodingValue() {
            return encodingCharsetValue;
        }
    }

    @Option
    @Documentation("Content encoding.")
    private Encoding.Type encodingType = Encoding.Type.UTF8;

    @Option
    @ActiveIf(target = "encodingType", value = "OTHER")
    @Documentation("Custom content encoding.")
    private String encoding;

    public String getEncoding() {
        if (encodingType == Encoding.Type.OTHER) {
            return this.encoding;
        }
        return this.encodingType.getEncodingValue();
    }
}