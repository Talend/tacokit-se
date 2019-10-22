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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

import static org.talend.components.common.service.http.UrlEncoder.queryEncode;

@Data
@GridLayout({ @GridLayout.Row({ "type" }), @GridLayout.Row({ "textValue" }), @GridLayout.Row({ "jsonValue" }),
        @GridLayout.Row({ "xmlValue" }), @GridLayout.Row({ "params" }) })
@Documentation("")
public class RequestBody implements Serializable {

    @Option
    @Documentation("")
    private Type type = Type.TEXT;

    @Option
    @TextArea
    @ActiveIf(target = "type", value = "RAW")
    @Documentation("")
    private String textValue;

    @Option
    @Code("json")
    @ActiveIf(target = "type", value = "JSON")
    @Documentation("")
    private String jsonValue;

    @Option
    @Code("xml")
    @ActiveIf(target = "type", value = "XML")
    @Documentation("")
    private String xmlValue;

    @Option
    @ActiveIf(target = "type", value = { "FORM_DATA", "X_WWW_FORM_URLENCODED" })
    @Documentation("")
    private List<Param> params = new ArrayList<>();

    public void setTextContent(String content) {
        switch (this.getType()) {
        case TEXT:
            this.setTextValue(content);
            break;
        case JSON:
            this.setJsonValue(content);
            break;
        case XML:
            this.setXmlValue(content);
            break;
        default:
            throw new IllegalArgumentException("You can't set text content for body type " + this.getType());
        }
    }

    public String getTextContent() {
        switch (this.getType()) {
        case TEXT:
            return this.getTextValue();
        case JSON:
            return this.getJsonValue();
        case XML:
            return this.getXmlValue();
        default:
            throw new IllegalArgumentException("You can't get text content for body type " + this.getType());
        }
    }

    public enum Type {
        TEXT("text/plain"),
        JSON("text/json"),
        XML("text/xml"),
        FORM_DATA("multipart/form-data"),
        X_WWW_FORM_URLENCODED("application/x-www-form-urlencoded");

        private final String contentType;

        Type(final String contentType) {
            this.contentType = contentType;
        }

        public String getContentType() {
            return this.contentType;
        }
    }

}
