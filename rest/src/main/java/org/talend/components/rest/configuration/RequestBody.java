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

    public enum Type {
        TEXT(
                "text/plain",
                conf -> Optional.ofNullable(conf.getTextValue()).map(b -> b.getBytes(StandardCharsets.UTF_8))
                        .orElse(new byte[0])),
        JSON(
                "text/json",
                conf -> Optional.ofNullable(conf.getJsonValue()).map(b -> b.getBytes(StandardCharsets.UTF_8))
                        .orElse(new byte[0])),
        XML(
                "text/xml",
                conf -> Optional.ofNullable(conf.getXmlValue()).map(b -> b.getBytes(StandardCharsets.UTF_8)).orElse(new byte[0])),
        FORM_DATA(
                "multipart/form-data",
                conf -> Base64.getUrlEncoder()
                        .encode(conf.getParams().stream().map(param -> param.getKey() + "=" + queryEncode(param.getValue()))
                                .collect(Collectors.joining("&")).getBytes(StandardCharsets.UTF_8))),
        X_WWW_FORM_URLENCODED(
                "application/x-www-form-urlencoded",
                conf -> Base64.getUrlEncoder()
                        .encode(conf.getParams().stream().map(param -> param.getKey() + "=" + queryEncode(param.getValue()))
                                .collect(Collectors.joining("&")).getBytes(StandardCharsets.UTF_8)));

        private final String contentType;

        private final Function<RequestBody, byte[]> transform;

        Type(final String contentType, final Function<RequestBody, byte[]> transform) {
            this.contentType = contentType;
            this.transform = transform;
        }

        public String getContentType() {
            return this.contentType;
        }

        public byte[] getBytes(final RequestBody conf) {
            return transform.apply(conf);
        }
    }

}
