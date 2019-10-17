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
package org.talend.components.rest.service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ContentType {

    public final static String HEADER_KEY = "Content-Type";

    public final static String CHARSET_KEY = "charset=";

    public enum ContentTypeEnum {
        TEXT_PLAIN("text/plain"),
        APP_JSON("application/json");

        private final String value;

        ContentTypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        private static final Map<String, ContentTypeEnum> revert;
        static {
            Map<String, ContentTypeEnum> map = new ConcurrentHashMap<>();
            for (ContentTypeEnum e : ContentTypeEnum.values()) {
                map.put(e.getValue(), e);
            }
            revert = Collections.unmodifiableMap(map);
        }

        public static ContentTypeEnum get(String name) {
            return revert.get(name);
        }
    }

}
