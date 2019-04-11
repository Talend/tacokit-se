// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.rest.service;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ContentType {

    public final static String headerValue = "Content-Type";

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
