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
package org.talend.components.common.stream;

import java.time.ZonedDateTime;
import java.util.Collection;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class SchemaHelper {

    public static Class<?> getFrom(Schema.Type recordFieldType) {
        Class<?> value = Object.class;
        switch (recordFieldType) {
        case RECORD:
            value = Record.class;
            break;
        case ARRAY:
            value = Collection.class;
            break;
        case STRING:
            value = String.class;
            break;
        case BYTES:
            value = byte[].class;
            break;
        case INT:
            value = Integer.class;
            break;
        case LONG:
            value = Long.class;
            break;
        case FLOAT:
            value = Float.class;
            break;
        case DOUBLE:
            value = Double.class;
            break;
        case BOOLEAN:
            value = Boolean.class;
            break;
        case DATETIME:
            value = ZonedDateTime.class;
            break;
        }
        return value;
    }

}
