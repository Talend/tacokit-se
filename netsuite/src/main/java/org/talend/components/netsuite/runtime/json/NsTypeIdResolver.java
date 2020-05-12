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
package org.talend.components.netsuite.runtime.json;

import org.talend.components.netsuite.runtime.model.BasicMetaData;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class NsTypeIdResolver extends TypeIdResolverBase {

    private BasicMetaData basicMetaData;

    protected NsTypeIdResolver(JavaType baseType, TypeFactory typeFactory, BasicMetaData basicMetaData) {
        super(baseType, typeFactory);

        this.basicMetaData = basicMetaData;
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
        Class<?> clazz = basicMetaData.getTypeClass(id);
        if (clazz == null) {
            return null;
        }
        return TypeFactory.defaultInstance().constructSimpleType(clazz, null);
    }

    @Override
    public String idFromValue(Object value) {
        return value.getClass().getSimpleName();
    }

    @Override
    public String idFromValueAndType(Object value, Class<?> suggestedType) {
        return suggestedType.getSimpleName();
    }

    @Override
    public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.NAME;
    }
}
