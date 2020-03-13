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

import java.util.Collection;

import javax.xml.datatype.XMLGregorianCalendar;

import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.model.BasicMetaData;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;

public class NsTypeResolverBuilder extends ObjectMapper.DefaultTypeResolverBuilder {

    public static final String TYPE_PROPERTY_NAME = "nsType";

    private BasicMetaData basicMetaData;

    public NsTypeResolverBuilder(BasicMetaData basicMetaData) {
        super(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE);

        this.basicMetaData = basicMetaData;

        init(JsonTypeInfo.Id.NAME, null);
        inclusion(JsonTypeInfo.As.PROPERTY);
        typeProperty(TYPE_PROPERTY_NAME);
    }

    @Override
    public boolean useForType(JavaType t) {
        return t.isCollectionLikeType() || t.getRawClass() == XMLGregorianCalendar.class ? false : super.useForType(t);
    }

    @Override
    protected TypeIdResolver idResolver(MapperConfig<?> config, JavaType baseType, PolymorphicTypeValidator subtypeValidator,
            Collection<NamedType> subtypes, boolean forSer, boolean forDeser) {

        if (_idType == null) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), "Mapping is not initialized");
        }

        return new NsTypeIdResolver(baseType, config.getTypeFactory(), basicMetaData);
    }
}
