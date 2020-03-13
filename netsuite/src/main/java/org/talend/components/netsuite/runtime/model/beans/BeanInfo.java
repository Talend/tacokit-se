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
package org.talend.components.netsuite.runtime.model.beans;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BeanInfo {

    /**
     * Properties of bean
     */
    private List<PropertyInfo> properties;

    private Map<String, PropertyInfo> propertyMap;

    public BeanInfo(List<PropertyInfo> properties) {
        this.properties = properties;
        propertyMap = properties.stream().collect(Collectors.toMap(PropertyInfo::getName, (element) -> element));
    }

    public List<PropertyInfo> getProperties() {
        return Collections.unmodifiableList(properties);
    }

    public Map<String, PropertyInfo> getPropertyMap() {
        return Collections.unmodifiableMap(propertyMap);
    }

    public PropertyInfo getProperty(String name) {
        return propertyMap.get(name);
    }

    @Override
    public String toString() {
        return new StringBuilder("BeanInfo{").append("properties=").append(properties).append('}').toString();
    }
}
