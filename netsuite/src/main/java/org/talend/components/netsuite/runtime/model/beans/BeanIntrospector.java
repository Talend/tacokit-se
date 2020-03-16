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

import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.client.NetSuiteException;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Responsible for introspection of beans and detecting of properties.
 *
 * <p>
 * This is simplified version of {@link java.beans.Introspector} and is intended
 * to be used for beans generated from NetSuite's XML schemas.
 */
public class BeanIntrospector {

    private static final String SET = "set";

    private static final String GET = "get";

    private static final String IS = "is";

    /**
     * This value we get by applying Abstract, Static and Public modifiers value got from
     * {@link java.lang.reflect.Modifier}
     */
    private static final int ACCEPTABLE_MODIFIERS = Modifier.ABSTRACT | Modifier.STATIC | Modifier.PUBLIC | Modifier.NATIVE;

    private static final Map<String, Class<?>> PRIMITIVE_WRAPPER_TYPES = new HashMap<>();

    static {
        PRIMITIVE_WRAPPER_TYPES.put(Byte.TYPE.getName(), Byte.class);
        PRIMITIVE_WRAPPER_TYPES.put(Boolean.TYPE.getName(), Boolean.class);
        PRIMITIVE_WRAPPER_TYPES.put(Character.TYPE.getName(), Character.class);
        PRIMITIVE_WRAPPER_TYPES.put(Double.TYPE.getName(), Double.class);
        PRIMITIVE_WRAPPER_TYPES.put(Float.TYPE.getName(), Float.class);
        PRIMITIVE_WRAPPER_TYPES.put(Integer.TYPE.getName(), Integer.class);
        PRIMITIVE_WRAPPER_TYPES.put(Long.TYPE.getName(), Long.class);
        PRIMITIVE_WRAPPER_TYPES.put(Short.TYPE.getName(), Short.class);
    }

    private BeanIntrospector() {

    }

    private static final BeanIntrospector INSTANCE = new BeanIntrospector();

    public static BeanIntrospector getInstance() {
        return INSTANCE;
    }

    public List<PropertyInfo> getProperties(String className) throws ClassNotFoundException {
        Class<?> clazz = Class.forName(className);
        return new ArrayList<>(getProperties(getMethods(clazz)));
    }

    /**
     * Detect and get properties for given set of methods.
     *
     * @param methods methods to be scanned
     * @return properties
     */
    protected Set<PropertyInfo> getProperties(Set<Method> methods) {
        Map<String, Method> getters = new HashMap<>();
        Map<String, List<Method>> setters = new HashMap<>();
        if (!methods.isEmpty()) {
            getters = methods.stream().filter(BeanIntrospector::isGetter)
                    .collect(Collectors.toMap(method -> getUpperPropertyName(method.getName()), method -> method));
            setters = methods.stream().filter(BeanIntrospector::isSetter)
                    .collect(Collectors.groupingBy(method -> getUpperPropertyName(method.getName())));
        }

        Set<PropertyInfo> properties = new HashSet<>();
        // Might be refactored later
        if (!getters.isEmpty()) {
            for (Map.Entry<String, Method> entry : getters.entrySet()) {
                String name = entry.getKey();
                Method getter = entry.getValue();
                Method setter = null;
                List<Method> setterList = setters.remove(name);
                if (setterList != null && setterList.size() != 0) {
                    for (int j = 0; j < setterList.size(); ++j) {
                        Method thisSetter = setterList.get(j);
                        Class<?> pinfo = thisSetter.getParameterTypes()[0];
                        if (getter.getReturnType().isPrimitive() && !pinfo.isPrimitive()
                                && getPrimitiveWrapperType(getter.getReturnType().getName()).getName().equals(pinfo.getName())) {
                            setter = thisSetter;
                            break;
                        } else if (!getter.getReturnType().isPrimitive() && pinfo.isPrimitive()
                                && getPrimitiveWrapperType(pinfo.getName()).getName().equals(getter.getReturnType().getName())) {
                            setter = thisSetter;
                            break;
                        } else if (getter.getReturnType().equals(pinfo)) {
                            setter = thisSetter;
                            break;
                        }
                    }
                }
                String lowerName = getLowerPropertyName(name);

                properties.add(
                        new PropertyInfo(lowerName, getPropertyReadType(getter), getPropertyWriteType(setter), getter, setter));
            }
        }
        if (!setters.isEmpty()) {
            setters.entrySet().stream().forEach(entry -> {
                final String lowerName = getLowerPropertyName(entry.getKey());
                entry.getValue().stream().forEach(method -> {
                    properties.add(new PropertyInfo(lowerName, null, method.getParameterTypes()[0], null, method.getName()));
                });
            });
        }
        return properties;
    }

    protected static boolean isGetter(Method minfo) {
        String name = minfo.getName();
        if ((name.length() > 3 && name.startsWith(GET)) || (name.length() > 2 && name.startsWith(IS))) {
            Class<?> returnType = minfo.getReturnType();

            // isBoolean() is not a getter for java.lang.Boolean
            if (name.startsWith(IS) && !returnType.isPrimitive()) {
                return false;
            }

            int params = minfo.getParameterTypes().length;
            if (params == 0 && !Void.TYPE.equals(returnType)) {
                return true;
            }
        }
        return false;
    }

    protected static boolean isSetter(Method minfo) {
        String name = minfo.getName();
        if ((name.length() > 3 && name.startsWith(SET))) {
            Class<?> returnType = minfo.getReturnType();

            int params = minfo.getParameterTypes().length;

            if (params == 1 && Void.TYPE.equals(returnType)) {
                return true;
            }
        }
        return false;
    }

    protected static String getUpperPropertyName(String name) {
        int start = name.startsWith(IS) ? 2 : 3;
        return name.substring(start);
    }

    protected static String getLowerPropertyName(String name) {
        // If the second character is upper case then we don't make
        // the first character lower case
        if (name == null || name.length() < 2) {
            return name;
        }
        char[] chars = name.toCharArray();
        if (Character.isUpperCase(chars[1])) {
            return name;
        } else {
            chars[0] = Character.toLowerCase(chars[0]);
            return new String(chars);
        }
    }

    protected Class<?> getPropertyReadType(Method getter) {
        if (getter == null) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), "Getter cannot be null");
        }
        return getter.getReturnType();
    }

    protected Class<?> getPropertyWriteType(Method setter) {
        if (setter == null) {
            return null;
        }
        return setter.getParameterTypes()[0];
    }

    private Set<Method> getMethods(Class<?> clazz) {
        return Arrays.stream(clazz.getMethods()).filter(method -> (method.getModifiers() & ACCEPTABLE_MODIFIERS) == 1)
                .collect(Collectors.toSet());
    }

    private static Class<?> getPrimitiveWrapperType(String primitiveName) {
        return PRIMITIVE_WRAPPER_TYPES.get(primitiveName);
    }
}
