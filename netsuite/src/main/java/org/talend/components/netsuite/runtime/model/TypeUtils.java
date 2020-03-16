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
package org.talend.components.netsuite.runtime.model;

import org.talend.components.netsuite.runtime.client.NetSuiteException;

import javax.xml.bind.annotation.XmlSeeAlso;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Provides utility methods to work with NetSuite native data object types.
 */
public class TypeUtils {

    private TypeUtils() {
    }

    /**
     * Traverse XML data object hierarchy and collect all descendants of a root class.
     *
     * @param rootClass root class of type hierarchy
     * @param clazz type class to be processed
     * @param classes set to collect type classes
     */
    public static void collectXmlTypes(Class<?> rootClass, Class<?> clazz, Set<Class<?>> classes) {
        if (classes.contains(clazz)) {
            return;
        }

        if (clazz != rootClass && rootClass.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
            classes.add(clazz);
        }

        XmlSeeAlso xmlSeeAlso = clazz.getAnnotation(XmlSeeAlso.class);
        if (xmlSeeAlso != null) {
            Collection<Class<?>> referencedClasses = new HashSet<>(Arrays.<Class<?>> asList(xmlSeeAlso.value()));
            for (Class<?> referencedClass : referencedClasses) {
                collectXmlTypes(rootClass, referencedClass, classes);
            }
        }
    }

    /**
     * Create new instance of given class.
     *
     * @param clazz target class to instantiate
     * @param <T> type of instance
     * @return instance of class
     * @throws NetSuiteException if an error occurs during instantiation
     */
    public static <T> T createInstance(Class<T> clazz) throws NetSuiteException {
        try {
            return clazz.cast(clazz.newInstance());
        } catch (IllegalAccessException | InstantiationException e) {
            throw new NetSuiteException("Failed to instantiate object: " + clazz, e);
        }
    }

}