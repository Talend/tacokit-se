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

import lombok.Data;
import org.apache.commons.beanutils.MethodUtils;
import org.apache.commons.beanutils.expression.DefaultResolver;
import org.apache.commons.beanutils.expression.Resolver;
import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.client.NetSuiteException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Util class that provides methods for working with {@code beans}.
 */
public class Beans {

    private static final ConcurrentMap<Class<?>, BeanInfo> BEAN_INFO_CACHE = new ConcurrentHashMap<>();

    private static final Resolver PROPERTY_RESOLVER = new DefaultResolver();

    private Beans() {

    }

    /**
     * Get property descriptor for given instance of a bean and it's property name.
     *
     * @param target target bean which to get property for
     * @param name name of property
     * @return property descriptor or {@code null} if specified property was not found
     */
    public static PropertyInfo getPropertyInfo(Object target, String name) {
        return Optional.ofNullable(getBeanInfo(target.getClass())).map(beanInfo -> beanInfo.getProperty(name)).orElse(null);
    }

    /**
     * Get bean descriptor for given class.
     *
     * @param clazz class
     * @return bean descriptor
     */
    public static BeanInfo getBeanInfo(Class<?> clazz) {
        BeanInfo beanInfo = BEAN_INFO_CACHE.get(clazz);
        if (beanInfo == null) {
            BeanInfo newBeanInfo = loadBeanInfoForClass(clazz);
            if (BEAN_INFO_CACHE.putIfAbsent(clazz, newBeanInfo) == null) {
                beanInfo = newBeanInfo;
            } else {
                beanInfo = BEAN_INFO_CACHE.get(clazz);
            }
        }
        return beanInfo;
    }

    private static BeanInfo loadBeanInfoForClass(Class<?> clazz) {
        try {
            List<PropertyInfo> properties = BeanIntrospector.getInstance().getProperties(clazz.getName());
            return new BeanInfo(properties);
        } catch (Exception e) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                    "Cannot load bean info from class: " + clazz.getName(), e);
        }
    }

    /**
     * Set a value for given bean's property.
     *
     * @param target target bean
     * @param expr property path
     * @param value value to be set
     */
    public static void setProperty(Object target, String expr, Object value) {
        Object current = target;
        if (PROPERTY_RESOLVER.hasNested(expr)) {
            String currExpr = expr;
            while (PROPERTY_RESOLVER.hasNested(currExpr)) {
                String next = PROPERTY_RESOLVER.next(currExpr);
                Object obj = getSimpleProperty(current, next);
                if (obj != null) {
                    current = obj;
                    currExpr = PROPERTY_RESOLVER.remove(currExpr);
                }
            }
            if (current != null) {
                setSimpleProperty(current, currExpr, value);
            }
        } else {
            if (current != null) {
                setSimpleProperty(current, expr, value);
            }
        }
    }

    /**
     * Get value of given bean's property.
     *
     * @param target target bean
     * @param expr property path
     * @return value
     */
    public static Object getProperty(Object target, String expr) {
        Object current = target;
        if (PROPERTY_RESOLVER.hasNested(expr)) {
            String currExpr = expr;
            while (PROPERTY_RESOLVER.hasNested(currExpr) && current != null) {
                String next = PROPERTY_RESOLVER.next(currExpr);
                current = getSimpleProperty(current, next);
                currExpr = PROPERTY_RESOLVER.remove(currExpr);
            }
            return current != null ? getSimpleProperty(current, currExpr) : null;
        } else {
            return getSimpleProperty(current, expr);
        }
    }

    /**
     * Get value of given bean's property.
     *
     * @param target target bean
     * @param name name of property
     * @return value
     */
    public static Object getSimpleProperty(Object target, String name) {
        return getPropertyAccessor(target).get(target, name);
    }

    /**
     * Set a value for given bean's property.
     *
     * @param target target bean
     * @param name name of property
     * @param value to be set
     */
    public static void setSimpleProperty(Object target, String name, Object value) {
        getPropertyAccessor(target).set(target, name, value);
    }

    /**
     * Get property accessor for given object.
     *
     * @param target target object
     * @param <T> type of object
     * @return property accessor
     */
    @SuppressWarnings("unchecked")
    protected static <T> PropertyAccessor<T> getPropertyAccessor(T target) {
        return target instanceof PropertyAccess ? (PropertyAccessor<T>) ((PropertyAccess) target).getPropertyAccessor()
                : (PropertyAccessor<T>) ReflectPropertyAccessor.INSTANCE;
    }

    /**
     * Get enum accessor for given enum class.
     *
     * @param clazz enum class
     * @return enum accessor
     */
    public static <T extends Enum<?>> EnumAccessor<T> getEnumAccessor(Class<T> clazz) {
        return getEnumAccessorImpl(clazz);
    }

    /**
     * Get enum accessor for given enum class.
     *
     * @param clazz enum class
     * @return enum accessor
     */
    protected static <T extends Enum<?>> AbstractEnumAccessor<T> getEnumAccessorImpl(Class<T> clazz) {
        EnumAccessor<T> accessor = null;
        Method m;
        try {
            m = clazz.getDeclaredMethod("getEnumAccessor", new Class[0]);
            if (!m.getReturnType().equals(EnumAccessor.class)) {
                throw new NoSuchMethodException();
            }
            try {
                accessor = (EnumAccessor<T>) m.invoke(new Object[0]);
            } catch (IllegalAccessException | InvocationTargetException e) {
                // TODO: Add logging
                // LOG.error("Failed to get optimized EnumAccessor: enum class " + clazz.getName(), e);
            }
        } catch (NoSuchMethodException e) {
        }
        return accessor != null ? new OptimizedEnumAccessor<>(clazz, accessor) : new ReflectEnumAccessor<>(clazz);
    }

    /**
     * Get mapper which maps an enum constant to string value.
     *
     * @param clazz enum class
     * @return mapper
     */
    public static <T extends Enum<?>> Function<T, String> getEnumToStringMapper(Class<T> clazz) {
        return getEnumAccessorImpl(clazz).getToStringMapper();
    }

    /**
     * Get mapper which maps string value to an enum constant.
     *
     * @param clazz enum class
     * @return mapper
     */
    public static <T extends Enum<?>> Function<String, T> getEnumFromStringMapper(Class<T> clazz) {
        return getEnumAccessorImpl(clazz).getFromStringMapper();
    }

    /**
     * Convert initial letter of given string value to upper case.
     *
     * @param value source value to be converted
     * @return converted value
     */
    public static String toInitialUpper(String value) {
        return value.substring(0, 1).toUpperCase() + value.substring(1);
    }

    /**
     * Convert initial letter of given string value to lower case.
     *
     * @param value source value to be converted
     * @return converted value
     */
    public static String toInitialLower(String value) {
        return value.substring(0, 1).toLowerCase() + value.substring(1);
    }

    /**
     * Property accessor which uses reflection to access properties.
     */
    protected static class ReflectPropertyAccessor implements PropertyAccessor<Object> {

        private static final String EXCEPTION_MESSAGE = "Cannot invoke %s.%s on bean class '%s' - %s - had objects of type \"%s\" but expected signature \"%s\"";

        protected static final ReflectPropertyAccessor INSTANCE = new ReflectPropertyAccessor();

        /** An empty class array */
        private static final Class<?>[] EMPTY_CLASS_PARAMETERS = new Class[0];

        /** An empty object array */
        private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

        @Override
        public Object get(Object target, String name) {
            if (name == null) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "No name specified for bean class " + target.getClass().getName());
            }

            // Retrieve the property getter method for the specified property
            BeanInfo metaData = Beans.getBeanInfo(target.getClass());
            PropertyInfo descriptor = metaData.getProperty(name);
            if (descriptor == null) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "Unknown property " + name + " on class " + target.getClass().getName());
            }
            Method readMethod = getReadMethod(target.getClass(), descriptor);
            if (readMethod == null) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "Property " + name + " has no getter method in class " + target.getClass().getName());
            }

            // Call the property getter and return the value
            try {
                return invokeMethod(readMethod, target, EMPTY_OBJECT_ARRAY);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "Error while invoking getter method");
            }
        }

        @Override
        public void set(Object target, String name, Object value) {
            if (name == null) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "No name specified for bean class " + target.getClass().getName());
            }

            // Retrieve the property setter method for the specified property
            BeanInfo metaData = Beans.getBeanInfo(target.getClass());
            PropertyInfo descriptor = metaData.getProperty(name);
            if (descriptor == null) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "Unknown property " + name + " on class " + target.getClass().getName());
            }
            Method writeMethod = getWriteMethod(target.getClass(), descriptor);
            if (writeMethod == null) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                        "Property " + name + " has no setter method in class " + target.getClass().getName());
            }

            // Call the property setter method
            Object[] values = new Object[1];
            values[0] = value;

            try {
                invokeMethod(writeMethod, target, values);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "Error while invoking getter method");
            }
        }

        /** This just catches and wraps IllegalArgumentException. */
        private Object invokeMethod(Method method, Object bean, Object[] values)
                throws IllegalAccessException, InvocationTargetException {

            try {
                return method.invoke(bean, values);
            } catch (IllegalArgumentException cause) {
                if (bean == null) {
                    throw new IllegalArgumentException(
                            "No bean specified - this should have been checked before reaching this method");
                }
                StringBuilder valueString = new StringBuilder();
                if (values != null) {
                    for (int i = 0; i < values.length; i++) {
                        if (i > 0) {
                            valueString.append(", ");
                        }
                        valueString.append(values[i].getClass().getName());
                    }
                }
                StringBuilder expectedString = new StringBuilder();
                Class<?>[] parTypes = method.getParameterTypes();
                if (parTypes != null) {
                    for (int i = 0; i < parTypes.length; i++) {
                        if (i > 0) {
                            expectedString.append(", ");
                        }
                        expectedString.append(parTypes[i].getName());
                    }
                }
                throw new IllegalArgumentException(String.format(EXCEPTION_MESSAGE, method.getDeclaringClass().getName(),
                        method.getName(), bean.getClass(), cause.getMessage(), valueString, expectedString));
            }
        }

        /**
         * <p>
         * Return an accessible property getter method for this property,
         * if there is one; otherwise return <code>null</code>.
         * </p>
         *
         * @param clazz The class of the read method will be invoked on
         * @param descriptor Property descriptor to return a getter for
         * @return The read method
         */
        Method getReadMethod(Class<?> clazz, PropertyInfo descriptor) {
            return (MethodUtils.getAccessibleMethod(clazz, descriptor.getReadMethodName(), EMPTY_CLASS_PARAMETERS));
        }

        /**
         * <p>
         * Return an accessible property setter method for this property,
         * if there is one; otherwise return <code>null</code>.
         * </p>
         *
         * @param clazz The class of the read method will be invoked on
         * @param descriptor Property descriptor to return a setter for
         * @return The write method
         */
        Method getWriteMethod(Class<?> clazz, PropertyInfo descriptor) {
            return (MethodUtils.getAccessibleMethod(clazz, descriptor.getWriteMethodName(),
                    new Class[] { descriptor.getWriteType() }));
        }
    }

    /**
     * Base class for enum accessors.
     */
    @Data
    protected static abstract class AbstractEnumAccessor<T> implements EnumAccessor<T> {

        protected Class<?> enumClass;

        protected Function<T, String> toStringMapper;

        protected Function<String, T> fromStringMapper;

        protected AbstractEnumAccessor(Class<T> enumClass) {
            this.enumClass = enumClass;
            toStringMapper = input -> getStringValue(input);
            fromStringMapper = input -> getEnumValue(input);
        }
    }

    /**
     * Enum accessor which uses reflection to access enum values.
     *
     * <p>
     * Enum classes generated from NetSuite's XML schemas have following methods
     * to access enum values:
     * <ul>
     * <li>{@code String value()) - get NetSuite specific string value of enum</li>
     *
    <li>{@code Enum fromValue(String)) - get enum constant for NetSuite specific string value</li>
     *
    </ul>
     */
    public static class ReflectEnumAccessor<T extends Enum<?>> extends AbstractEnumAccessor<T> {

        private static final String FROM_VALUE = "fromValue";

        private static final String VALUE = "value";

        public ReflectEnumAccessor(Class<T> enumClass) {
            super(enumClass);
        }

        @Override
        public String getStringValue(T enumValue) {
            try {
                return (String) MethodUtils.invokeExactMethod(enumValue, VALUE, null);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e.getTargetException();
                }
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "Error while invoking getter method", e.getTargetException());
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), "No such method " + VALUE);
            }
        }

        @Override
        public T getEnumValue(String value) {
            try {
                return (T) MethodUtils.invokeExactStaticMethod(enumClass, FROM_VALUE, value);
            } catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) e.getTargetException();
                }
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "Error while invoking getter method", e.getTargetException());
            } catch (NoSuchMethodException | IllegalAccessException e) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                        "No such method " + FROM_VALUE);
            }
        }
    }

    public static class OptimizedEnumAccessor<T extends Enum<?>> extends AbstractEnumAccessor<T> {

        private EnumAccessor<T> accessor;

        public OptimizedEnumAccessor(Class<T> enumClass, EnumAccessor<T> accessor) {
            super(enumClass);
            this.accessor = accessor;
        }

        @Override
        public String getStringValue(T enumValue) {
            return accessor.getStringValue(enumValue);
        }

        @Override
        public T getEnumValue(String value) {
            return accessor.getEnumValue(value);
        }
    }
}
