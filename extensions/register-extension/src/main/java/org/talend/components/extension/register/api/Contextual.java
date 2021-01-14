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
package org.talend.components.extension.register.api;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.talend.sdk.component.runtime.base.Delegated;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.serialization.ContainerFinder;
import org.talend.sdk.component.runtime.serialization.EnhancedObjectInputStream;
import org.talend.sdk.component.runtime.serialization.LightContainer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static lombok.AccessLevel.PRIVATE;
import static org.talend.sdk.component.runtime.base.lang.exception.InvocationExceptionWrapper.toRuntimeException;

@RequiredArgsConstructor(access = PRIVATE)
public class Contextual implements InvocationHandler, Serializable {

    @Getter
    private final String plugin;

    @Getter
    private final Object delegate;

    @Getter
    private final ResultWrapper methodsHandler;

    private volatile ClassLoader loader;

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) {
        final Thread thread = Thread.currentThread();
        final ClassLoader oldLoader = thread.getContextClassLoader();
        thread.setContextClassLoader(findLoader());
        try {
            final Object result = method.invoke(delegate, args);
            if (methodsHandler != null && methodsHandler.handles(method)) {
                return methodsHandler.wrap(result);
            }
            return result;
        } catch (final IllegalAccessException e) {
            throw new IllegalStateException(e);
        } catch (final InvocationTargetException e) {
            throw toRuntimeException(e);
        } finally {
            thread.setContextClassLoader(oldLoader);
        }
    }

    private ClassLoader findLoader() {
        if (loader == null) {
            synchronized (this) {
                if (loader == null) {
                    loader = ContainerFinder.Instance.get().find(plugin).classloader();
                }
            }
        }
        return loader;
    }

    Object writeReplace() throws ObjectStreamException {
        return new SerializableInstance(plugin, toBytes(delegate), methodsHandler == null ? null : toBytes(methodsHandler));
    }

    private byte[] toBytes(final Object delegate) {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (final ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeObject(delegate);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

    public static <T> T proxy(final Class<T> type, final String plugin, final Object delegate,
            final ResultWrapper methodsHandler) {
        final Stream<Class<?>> additionalImpl = Delegated.class.isInstance(delegate) ? Stream.of(Delegated.class)
                : Stream.empty();
        return type.cast(Proxy.newProxyInstance(ofNullable(type.getClassLoader()).orElseGet(ClassLoader::getSystemClassLoader),
                Stream.concat(Stream.of(type, Serializable.class), additionalImpl).toArray(Class<?>[]::new),
                new Contextual(plugin, delegate, methodsHandler)));
    }

    @RequiredArgsConstructor
    public static class SerializableInstance implements Serializable {

        private final String plugin;

        private final byte[] bytesMapper;

        private final byte[] bytesHandler;

        public Object readResolve() throws ObjectStreamException {
            final ContainerFinder containerFinder = ContainerFinder.Instance.get();
            final LightContainer container = containerFinder.find(plugin);
            final ClassLoader loader = container.classloader();
            final Thread thread = Thread.currentThread();
            final ClassLoader oldLoader = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                return new Contextual(plugin, load(bytesMapper, loader, Mapper.class),
                        load(bytesHandler, loader, ResultWrapper.class));
            } finally {
                thread.setContextClassLoader(oldLoader);
            }
        }

        private <T> T load(final byte[] bytes, final ClassLoader loader, final Class<T> expected) {
            try (final ObjectInputStream stream = new EnhancedObjectInputStream(new ByteArrayInputStream(bytes), loader)) {
                return expected.cast(stream.readObject());
            } catch (final ClassNotFoundException | IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public interface ResultWrapper extends Serializable {

        boolean handles(Method method);

        Object wrap(Object value);
    }
}
