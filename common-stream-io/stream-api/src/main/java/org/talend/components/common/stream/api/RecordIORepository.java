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
package org.talend.components.common.stream.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonValue;

import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.sdk.component.api.service.Service;

/**
 * Service to group record reader and writer suppliers (one reader/writer for each configuration).
 */
@Service
public class RecordIORepository {

    @Service
    private JsonReaderFactory jsonReaderFactory;

    @Service
    private Messages i18n;

    /** all readers suppliers */
    private final ConcurrentMap<Class<? extends ContentFormat>, RecordReaderSupplier> readers = new ConcurrentHashMap<>();

    /** all writers suppliers */
    private final ConcurrentMap<Class<? extends ContentFormat>, RecordWriterSupplier> writers = new ConcurrentHashMap<>();

    /**
     * Load json file to get reader/writer class.
     * Json schema is of form :
     * {
     * "[configuration class]": { "reader": "[reader supplier class]", "writer": "[writer supplier class]" }
     * }
     */
    @PostConstruct
    public void init() {
        final ClassLoader loader = RecordIORepository.class.getClassLoader();
        try {
            Collections.list(loader.getResources("TALEND-INF/components/format.json")).stream() //
                    .map(this::readJson) // url -> json object
                    .forEach((JsonObject json) -> this.loadJson(loader, json)); // load json to update readers & writers
        } catch (IOException exIO) {
            throw new UncheckedIOException(this.i18n.cantLoad("TALEND-INF/components/format.json"), exIO);
        }
    }

    /**
     * Load json content that config a reader/writer.
     * 
     * @param loader : current class loader.
     * @param json : json content for a reader/writers declaration << { "[config class name]": {
     * "reader": "[ReaderSupplierClassName]", "writer": "[WriterSupplierClassName]" } }
     */
    private void loadJson(ClassLoader loader, JsonObject json) {
        json.entrySet().forEach((Entry<String, JsonValue> e) -> {
            this.loadClasses(loader, e.getKey(), e.getValue());
        });
    }

    /**
     * Load a reader & writer.
     * 
     * @param loader : class loader.
     * @param contentFormatClasses : format class name.
     * @param value : reader & writer class
     */
    private void loadClasses(ClassLoader loader, String contentFormatClasses, JsonValue value) {

        final Class<? extends ContentFormat> contentFormatClass = this.loadClass(ContentFormat.class, loader,
                contentFormatClasses);
        if (value.getValueType() != JsonValue.ValueType.OBJECT) {
            throw new IllegalArgumentException("json pb : " + value.toString() + " is not a json object");
        }
        final JsonObject jsonObject = value.asJsonObject();

        if (jsonObject.containsKey("reader")) {
            final String readerClassName = jsonObject.getString("reader");
            final RecordReaderSupplier reader = this.newInstance(RecordReaderSupplier.class, loader, readerClassName);
            this.readers.put(contentFormatClass, reader);
        }

        if (jsonObject.containsKey("writer")) {
            final String writerClassName = jsonObject.getString("writer");
            final RecordWriterSupplier writer = this.newInstance(RecordWriterSupplier.class, loader, writerClassName);
            this.writers.put(contentFormatClass, writer);
        }
    }

    /**
     * Build new instance of a class (assuming there's a no arg constructor.
     * 
     * @param baseClazz
     * @param loader
     * @param name
     * @param <T>
     * @return
     */
    private <T> T newInstance(Class<T> baseClazz, ClassLoader loader, String name) {
        try {
            final Class<? extends T> clazz = this.loadClass(baseClazz, loader, name);
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new IllegalArgumentException(i18n.cantInstanciate(name), ex);
        }
    }

    private <T> Class<? extends T> loadClass(Class<T> baseClazz, ClassLoader loader, String name) {
        try {
            final Class<?> formatClass = loader.loadClass(name);
            if (!baseClazz.isAssignableFrom(formatClass)) {
                throw new IllegalArgumentException(name + " is not a " + baseClazz.getName() + " class");
            }
            return (Class<? extends T>) formatClass;
        } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException(name + " class not found", ex);
        }
    }

    private JsonObject readJson(URL urlFormat) {
        try (final InputStream input = urlFormat.openStream(); //
                final JsonReader reader = jsonReaderFactory.createReader(input)) {
            return reader.readObject();
        } catch (final IOException exIO) {
            throw new UncheckedIOException("unable to read " + urlFormat.getPath(), exIO);
        }
    }

    public <T extends ContentFormat> RecordReaderSupplier findReader(Class<T> clazz) {
        return this.readers.get(clazz);
    }

    public <T extends ContentFormat> RecordWriterSupplier findWriter(Class<T> clazz) {
        return this.writers.get(clazz);
    }

}
