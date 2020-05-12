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
package org.talend.components.netsuite.runtime.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.beans.EnumAccessor;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.GregorianCalendar;
import java.util.Optional;

public class ConverterFactory {

    private ConverterFactory() {
    }

    public static Converter<?, ?> getValueConverter(Class<?> valueClass, ObjectMapper objectMapper) {
        if (valueClass == Boolean.TYPE || valueClass == Boolean.class || valueClass == Integer.TYPE || valueClass == Integer.class
                || valueClass == Long.TYPE || valueClass == Long.class || valueClass == Double.TYPE || valueClass == Double.class
                || valueClass == String.class) {
            return new IdentityConverter<>(valueClass);
        } else if (valueClass == XMLGregorianCalendar.class) {
            return new XMLGregorianCalendarToDateTimeConverter();
        } else if (valueClass.isEnum()) {
            return new EnumToStringConverter<>((Class<Enum>) valueClass);
        } else if (!valueClass.isPrimitive()) {
            return new ObjectToJsonConverter<>(valueClass, objectMapper);
        }
        return new NullConverter<>(valueClass);
    }

    public static abstract class Converter<DatumT, RecordT> {

        private Class<DatumT> clazz;

        protected Converter(Class<DatumT> clazz) {
            this.clazz = clazz;
        }

        public Class<DatumT> getDatumClass() {
            return clazz;
        }

        public abstract DatumT convertToDatum(RecordT value);

        public abstract RecordT convertToRecordType(DatumT value);
    }

    public static class NullConverter<DatumT, RecordT> extends Converter<DatumT, RecordT> {

        public NullConverter(Class<DatumT> clazz) {
            super(clazz);
        }

        @Override
        public DatumT convertToDatum(RecordT value) {
            return null;
        }

        @Override
        public RecordT convertToRecordType(DatumT value) {
            return null;
        }

    }

    public static class ObjectToJsonConverter<DatumT> extends Converter<DatumT, String> {

        private final ObjectReader objectReader;

        private final ObjectWriter objectWriter;

        public ObjectToJsonConverter(Class<DatumT> clazz, ObjectMapper objectMapper) {
            super(clazz);
            this.objectWriter = objectMapper.writer().forType(clazz);
            this.objectReader = objectMapper.reader().forType(clazz);
        }

        @Override
        public DatumT convertToDatum(String value) {
            if (value == null) {
                return null;
            }
            try {
                return objectReader.readValue(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public String convertToRecordType(DatumT value) {
            return Optional.ofNullable(value).map(valueTemp -> {
                try {
                    return objectWriter.writeValueAsString(valueTemp);
                } catch (Exception e) {
                    throw new RuntimeException();
                }
            }).orElse(null);
        }
    }

    public static class EnumToStringConverter<DatumT extends Enum<DatumT>> extends Converter<DatumT, String> {

        private final EnumAccessor enumAccessor;

        public EnumToStringConverter(Class<DatumT> clazz) {
            super(clazz);
            this.enumAccessor = Beans.getEnumAccessor(clazz);
        }

        @Override
        public DatumT convertToDatum(String value) {
            return Optional.ofNullable(value).map(valueTemp -> {
                try {
                    return (DatumT) enumAccessor.getEnumValue(value);
                } catch (Exception e) {
                    return Enum.valueOf(getDatumClass(), value);
                }
            }).orElse(null);
        }

        @Override
        public String convertToRecordType(DatumT value) {
            return Optional.ofNullable(value).map(valueTemp -> {
                try {
                    return enumAccessor.getStringValue(valueTemp);
                } catch (IllegalArgumentException ex) {
                    return valueTemp.name();
                }
            }).orElse(null);
        }
    }

    public static class XMLGregorianCalendarToDateTimeConverter extends Converter<XMLGregorianCalendar, ZonedDateTime> {

        /** XML data type factory used. */
        protected final DatatypeFactory datatypeFactory;

        public XMLGregorianCalendarToDateTimeConverter() {
            super(XMLGregorianCalendar.class);
            try {
                datatypeFactory = DatatypeFactory.newInstance();
            } catch (DatatypeConfigurationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public XMLGregorianCalendar convertToDatum(ZonedDateTime value) {
            return Optional.ofNullable(value)
                    .map(valueTemp -> datatypeFactory.newXMLGregorianCalendar(GregorianCalendar.from(valueTemp))).orElse(null);
        }

        @Override
        public ZonedDateTime convertToRecordType(XMLGregorianCalendar value) {
            return Optional.ofNullable(value).map(valueTemp -> valueTemp.toGregorianCalendar().toZonedDateTime()).orElse(null);
        }
    }

    public static class IdentityConverter<DatumT> extends Converter<DatumT, DatumT> {

        public IdentityConverter(Class<DatumT> clazz) {
            super(clazz);
        }

        @Override
        public DatumT convertToDatum(DatumT value) {
            return value;
        }

        @Override
        public DatumT convertToRecordType(DatumT value) {
            return value;
        }
    }

}