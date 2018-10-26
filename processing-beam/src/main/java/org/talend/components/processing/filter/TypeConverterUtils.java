// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.processing.filter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

// TODO this is a debug class, it should be revamp during the implementation TCOMP-283
class TypeConverterUtils {

    static Boolean parseToBoolean(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            if (StringUtils.isBlank((String) value)) {
                return null;
            } else if ("1".equals(value)) {
                return true;
            } else {
                return Boolean.parseBoolean((String) value);
            }
        } else {
            return Boolean.valueOf(value.toString());
        }
    }

    static Byte parseToByte(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).byteValue();
        } else if (value instanceof Boolean) {
            return (byte) (((Boolean) value) ? 1 : 0);
        } else if (value instanceof String) {
            return Byte.decode((String) value).byteValue();
        } else {
            return Byte.valueOf(value.toString());
        }
    }

    static ByteBuffer parseToByteBuffer(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        } else {
            return ByteBuffer.wrap(value.toString().getBytes());
        }
    }

    static Character parseToCharacter(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Character) {
            return (Character) value;
        } else {
            return Character.valueOf(value.toString().charAt(0));
        }
    }

    static Double parseToDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else {
            return Double.parseDouble(value.toString());
        }
    }

    static Float parseToFloat(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else {
            return Float.parseFloat(value.toString());
        }
    }

    static BigDecimal parseToBigDecimal(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Byte) {
            return new BigDecimal((Byte) value);
        } else if (value instanceof Double) {
            return new BigDecimal((Double) value);
        } else if (value instanceof Float) {
            return new BigDecimal((Float) value);
        } else if (value instanceof Integer) {
            return new BigDecimal((Integer) value);
        } else if (value instanceof Long) {
            return new BigDecimal((Long) value);
        } else if (value instanceof Short) {
            return new BigDecimal((Short) value);
        } else {
            return new BigDecimal(value.toString());
        }
    }

    static Integer parseToInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else {
            return Integer.parseInt(value.toString());
        }
    }

    static Long parseToLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            return Long.parseLong(value.toString());
        }
    }

    static Short parseToShort(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        } else {
            return Short.parseShort(value.toString());
        }
    }

    static String parseToString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof ByteBuffer) {
            return new String(((ByteBuffer) value).array());
        } else {
            return value.toString();
        }
    }

    static <T> T parseTo(Object value, Class<T> clazz) {
        if (clazz == null) {
            return (T) parseToString(value);
        } else if (value != null && clazz.equals(value.getClass())) {
            return (T) value;
        } else if (clazz.equals(Boolean.class)) {
            return (T) parseToBoolean(value);
        } else if (clazz.equals(Byte.class)) {
            return (T) parseToByte(value);
        } else if (clazz.equals(ByteBuffer.class)) {
            return (T) parseToByteBuffer(value);
        } else if (clazz.equals(Character.class)) {
            return (T) parseToCharacter(value);
        } else if (clazz.equals(Date.class)) {
            // TODO need a defintion of date type
        } else if (clazz.equals(Double.class)) {
            return (T) parseToDouble(value);
        } else if (clazz.equals(Float.class)) {
            return (T) parseToFloat(value);
        } else if (clazz.equals(BigDecimal.class)) {
            return (T) parseToBigDecimal(value);
        } else if (clazz.equals(Integer.class)) {
            return (T) parseToInteger(value);
        } else if (clazz.equals(Long.class)) {
            return (T) parseToLong(value);
        } else if (clazz.equals(Short.class)) {
            return (T) parseToShort(value);
        } else if (clazz.equals(String.class)) {
            return (T) parseToString(value);
        } else { // Object
            // TODO to define
        }
        return (T) value;
    }

    static <T extends Comparable<T>> Class<T> getComparableClass(Object value) {
        if (value instanceof Boolean) {
            return (Class<T>) Boolean.class;
        } else if (value instanceof Byte) {
            return (Class<T>) Byte.class;
        } else if (value instanceof ByteBuffer) {
            return (Class<T>) ByteBuffer.class;
        } else if (value instanceof Character) {
            return (Class<T>) Character.class;
        } else if (value instanceof Date) {
            // TODO need a defintion of date type
        } else if (value instanceof Double) {
            return (Class<T>) Double.class;
        } else if (value instanceof Float) {
            return (Class<T>) Float.class;
        } else if (value instanceof BigDecimal) {
            return (Class<T>) BigDecimal.class;
        } else if (value instanceof Integer) {
            return (Class<T>) Integer.class;
        } else if (value instanceof Long) {
            return (Class<T>) Long.class;
        } else if (value instanceof Short) {
            return (Class<T>) Short.class;
        } else if ((value instanceof String)) {
            return (Class<T>) String.class;
        } else {
            // TODO to define
        }
        return null;
    }

}
