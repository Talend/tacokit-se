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
package org.talend.components.assertion.service;

import org.talend.sdk.component.api.record.Schema;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

public class Util {

    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.UTF_8);

    public static Class getClassFromType(Schema.Type type) {
        switch (type) {
        case ARRAY:
            return Collection.class;
        case STRING:
            return String.class;
        case BYTES:
            return byte[].class;
        case INT:
            return Integer.class;
        case LONG:
            return Long.class;
        case FLOAT:
            return Float.class;
        case DOUBLE:
            return Double.class;
        case BOOLEAN:
            return Boolean.class;
        case DATETIME:
            return ZonedDateTime.class;
        }

        throw new RuntimeException("The given type is not supported : " + type);
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();

        if (len % 2 != 0) {
            throw new RuntimeException("Expected bytes array is wrong. It should have an even length : " + s);
        }

        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static String byteArrayToString(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }
}
