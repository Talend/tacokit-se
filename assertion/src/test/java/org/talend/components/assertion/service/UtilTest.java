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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class UtilTest {

    @Test
    void hexStringToByteArrayTest() {
        String sbytes = "000102030A7F80FFFEFDF6";
        byte[] bb = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x0A, 0x7F, (byte) 0x80, (byte) 0xFF, (byte) 0xFE, (byte) 0xFD,
                (byte) 0xF6 };

        final byte[] bytes = Util.hexStringToByteArray(sbytes);
        assertArrayEquals(bb, bytes);
    }

    @Test
    void byteArrayToString() {
        final String s = Util.byteArrayToString(new byte[] { 0x00, 0x01, 0x02, 0x03, 0x0A, 0x7F, (byte) 0x80, (byte) 0xFF,
                (byte) 0xFE, (byte) 0xFD, (byte) 0xF6 });
        String sbytes = "000102030A7F80FFFEFDF6";
        assertEquals(sbytes, s);
    }

}