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
package org.talend.components.google.storage.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BlobNameBuilderTest {

    @Test
    void generateName() {
        final BlobNameBuilder builder = new BlobNameBuilder();
        final String generateName = builder.generateName("Hello");

        Assertions.assertTrue(generateName.startsWith("Hello"), generateName + " not start with Hello");
        Assertions.assertTrue(builder.isGenerated("Hello", generateName));
        Assertions.assertFalse(builder.isGenerated("HelloW", generateName));
        Assertions.assertFalse(builder.isGenerated("Hello", "Hello_XX"));

        Assertions.assertFalse(builder.isGenerated("Hello", "Hello_1234567890ABCDEF1234567890ABCDEF1234"));

        Assertions.assertNull(builder.revert(null));
        Assertions.assertEquals("Hello", builder.revert("Hello"));
        Assertions.assertEquals("Hello_", builder.revert("Hello_"));
        Assertions.assertEquals("_", builder.revert("_"));
        Assertions.assertEquals("Hello_1234567890ABCDEF1234567890ABCDEF1234",
                builder.revert("Hello_1234567890ABCDEF1234567890ABCDEF1234"));

        Assertions.assertEquals("Hello", builder.revert(builder.generateName("Hello")));
    }
}