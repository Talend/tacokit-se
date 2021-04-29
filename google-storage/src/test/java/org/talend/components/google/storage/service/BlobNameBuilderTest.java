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

import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @MethodSource("provideNames")
    void generateNameWithExtension(final String input, final String pattern) {
        final BlobNameBuilder builder = new BlobNameBuilder();
        final String generateName = builder.generateName(input);
        Assertions.assertTrue(generateName.matches(pattern),
                "Wrong generated name '" + generateName + "'" + " for '" + input + "'");
        Assertions.assertTrue(builder.isGenerated(input, generateName));

        Assertions.assertEquals(input, builder.revert(generateName));
    }

    private static Stream<Arguments> provideNames() {
        return Stream.of( //
                Arguments.of("Hello.csv", "^Hello_[0-9a-f\\-]{36}\\.csv$"), //
                Arguments.of("Hello._", "^Hello_[0-9a-f\\-]{36}\\._$"), //
                Arguments.of("Hello.", "^Hello_[0-9a-f\\-]{36}\\.$"), //
                Arguments.of("Hello", "^Hello_[0-9a-f\\-]{36}$"), //
                Arguments.of(".gitignore", "^_[0-9a-f\\-]{36}\\.gitignore$"), //
                Arguments.of(".", "^_[0-9a-f\\-]{36}\\.$"));
    }
}