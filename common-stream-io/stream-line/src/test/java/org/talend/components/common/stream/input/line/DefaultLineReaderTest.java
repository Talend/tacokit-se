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
package org.talend.components.common.stream.input.line;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.common.stream.input.line.schema.HeaderHandler;

class DefaultLineReaderTest {

    @ParameterizedTest
    @MethodSource("lineProvider")
    void read(String separator, String line, List<String> list) {
        HeaderHandler handler = new HeaderHandler(0, null);
        DefaultLineReader reader = new DefaultLineReader(separator, "UTF-8", handler);

        ByteArrayInputStream lineReader = new ByteArrayInputStream(line.getBytes(Charset.forName("UTF-8")));

        final Iterator<String> lines = reader.read(lineReader);
        int index = 0;
        while (lines.hasNext()) {
            String l = lines.next();
            Assertions.assertEquals(list.get(index), l);
            index++;
        }
    }

    static Stream<Arguments> lineProvider() {
        return Stream.of(Arguments.arguments("\n", "Hello\nWorld\nTDI", Arrays.asList("Hello", "World", "TDI")),
                Arguments.arguments("\r\n", "Hello\r\nWorld\r\nTDI", Arrays.asList("Hello", "World", "TDI")));
    }

}