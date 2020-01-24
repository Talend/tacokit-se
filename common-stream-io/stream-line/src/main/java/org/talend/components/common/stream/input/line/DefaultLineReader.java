/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * Default implementation for line reader.
 */
public class DefaultLineReader implements LineReader {

    /** line separator in reg exp form */
    private final Pattern regExpSeparator;

    /** current scanner */
    private Scanner scanner = null;

    public DefaultLineReader(String recordSeparator) {
        this(Pattern.compile(DefaultLineReader.escapeChars(recordSeparator)));
    }

    public DefaultLineReader(Pattern regExpSeparator) {
        this.regExpSeparator = regExpSeparator;
    }

    @Override
    public Iterator<String> read(InputStream reader) {
        this.close();
        this.scanner = new Scanner(reader).useDelimiter(this.regExpSeparator);
        return this.scanner;
    }

    @Override
    public void close() {
        if (this.scanner != null) {
            this.scanner.close();
            this.scanner = null;
        }
    }

    /**
     * Refine prefix and suffix for regular expression.
     * 
     * @param val : prefix of suffix expression.
     * @return expression with reg exp compatibility.
     */
    private static String escapeChars(String val) {
        val = val.replace("{", "\\{").replace("}", "\\}").replace("(", "\\(").replace(")", "\\)").replace("[", "\\[")
                .replace("]", "\\]").replace("$", "\\$");

        return val;
    }
}
