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

import java.io.InputStream;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.talend.components.common.stream.input.line.schema.HeaderHandler;

/**
 * Default implementation for line reader.
 */
public class DefaultLineReader implements LineReader {

    /** line separator in reg exp form */
    private final Pattern regExpSeparator;

    /** charset name. */
    private final String chartSetName;

    /** headers treatment. */
    private final HeaderHandler headers;

    /** current scanner */
    private Scanner scanner = null;

    public DefaultLineReader(String recordSeparator, String chartSetName, HeaderHandler headers) {
        this(Pattern.compile(Pattern.quote(recordSeparator)), chartSetName, headers);
    }

    public DefaultLineReader(Pattern regExpSeparator, String chartSetName, HeaderHandler headers) {
        this.regExpSeparator = regExpSeparator;
        this.chartSetName = chartSetName;
        this.headers = headers;
    }

    @Override
    public Iterator<String> read(InputStream reader) {
        this.close();
        this.scanner = new Scanner(reader, this.chartSetName).useDelimiter(this.regExpSeparator);
        this.headers.treat(this.scanner);
        return this.scanner;
    }

    @Override
    public void close() {
        if (this.scanner != null) {
            this.scanner.close();
            this.scanner = null;
        }
    }
}
