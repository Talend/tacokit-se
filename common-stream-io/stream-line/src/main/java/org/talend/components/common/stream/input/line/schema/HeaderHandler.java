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
package org.talend.components.common.stream.input.line.schema;

import java.util.Iterator;
import java.util.function.Consumer;

public class HeaderHandler {

    private final int headerLines;

    /** header listener */
    private final Consumer<String> headerListener;

    public HeaderHandler(int headerLines, Consumer<String> headerListener) {
        this.headerLines = headerLines;
        this.headerListener = headerListener;
    }

    public void treat(Iterator<String> linesWithHeaders) {
        if (this.headerLines <= 0 || this.headerListener == null) {
            return;
        }
        // skip un-usefull lines
        for (int i = 1; i < this.headerLines && linesWithHeaders.hasNext(); i++) {
            linesWithHeaders.next();
        }
        if (linesWithHeaders.hasNext()) {
            final String headerLine = linesWithHeaders.next();
            this.headerListener.accept(headerLine);
        }
    }
}
