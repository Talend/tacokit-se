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
package org.talend.components.adlsgen2.common.format.unknown;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.Configuration;

public class UnknownIterator implements Iterator<Record> {

    private UnknownConverter converter;

    private BufferedReader reader;

    private String current;

    private UnknownIterator(BufferedReader ureader) {
        reader = ureader;
        converter = UnknownConverter.of();
    }

    @Override
    public boolean hasNext() {
        try {
            current = reader.readLine();
            if (current == null) {
                reader.close();
                return false;
            }
            return true;
        } catch (IOException e) {
            return false; // last call when stream is closed
        }
    }

    @Override
    public Record next() {
        if ((current != null) || hasNext()) {
            Record r = converter.toRecord(current);
            current = null;
            return r;
        } else {
            return null;
        }
    }

    public static class Builder {

        private Builder() {
        }

        public static UnknownIterator.Builder of() {
            return new UnknownIterator.Builder();
        }

        public UnknownIterator.Builder withConfiguration(
                @Configuration("unknownConfiguration") final UnknownConfiguration configuration) {
            return this;
        }

        public UnknownIterator parse(InputStream in) {
            // TODO manage fileEncoding
            return new UnknownIterator(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
        }

        public UnknownIterator parse(String content) {
            return new UnknownIterator(new BufferedReader(new StringReader(content)));
        }
    }
}
