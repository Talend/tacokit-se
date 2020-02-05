/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream.output.json;

import org.talend.components.common.stream.api.output.FormatWriter;
import org.talend.components.common.stream.api.output.RecordByteWriter;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.sdk.component.api.record.Record;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonWriter extends RecordByteWriter<JsonConfiguration> {

    public JsonWriter(JsonConfiguration config, TargetFinder target) {
        super(new RecordToByte(), new JsonFormatWriter(), config, target);
    }

    static class JsonFormatWriter implements FormatWriter<JsonConfiguration> {

        /**
         * Start json output with starting an array.
         * 
         * @param config : null for this.
         * @param first : first record.
         * @return start array.
         */
        @Override
        public byte[] start(JsonConfiguration config, Record first) {
            return ("[" + System.lineSeparator()).getBytes();
        }

        @Override
        public byte[] between(JsonConfiguration config) {
            return ("," + System.lineSeparator()).getBytes();
        }

        /**
         * End json output with ending this array.
         * 
         * @param config : null for this.
         * @return end array.
         */
        @Override
        public byte[] end(JsonConfiguration config) {
            return (System.lineSeparator() + "]").getBytes();
        }
    }

}
