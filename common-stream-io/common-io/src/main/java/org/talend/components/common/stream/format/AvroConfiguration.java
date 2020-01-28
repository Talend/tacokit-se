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
package org.talend.components.common.stream.format;

public class AvroConfiguration implements ContentFormat {

    static {
        try {
            Class.forName("org.talend.components.common.stream.input.avro.AvroReaderSupplier");
            Class.forName("org.talend.components.common.stream.output.avro.AvroWriterSupplier");
        } catch (ClassNotFoundException e) {
            // not exist if no dependencies to stream-csv, csv format is not used.
        }
    }
}
