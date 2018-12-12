/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.commons.converter;

import org.apache.avro.Schema;

public class BytesConverter extends AsStringConverter<byte[]> {

    public BytesConverter(Schema.Field field) {
        super(field);
    }

    @Override
    public byte[] convertToAvro(String value) {
        return value == null ? null : value.getBytes();
    }
}
