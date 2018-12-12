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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.talend.daikon.avro.SchemaConstants;

public class DateConverter extends AsStringConverter<Long> {

    private final SimpleDateFormat format;

    public DateConverter(Schema.Field field) {
        super(field);
        String pattern = field.getProp(SchemaConstants.TALEND_COLUMN_PATTERN);
        if (pattern == null) {
            throw new RuntimeException("pattern mssing!");
        }
        format = new SimpleDateFormat(pattern);
    }

    @Override
    public Long convertToAvro(String value) {
        try {
            return StringUtils.isEmpty(value) ? null : format.parse(value).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String convertToDatum(Long value) {
        return value == null ? null : format.format(new Date(value));
    }

}
