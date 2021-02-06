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
package org.talend.components.assertion.service;

import org.talend.components.assertion.conf.Config;
import org.talend.sdk.component.api.record.Record;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateValidator extends Validator {

    public final static String DEFAULT_DATE_FORMAT = "yyyy/MM/dd HH:mm:ssX";

    // private SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT)
    private DateTimeFormatter dtf = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT).withZone(ZoneId.of("UTC"));

    @Override
    public boolean validate(Config.Condition condition, String expected, Object value, Record record) {
        ZonedDateTime zdtExpected = ZonedDateTime.parse(expected, dtf);
        ZonedDateTime zdtValue = null;

        if (value instanceof String) {
            zdtValue = ZonedDateTime.parse((String) value, dtf);
        } else if (value instanceof ZonedDateTime) {
            zdtValue = (ZonedDateTime) value;
        }

        int compare = zdtValue.compareTo(zdtExpected);
        switch (condition) {
        case EQUALS:
            return compare == 0;
        case INFERIOR:
            return compare < 0;
        case SUPERIOR:
            return compare > 0;
        default:
            throw new UnsupportedOperationException("Unspported date condition : " + condition);
        }
    }

    private static LocalDate convertFromDate(final Date date) {
        return ((Date) date).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public void setFormat(final String format) {
        this.dtf = DateTimeFormatter.ofPattern(format);
    }
}
