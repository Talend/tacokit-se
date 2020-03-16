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
package org.talend.components.netsuite.runtime.model.search;

import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.beans.Beans;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;

/**
 * Search field adapter for {@code SearchDateField} and {@code SearchDateCustomField}.
 */
public class SearchDateFieldAdapter<T> extends SearchFieldAdapter<T> {

    private DatatypeFactory datatypeFactory;

    private String dateFormatPattern = "yyyy-MM-dd";

    private String timeFormatPattern = "HH:mm:ss";

    public SearchDateFieldAdapter(BasicMetaData metaData, SearchFieldType fieldType, Class<T> fieldClass) {
        super(metaData, fieldType, fieldClass);

        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new NetSuiteException("Failed to create XML data type factory", e);
        }
    }

    @Override
    public T populate(T fieldObject, String internalId, String operatorName, List<String> values) {
        T nsObject = fieldObject != null ? fieldObject : createField(internalId);

        SearchFieldOperatorName operatorQName = new SearchFieldOperatorName(operatorName);

        if (SearchFieldOperatorType.PREDEFINED_DATE.dataTypeEquals(operatorQName.getDataType())) {
            Beans.setSimpleProperty(nsObject, "predefinedSearchValue",
                    metaData.getSearchFieldOperatorByName(fieldType.getFieldTypeName(), operatorName));
        } else {
            if (values != null && values.size() != 0) {
                Beans.setSimpleProperty(nsObject, SEARCH_VALUE, convertDateTime(values.get(0)));
                String temp;
                if (values.size() > 1 && (temp = values.get(1)) != null && !temp.isEmpty()) {
                    Beans.setSimpleProperty(nsObject, SEARCH_VALUE_2, convertDateTime(temp));
                }
            }

            Beans.setSimpleProperty(nsObject, OPERATOR,
                    metaData.getSearchFieldOperatorByName(fieldType.getFieldTypeName(), operatorName));
        }

        return nsObject;
    }

    protected XMLGregorianCalendar convertDateTime(String input) {
        String valueToParse = input;
        String dateTimeFormatPattern = dateFormatPattern + " " + timeFormatPattern;
        if (input.length() == dateFormatPattern.length()) {
            dateTimeFormatPattern = dateFormatPattern;
        } else if (input.length() == timeFormatPattern.length()) {
            Instant now = Instant.now();
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(dateFormatPattern);
            valueToParse = dateFormatter.format(now) + " " + input;
        }

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormatPattern);

        TemporalAccessor dateTime;
        try {
            dateTime = dateTimeFormatter.parse(valueToParse);
        } catch (IllegalArgumentException e) {
            // TODO: Update Exception
            throw new RuntimeException(e);
            // throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
            // NetSuiteRuntimeI18n.MESSAGES.getMessage("error.searchDateField.invalidDateTimeFormat",
            // valueToParse));
        }
        XMLGregorianCalendar xts = datatypeFactory.newXMLGregorianCalendar();
        xts.setYear(dateTime.get(ChronoField.YEAR));
        xts.setMonth(dateTime.get(ChronoField.MONTH_OF_YEAR));
        xts.setDay(dateTime.get(ChronoField.DAY_OF_MONTH));
        xts.setHour(dateTime.get(ChronoField.HOUR_OF_DAY));
        xts.setMinute(dateTime.get(ChronoField.MINUTE_OF_HOUR));
        xts.setSecond(dateTime.get(ChronoField.SECOND_OF_MINUTE));
        xts.setMillisecond(dateTime.get(ChronoField.MILLI_OF_SECOND));
        xts.setTimezone(ZoneOffset.from(dateTime).getTotalSeconds() / 60);

        return xts;
    }
}
