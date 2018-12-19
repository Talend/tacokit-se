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

package org.talend.components.salesforce.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.sforce.ws.bind.CalendarCodec;

/**
 *
 */
public class SalesforceRuntimeHelperTest {

    @Test
    @DisplayName("Test convert date to calendar")
    public void testConvertDateToCalendar() throws Throwable {
        long timestamp = System.currentTimeMillis();
        Calendar calendar1 = SalesforceRuntimeHelper.convertDateToCalendar(new Date(timestamp), false);
        assertNotNull(calendar1);
        assertEquals(TimeZone.getTimeZone("GMT"), calendar1.getTimeZone());

        assertNull(SalesforceRuntimeHelper.convertDateToCalendar(null, false));

        CalendarCodec calCodec = new CalendarCodec();
        java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.parse("2017-10-24T20:09:14.000Z");
        Date date = dateFormat.getCalendar().getTime();
        Calendar calIgnoreTZ = SalesforceRuntimeHelper.convertDateToCalendar(date, true);

        assertEquals("2017-10-24T20:09:14.000Z", calCodec.getValueAsString(calIgnoreTZ));

    }
}
