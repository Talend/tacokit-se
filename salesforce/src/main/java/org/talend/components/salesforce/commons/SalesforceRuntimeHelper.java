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

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Contains only runtime helper classes, mainly to do with logging.
 */
public class SalesforceRuntimeHelper {

    private SalesforceRuntimeHelper() {
    }

    /**
     * Convert date to calendar with timezone "GMT"
     * 
     * @param date
     * @param useLocalTZ whether use local timezone during convert
     *
     * @return Calendar instance
     */
    public static Calendar convertDateToCalendar(Date date, boolean useLocalTZ) {
        if (date != null) {
            Calendar cal = Calendar.getInstance();
            cal.clear();
            if (useLocalTZ) {
                TimeZone tz = TimeZone.getDefault();
                cal.setTimeInMillis(date.getTime() + tz.getRawOffset() + tz.getDSTSavings());
            } else {
                cal.setTimeZone(TimeZone.getTimeZone("GMT"));
                cal.setTime(date);
            }
            return cal;
        } else {
            return null;
        }
    }

}
