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
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.Test;

/**
 *
 */
public class BulkResultTest {

    @Test
    public void testCopyValues() throws IOException {

        BulkResult result = new BulkResult();
        result.setValue("fieldA", "fieldValueA");
        result.setValue("fieldB", "fieldValueB");
        result.setValue("fieldC", "fieldValueC");
        result.setValue("fieldD", "#N/A");

        BulkResult result2 = new BulkResult();
        result2.copyValues(result);

        assertEquals("fieldValueA", result2.getValue("fieldA"));
        assertEquals("fieldValueB", result2.getValue("fieldB"));
        assertEquals("fieldValueC", result2.getValue("fieldC"));
        assertNull(result2.getValue("fieldD"));

        BulkResult result3 = new BulkResult();
        result3.copyValues(null);
        assertNull(result3.getValue("fieldA"));
        assertNull(result3.getValue("fieldB"));
    }
}
