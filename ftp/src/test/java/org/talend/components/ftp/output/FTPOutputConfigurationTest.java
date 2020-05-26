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
package org.talend.components.ftp.output;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FTPOutputConfigurationTest {

    @Test
    public void testLimitByFlags() {
        Assertions.assertTrue(FTPOutputConfiguration.LimitBy.RECORDS.isLimitedByRecords(),
                "LimitBy.RECORDS should limit records");
        Assertions.assertFalse(FTPOutputConfiguration.LimitBy.RECORDS.isLimitedBySize(), "LimitBy.RECORDS should not limit size");
        Assertions.assertFalse(FTPOutputConfiguration.LimitBy.SIZE.isLimitedByRecords(), "LimitBy.SIZE should limit records");
        Assertions.assertTrue(FTPOutputConfiguration.LimitBy.SIZE.isLimitedBySize(), "LimitBy.SIZE should not limit size");
        Assertions.assertTrue(FTPOutputConfiguration.LimitBy.SIZE_AND_RECORDS.isLimitedByRecords(),
                "LimitBy.SIZE_AND_RECORDS should limit records");
        Assertions.assertTrue(FTPOutputConfiguration.LimitBy.SIZE_AND_RECORDS.isLimitedBySize(),
                "LimitBy.SIZE_AND_RECORDS should limit size");
    }
}
