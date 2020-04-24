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
package org.talend.components.jdbc.suite.output;

import org.junit.jupiter.api.Tag;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.containers.MariaDBTestContainer;
import org.talend.components.jdbc.suite.OutputTest;
import org.talend.sdk.component.junit5.WithComponents;

@Tag("IT")
@WithComponents("org.talend.components.jdbc")
public class MariaOutputTest extends OutputTest {

    @Override
    public JdbcTestContainer buildContainer() {
        return new MariaDBTestContainer();
    }
}