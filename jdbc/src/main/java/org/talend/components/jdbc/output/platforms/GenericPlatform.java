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
package org.talend.components.jdbc.output.platforms;

import org.talend.components.jdbc.service.I18nMessage;

import java.sql.Connection;
import java.sql.SQLException;

public class GenericPlatform extends Platform {

    public static final String GENERIC = "Generic";

    public GenericPlatform(final I18nMessage i18n) {
        super(i18n);
    }

    @Override
    public String name() {
        return GENERIC;
    }

    @Override
    protected String delimiterToken() {
        return null;
    }

    @Override
    protected String buildQuery(Connection connection, Table table) throws SQLException {
        return null;
    }

    @Override
    protected boolean isTableExistsCreationError(Throwable e) {
        return false;
    }
}
