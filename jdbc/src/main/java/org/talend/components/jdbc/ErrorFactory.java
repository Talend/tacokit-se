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
package org.talend.components.jdbc;

import java.sql.SQLException;

public final class ErrorFactory {

    private ErrorFactory() {
    }

    public static IllegalStateException toIllegalStateException(final Throwable e) {
        if (!(e instanceof SQLException)) {
            return new IllegalStateException(e);
        }

        final IllegalStateException error = new IllegalStateException(e.getMessage());
        error.setStackTrace(e.getStackTrace());
        return error;

    }
}
