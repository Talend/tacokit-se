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
package org.talend.components.netsuite.runtime.client;

import org.talend.components.netsuite.runtime.NetSuiteErrorCode;

/**
 * Thrown when NetSuite related error occurs.
 */
public class NetSuiteException extends RuntimeException {

    public NetSuiteException(String message) {
        super(message);
    }

    public NetSuiteException(String message, Throwable cause) {
        super(message, cause);
    }

    public NetSuiteException(NetSuiteErrorCode code, String message) {
        this(code.getCode() + ":" + message == null ? "" : message);
    }

    public NetSuiteException(NetSuiteErrorCode code, String message, Throwable cause) {
        this(code.getCode() + ":" + message == null ? "" : message, cause);
    }
}
