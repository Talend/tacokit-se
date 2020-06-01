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
package org.talend.components.adlsgen2.runtime;

public class AdlsGen2RuntimeException extends RuntimeException {

    public AdlsGen2RuntimeException(String msg) {
        super(msg);
    }

    public AdlsGen2RuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public AdlsGen2RuntimeException(Throwable cause) {
        super(cause);
    }
}
