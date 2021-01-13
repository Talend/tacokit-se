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
package org.talend.components.adlsgen2;

import java.util.List;
import java.util.Map;

import org.talend.sdk.component.api.service.http.Response;

public class FakeResponse<T> implements Response<T> {

    private final int status;

    private final T body;

    private final Map<String, List<String>> headers;

    private final String error;

    public FakeResponse(int status, T body, Map<String, List<String>> headers, String error) {
        this.status = status;
        this.body = body;
        this.headers = headers;
        this.error = error;
    }

    @Override
    public int status() {
        return this.status;
    }

    @Override
    public Map<String, List<String>> headers() {
        return this.headers;
    }

    @Override
    public T body() {
        return this.body;
    }

    @Override
    public <E> E error(Class<E> type) {
        if (String.class.equals(type)) {
            return (E) this.error;
        }
        return null;
    }
}
