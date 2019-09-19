/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.couchbase.source.holder;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryRow;

import java.util.Iterator;

public class N1QLResult implements ResultHolder {

    private Iterator<N1qlQueryRow> value;

    public N1QLResult(Iterator<N1qlQueryRow> value) {
        this.value = value;
    }

    @Override
    public JsonObject next() {
        return value.next().value();
    }

    @Override
    public boolean hasNext() {
        return value.hasNext();
    }
}
