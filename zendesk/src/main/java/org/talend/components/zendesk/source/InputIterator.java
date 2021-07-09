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
package org.talend.components.zendesk.source;

import java.util.Iterator;

import javax.json.JsonObject;
import javax.json.JsonReaderFactory;

import org.talend.components.zendesk.helpers.JsonHelper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class InputIterator implements Iterator<JsonObject> {

    private final Iterator<?> dataListIterator;

    private final JsonReaderFactory jsonReaderFactory;

    @Override
    public boolean hasNext() {
        return dataListIterator.hasNext();
    }

    @Override
    public JsonObject next() {
        if (!dataListIterator.hasNext()) {
            return null;
        }
        Object obj = dataListIterator.next();
        return JsonHelper.toJsonObject(obj, jsonReaderFactory);
    }
}
