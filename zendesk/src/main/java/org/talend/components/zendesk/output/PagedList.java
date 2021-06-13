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
package org.talend.components.zendesk.output;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PagedList<T> {

    private final int pageSize;

    private final Iterator<T> listIterator;

    public PagedList(List<T> list, int pageSize) {
        this.listIterator = list.iterator();
        this.pageSize = pageSize;
    }

    public List<T> getNextPage() {
        List<T> result = new ArrayList<>();
        for (int i = 0; i < pageSize; i++) {
            if (listIterator.hasNext()) {
                result.add(listIterator.next());
            } else {
                break;
            }
        }
        return result.isEmpty() ? null : result;
    }

}
