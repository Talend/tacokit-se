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
package org.talend.components.netsuite.runtime.client.search;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NsSearchResult;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;

import lombok.Getter;

import static java.util.stream.Collectors.toCollection;

/**
 * Result set for search results.
 *
 * @see SearchQuery#search()
 * @see NetSuiteClientService#search(Object)
 */
public class SearchResultSet<R> implements Iterator<R> {

    /**
     * NetSuite client which this result set is owned by.
     */
    private NetSuiteClientService<?> clientService;

    /**
     * Descriptor of target record type.
     */
    @Getter
    private RecordTypeInfo recordTypeDesc;

    /**
     * Descriptor of target search record type.
     */
    private SearchRecordTypeDesc searchRecordTypeDesc;

    /**
     * range of pages
     */
    private PageSelection pageSelection;

    private Iterator<Integer> pageIterator;

    private Iterator<R> recordIterator;

    /**
     * Last retrieved record.
     */
    private R current;

    public SearchResultSet(NetSuiteClientService<?> clientService, String recordTypeName, PageSelection pageSelection,
            boolean customizationEnabled) {
        this.clientService = clientService;
        this.pageSelection = pageSelection;
        recordTypeDesc = clientService.getMetaDataSource().getRecordType(recordTypeName, customizationEnabled);
        searchRecordTypeDesc = clientService.getMetaDataSource().getSearchRecordType(recordTypeName, customizationEnabled);
        // search not found or not supported
        if (searchRecordTypeDesc == null) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                    clientService.getI18n().searchRecordNotFound(recordTypeName));
        }

        pageIterator = IntStream.range(pageSelection.getFirstPage(), pageSelection.getFirstPage() + pageSelection.getPageCount())
                .iterator();
        recordIterator = Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() {
        if (recordIterator.hasNext()) {
            current = recordIterator.next();
            return true;
        }
        if (pageIterator.hasNext()) {
            recordIterator = getRecordIterator(pageIterator.next());
            return hasNext();
        }
        return false;
    }

    @Override
    public R next() {
        return current;
    }

    private Iterator<R> getRecordIterator(int nextPageIndex) {
        String searchId = pageSelection.getSearchId();
        if (searchId != null) {
            NsSearchResult<R> nextPageResult = clientService.searchMoreWithId(searchId, nextPageIndex);
            if (!nextPageResult.isSuccess()) {
                NetSuiteClientService.checkError(nextPageResult.getStatus());
            }
            List<R> recordList = nextPageResult.getRecordList();
            if (recordList == null || recordList.isEmpty()) {
                return Collections.emptyIterator();
            }
            if (BasicRecordType.ITEM.getType().equals(searchRecordTypeDesc.getType())) {
                return recordList.stream().filter(r -> r.getClass() == recordTypeDesc.getRecordType().getRecordClass())
                        .collect(toCollection(LinkedList::new)).iterator();
            } else {
                return recordList.iterator();
            }
        }
        return Collections.emptyIterator();
    }
}
