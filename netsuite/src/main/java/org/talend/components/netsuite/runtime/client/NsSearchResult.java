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

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

/**
 * Holds search result's data.
 *
 * <p>
 * This data object is mirror of NetSuite's native {@code SearchResult} data object.
 *
 * @param <RecT> type of record data object
 */

@Data
@NoArgsConstructor
@ToString
public class NsSearchResult<RecT> implements Serializable {

    /** Status of 'search' operation. */
    private NsStatus status;

    private Integer totalRecords;

    private Integer pageSize;

    private Integer totalPages;

    private String searchId;

    /**
     * current page
     */
    private Integer pageIndex;

    /**
     * contains records from the current page
     */
    private List<RecT> recordList;

    public NsSearchResult(NsStatus status) {
        this.status = status;
    }

    public boolean isSuccess() {
        return status.isSuccess();
    }
}
