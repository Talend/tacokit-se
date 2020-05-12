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
package org.talend.components.netsuite.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NsSearchResult;
import org.talend.components.netsuite.runtime.client.search.SearchCondition;
import org.talend.components.netsuite.runtime.client.search.SearchQuery;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Getter;

import static java.util.stream.Collectors.toList;

@Documentation("TODO fill the documentation for this source")
public class NetSuiteInputSearcher {

    private final NetSuiteInputProperties configuration;

    private NetSuiteClientService<?> clientService;

    @Getter
    private SearchQuery<?, ?> search;

    public NetSuiteInputSearcher(final NetSuiteInputProperties configuration, final NetSuiteClientService<?> clientService) {
        this.configuration = configuration;
        this.clientService = clientService;
        clientService.setBodyFieldsOnly(configuration.isBodyFieldsOnly());
    }

    /**
     * Build and execute NetSuite search query.
     *
     * @return
     * @throws NetSuiteException if an error occurs during execution of search
     */
    public NsSearchResult<?> search() throws NetSuiteException {
        search = buildSearchQuery();
        return search.search();
    }

    /**
     * Build search query from properties.
     *
     * @return search query object
     */
    private SearchQuery<?, ?> buildSearchQuery() {
        String recordType = configuration.getDataSet().getRecordType();
        boolean customizationEnbled = configuration.getDataSet().isEnableCustomization();

        SearchQuery<?, ?> search = clientService.newSearch(clientService.getMetaDataSource(), recordType, customizationEnbled);
        Optional<List<SearchConditionConfiguration>> searchConfigurationsOptional = Optional
                .ofNullable(configuration.getSearchCondition()).filter(list -> !list.isEmpty());
        searchConfigurationsOptional.ifPresent(searchConditionConfigurations -> searchConditionConfigurations.stream()
                .map(this::buildSearchCondition).forEach(search::condition));
        return search;
    }

    /**
     * Build search condition.
     */
    private SearchCondition buildSearchCondition(SearchConditionConfiguration searchConfiguration) {
        List<String> values = buildSearchConditionValueList(searchConfiguration.getSearchValue(),
                searchConfiguration.getAdditionalSearchValue());
        return new SearchCondition(searchConfiguration.getField(), searchConfiguration.getOperator(), values);
    }

    /**
     * Build search value list.
     *
     * @param searchValue first search value
     * @param additionalSearchValue second search value
     * @return
     */
    private List<String> buildSearchConditionValueList(Object searchValue, Object additionalSearchValue) {
        if (searchValue == null) {
            return null;
        }

        List<String> valueList;
        if (searchValue instanceof Collection) {
            Collection<?> elements = (Collection<?>) searchValue;
            return elements.stream().filter(Objects::nonNull).map(Objects::toString).collect(toList());
        } else {
            valueList = new ArrayList<>(2);
            String value = searchValue.toString();
            if (StringUtils.isNotEmpty(value)) {
                valueList.add(value);
                Optional.ofNullable(additionalSearchValue).map(String.class::cast).filter(StringUtils::isNotEmpty)
                        .ifPresent(valueList::add);
            }
        }

        return valueList;
    }

}