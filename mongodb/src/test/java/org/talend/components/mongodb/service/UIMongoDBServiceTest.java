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

package org.talend.components.mongodb.service;

import org.junit.jupiter.api.Test;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

@WithComponents("org.talend.components.mongodb")
public class UIMongoDBServiceTest {

    @Service
    private UIMongoDBService uiMongoDBService;

    @Test
    public void testSchemaFields() {
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setSchema(Arrays.asList("column1", "column2"));

        SuggestionValues values = uiMongoDBService.getFields(dataset);

        assertEquals(2, values.getItems().size());
        assertThat(values.getItems().stream().map(SuggestionValues.Item::getId).collect(Collectors.toList()),
                containsInAnyOrder(dataset.getSchema().toArray()));
    }
}
