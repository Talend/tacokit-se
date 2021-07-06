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
package org.talend.components.salesforce.service;

import java.util.Collection;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.IDeleteResult;
import com.sforce.soap.partner.IDescribeSObjectResult;
import com.sforce.soap.partner.IField;
import com.sforce.soap.partner.ISaveResult;
import com.sforce.soap.partner.IUpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.service.operation.ConnectionFacade;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.injector.Injector;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.ComponentManager.AllServices;

@WithComponents("org.talend.components.salesforce")
class UiActionServiceUnitTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private UiActionService uiService;

    @Test
    void listColumns() {
        UiActionServiceUnitTest.inject(componentsHandler.asManager(), new SalesforceServiceFake());
        final BasicDataStore bds = new BasicDataStore();
        final SuggestionValues values = uiService.listColumns(bds, "test");
        final Collection<Item> items = values.getItems();
        Assertions.assertEquals(3, items.size());
        Assertions.assertTrue(items.stream().map(Item::getLabel).anyMatch("f1"::equals));
        Assertions.assertFalse(items.stream().map(Item::getLabel).anyMatch("f4"::equals));
    }

    static void inject(final ComponentManager manager, final SalesforceService fakeService) {
        final Container container = manager.findPlugin("classes").orElse(null);
        final AllServices allServices = container.get(AllServices.class);

        // put fake as real
        allServices.getServices().put(SalesforceService.class, fakeService);

        // inject fake on all others services that use it
        final UiActionService service = (UiActionService) allServices.getServices().get(UiActionService.class);
        final Injector injector = Injector.class.cast(allServices.getServices().get(Injector.class));
        injector.inject(service);
    }

    static class SalesforceServiceFake extends SalesforceService {

        @Override
        public ConnectionFacade buildConnection(final BasicDataStore datastore, final LocalConfiguration localConfiguration)
                throws ConnectionException {
            return new ConnectionFacadeTest();
        }
    }

    static class ConnectionFacadeTest implements ConnectionFacade {

        @Override
        public ISaveResult[] create(SObject[] sObjects) throws ConnectionException {
            return new ISaveResult[0];
        }

        @Override
        public IDeleteResult[] delete(String[] ids) throws ConnectionException {
            return new IDeleteResult[0];
        }

        @Override
        public ISaveResult[] update(SObject[] sObjects) throws ConnectionException {
            return new ISaveResult[0];
        }

        @Override
        public IUpsertResult[] upsert(String externalIDFieldName, SObject[] sObjects) throws ConnectionException {
            return new IUpsertResult[0];
        }

        @Override
        public IDescribeSObjectResult describeSObject(String sObjectType) throws ConnectionException {
            final DescribeSObjectResult result = new DescribeSObjectResult();
            final IField[] fields = new IField[5];
            fields[0] = this.newField(FieldType.string, "f1");
            fields[1] = this.newField(FieldType._int, "f2");
            fields[2] = this.newField(FieldType._boolean, "f3");
            fields[3] = this.newField(FieldType.address, "f4");
            fields[4] = this.newField(FieldType.location, "f5");

            result.setFields(fields);
            return result;
        }

        private IField newField(final FieldType ft, final String name) {
            Field f = new Field();
            f.setType(ft);
            f.setName(name);
            return f;
        }
    }
}