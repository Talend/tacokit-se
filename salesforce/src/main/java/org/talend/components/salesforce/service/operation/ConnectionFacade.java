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
package org.talend.components.salesforce.service.operation;

import com.sforce.soap.partner.IDeleteResult;
import com.sforce.soap.partner.IDescribeSObjectResult;
import com.sforce.soap.partner.ISaveResult;
import com.sforce.soap.partner.IUpsertResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import lombok.RequiredArgsConstructor;

public interface ConnectionFacade {

    ISaveResult[] create(com.sforce.soap.partner.sobject.SObject[] sObjects) throws ConnectionException;

    IDeleteResult[] delete(String[] ids) throws ConnectionException;

    ISaveResult[] update(com.sforce.soap.partner.sobject.SObject[] sObjects) throws ConnectionException;

    IUpsertResult[] upsert(String externalIDFieldName, com.sforce.soap.partner.sobject.SObject[] sObjects)
            throws ConnectionException;

    default IDescribeSObjectResult describeSObject(java.lang.String sObjectType) throws ConnectionException {
        return null;
    }

    @RequiredArgsConstructor
    class ConnectionImpl implements ConnectionFacade {

        private final PartnerConnection connection;

        @Override
        public ISaveResult[] create(SObject[] sObjects) throws ConnectionException {
            return this.connection.create(sObjects);
        }

        @Override
        public IDeleteResult[] delete(String[] ids) throws ConnectionException {
            return this.connection.delete(ids);
        }

        @Override
        public ISaveResult[] update(SObject[] sObjects) throws ConnectionException {
            return this.connection.update(sObjects);
        }

        @Override
        public IUpsertResult[] upsert(String externalIDFieldName, SObject[] sObjects) throws ConnectionException {
            return this.connection.upsert(externalIDFieldName, sObjects);
        }

        @Override
        public IDescribeSObjectResult describeSObject(String sObjectType) throws ConnectionException {
            return this.connection.describeSObject(sObjectType);
        }
    }
}
