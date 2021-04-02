package org.talend.components.salesforce.service.operation;

import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import lombok.RequiredArgsConstructor;

public interface ConnectionFacade {

    com.sforce.soap.partner.SaveResult[] create(com.sforce.soap.partner.sobject.SObject[] sObjects)
            throws com.sforce.ws.ConnectionException;

    com.sforce.soap.partner.DeleteResult[] delete(String[] ids)
            throws com.sforce.ws.ConnectionException;

    com.sforce.soap.partner.SaveResult[] update(com.sforce.soap.partner.sobject.SObject[] sObjects)
            throws com.sforce.ws.ConnectionException;

    com.sforce.soap.partner.UpsertResult[] upsert(String externalIDFieldName,
            com.sforce.soap.partner.sobject.SObject[] sObjects)
            throws com.sforce.ws.ConnectionException;

    @RequiredArgsConstructor
    class ConnectionImpl implements ConnectionFacade {
        private final PartnerConnection connection;

        @Override
        public SaveResult[] create(SObject[] sObjects) throws ConnectionException {
            return this.connection.create(sObjects);
        }

        @Override
        public DeleteResult[] delete(String[] ids) throws ConnectionException {
            return this.connection.delete(ids);
        }

        @Override
        public SaveResult[] update(SObject[] sObjects) throws ConnectionException {
            return this.connection.update(sObjects);
        }

        @Override
        public UpsertResult[] upsert(String externalIDFieldName, SObject[] sObjects)
                throws ConnectionException {
            return this.connection.upsert(externalIDFieldName, sObjects);
        }
    }
}
