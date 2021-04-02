package org.talend.components.salesforce.service.operation;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.UpsertResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import org.talend.components.salesforce.service.operation.converters.SObjectConvertorForUpdate;
import org.talend.sdk.component.api.record.Record;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class Upsert implements RecordsOperation {

    private final ConnectionFacade connection;

    private final SObjectConvertorForUpdate converter;

    private final String upsertKeyColumn;

    @Override
    public List<Result> execute(List<Record> records) throws IOException {
        final SObject[] upds = new SObject[records.size()];
        for (int i = 0; i < records.size(); i++) {
            upds[i] = converter.fromRecord(records.get(i));
        }

        String[] changedItemKeys = new String[upds.length];
        for (int ix = 0; ix < upds.length; ++ix) {
            Object value = upds[ix].getField(upsertKeyColumn);
            if (value != null) {
                changedItemKeys[ix] = String.valueOf(value);
            }
        }

        try {
            final UpsertResult[] saveResults = connection.upsert(upsertKeyColumn, upds);
            return Stream.of(saveResults) //
                    .map(this::toResult)
                    .collect(Collectors.toList());
        } catch (ConnectionException e) {
            throw new IOException(e);
        }
    }

    private Result toResult(UpsertResult saveResult) {
        if (saveResult.isSuccess()) {
            return Result.OK;
        }
        final List<String> errors =
                Stream.of(saveResult.getErrors()) //
                        .map(Error::getMessage) //
                        .collect(Collectors.toList());
        return new Result(errors);
    }
}
