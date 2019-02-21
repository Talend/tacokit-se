package org.talend.components.salesforce.input;

import static java.util.stream.Collectors.joining;
import static org.talend.components.salesforce.dataset.QueryDataSet.SourceType.MODULE_SELECTION;
import static org.talend.components.salesforce.dataset.QueryDataSet.SourceType.SOQL_QUERY;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.fault.ApiFault;
import com.sforce.ws.ConnectionException;

import org.talend.components.salesforce.BulkResultSet;
import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.components.salesforce.service.SalesforceService;
import org.talend.components.salesforce.service.UiActionService;
import org.talend.components.salesforce.service.BulkQueryService;
import org.talend.components.salesforce.service.Messages;
import org.talend.components.salesforce.soql.SoqlQuery;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "SalesforceInput")
@Emitter(name = "Input")
@Documentation("Salesforce query input ")
public class InputEmitter implements Serializable {

    private final SalesforceService service;

    private final QueryDataSet dataset;

    private final LocalConfiguration localConfiguration;

    private BulkQueryService bulkQueryService;

    private BulkResultSet bulkResultSet;

    private JsonBuilderFactory jsonBuilderFactory;

    private Messages messages;

    public InputEmitter(@Option("configuration") final QueryDataSet queryDataSet, final SalesforceService service,
            LocalConfiguration configuration, final JsonBuilderFactory jsonBuilderFactory, final Messages messages) {
        this.service = service;
        this.dataset = queryDataSet;
        this.localConfiguration = configuration;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.messages = messages;
    }

    @PostConstruct
    public void init() {
        try {
            final BulkConnection bulkConnection = service.bulkConnect(dataset.getDataStore(), localConfiguration);
            bulkQueryService = new BulkQueryService(bulkConnection, jsonBuilderFactory, messages);
            bulkQueryService.doBulkQuery(getModuleName(), getSoqlQuery());
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        } catch (AsyncApiException e) {
            throw new IllegalStateException(e.getExceptionMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Producer
    public JsonObject next() {
        try {
            if (bulkResultSet == null) {
                bulkResultSet = bulkQueryService.getQueryResultSet(bulkQueryService.nextResultId());
            }
            JsonObject currentRecord = bulkResultSet.next();
            if (currentRecord == null) {
                String resultId = bulkQueryService.nextResultId();
                if (resultId != null) {
                    bulkResultSet = bulkQueryService.getQueryResultSet(resultId);
                    currentRecord = bulkResultSet.next();
                }
            }
            return currentRecord;
        } catch (ConnectionException e) {
            throw service.handleConnectionException(e);
        } catch (AsyncApiException e) {
            throw new IllegalStateException(e.getExceptionMessage(), e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String getModuleName() {
        if (dataset.getSourceType() == MODULE_SELECTION) {
            return dataset.getModuleName();
        }
        String query = dataset.getQuery();
        if (query != null && !query.isEmpty()) {
            SoqlQuery soqlInstance = SoqlQuery.getInstance();
            soqlInstance.init(query);
            return soqlInstance.getDrivingEntityName();
        }

        throw new IllegalStateException("Module name can't be retrieved");
    }

    private String getSoqlQuery() {
        if (dataset.getSourceType() == SOQL_QUERY) {
            return dataset.getQuery();
        }

        List<String> allModuleFields;
        DescribeSObjectResult describeSObjectResult;
        try {
            final PartnerConnection connection = service.connect(dataset.getDataStore(), localConfiguration);
            describeSObjectResult = connection.describeSObject(dataset.getModuleName());
            allModuleFields = getColumnNames(describeSObjectResult);
        } catch (ConnectionException e) {
            if (ApiFault.class.isInstance(e)) {
                ApiFault fault = ApiFault.class.cast(e);
                throw new IllegalStateException(fault.getExceptionMessage(), e);
            }
            throw new IllegalStateException(e);
        }

        List<String> queryFields;
        if (dataset.getSelectColumnIds() == null || dataset.getSelectColumnIds().isEmpty()) {
            queryFields = allModuleFields;
        } else if (!allModuleFields.containsAll(dataset.getSelectColumnIds())) { // ensure requested fields exist
            throw new IllegalStateException("columns { "
                    + dataset.getSelectColumnIds().stream().filter(c -> !allModuleFields.contains(c)).collect(joining(","))
                    + " } " + "doesn't exist in module '" + dataset.getModuleName() + "'");
        } else {
            queryFields = dataset.getSelectColumnIds();
        }

        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        int count = 0;
        for (String se : queryFields) {
            if (count++ > 0) {
                sb.append(", ");
            }
            sb.append(se);
        }
        sb.append(" from ");
        sb.append(dataset.getModuleName());
        if (dataset.getCondition() != null && !dataset.getCondition().isEmpty()) {
            sb.append(" where ");
            sb.append(dataset.getCondition());
        }
        return sb.toString();
    }

    private List<String> getColumnNames(DescribeSObjectResult in) {
        List<String> fields = new ArrayList<>();
        for (Field field : in.getFields()) {
            // filter the invalid compound columns for salesforce bulk query api
            if (field.getType() == FieldType.address || // no address
                    field.getType() == FieldType.location || // no location
                    // no picklist that has a parent
                    (field.getType() == FieldType.picklist && field.getCompoundFieldName() != null
                            && !field.getCompoundFieldName().trim().isEmpty())) {
                continue;
            }
            fields.add(field.getName());
        }
        return fields;
    }
}
