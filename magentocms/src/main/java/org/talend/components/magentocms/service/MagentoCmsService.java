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
package org.talend.components.magentocms.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.MagentoCmsHealthChecker;
import org.talend.components.magentocms.common.MagentoDataSet;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.ConfigurationFilter;
import org.talend.components.magentocms.input.FilterAdvancedValueWrapper;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.messages.Messages;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.update.Update;

import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MagentoCmsService {

    @Service
    private Messages i18n;

    @Service
    private MagentoCmsHealthChecker magentoCmsHealthChecker;

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @DiscoverSchema(ConfigurationHelper.DISCOVER_SCHEMA_INPUT_ID)
    public Schema guessTableSchema(final MagentoDataSet dataSet) {
        log.debug("guess my schema");
        return getSchema(dataSet.getSelectionType());
    }

    @Update(ConfigurationHelper.UPDATABLE_FILTER_ADVANCED_ID)
    public FilterAdvancedValueWrapper updatableFilterAdvanced(
            @Option("filterOperator") final SelectionFilterOperator filterOperator,
            @Option("filterLines") final List<SelectionFilter> filterLines) {
        log.debug("suggest advanced filter");
        ConfigurationFilter filter = new ConfigurationFilter(filterOperator, filterLines, null);
        Map<String, String> allParameters = new TreeMap<>();
        try {
            ConfigurationHelper.fillFilterParameters(allParameters, filter, false);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        String allParametersStr = allParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
        FilterAdvancedValueWrapper filterAdvancedValueWrapper = new FilterAdvancedValueWrapper();
        filterAdvancedValueWrapper.setFilterAdvancedValue(allParametersStr);
        return filterAdvancedValueWrapper;
    }

    @HealthCheck(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
    public HealthCheckStatus validateBasicConnection(@Option final MagentoDataStore dataStore) {
        log.debug("start health check");
        MagentoInputConfiguration config = new MagentoInputConfiguration();
        config.getMagentoDataSet().setMagentoDataStore(dataStore);
        ConfigurationHelper.setupServicesInput(config, magentoHttpClientService);

        try {
            magentoCmsHealthChecker.checkHealth(dataStore);
        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.healthCheckFailed(e.getMessage()));
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.healthCheckOk());
    }

    @AsyncValidation(ConfigurationHelper.VALIDATE_WEB_SERVER_URL_ID)
    public ValidationResult validateWebServerUrl(String url) {
        if (url.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.healthCheckServerUrlIsEmpty());
        }
        return new ValidationResult(ValidationResult.Status.OK, "");
    }

    @AsyncValidation(ConfigurationHelper.VALIDATE_AUTH_LOGIN_PASSWORD_LOGIN_ID)
    public ValidationResult validateAuthLoginPasswordLogin(String login) {
        if (login.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.healthCheckLoginIsEmpty());
        }
        return new ValidationResult(ValidationResult.Status.OK, "");
    }

    @AsyncValidation(ConfigurationHelper.VALIDATE_AUTH_OAUTH_PARAMETER_ID)
    public ValidationResult validateOauthParameter(String oauthParameter) {
        if (oauthParameter.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.healthCheckOauthParameterIsEmpty());
        }
        return new ValidationResult(ValidationResult.Status.OK, "");
    }

    @AsyncValidation(ConfigurationHelper.VALIDATE_AUTH_TOKEN_ID)
    public ValidationResult validateToken(String token) {
        if (token.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.healthCheckTokenIsEmpty());
        }
        return new ValidationResult(ValidationResult.Status.OK, "");
    }

    private Schema getSchema(SelectionType selectionType) {
        Schema schema = null;
        switch (selectionType) {
        case PRODUCTS:
            schema = getSchemaForProduct(recordBuilderFactory);
            break;
        default:
            throw new UnsupportedOperationException("Unknown selection type");
        }
        return schema;
    }

    private Schema getSchemaForProduct(RecordBuilderFactory factory) {
        Schema schema = factory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(factory.newEntryBuilder().withName("id").withType(Schema.Type.LONG).build())
                .withEntry(factory.newEntryBuilder().withName("sku").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("name").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("attribute_set_id").withType(Schema.Type.LONG).withNullable(true)
                        .build())
                .withEntry(factory.newEntryBuilder().withName("price").withType(Schema.Type.DOUBLE).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("status").withType(Schema.Type.INT).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("visibility").withType(Schema.Type.INT).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("type_id").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(
                        factory.newEntryBuilder().withName("created_at").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(
                        factory.newEntryBuilder().withName("updated_at").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("product_links").withType(Schema.Type.ARRAY)// .withNullable(true)
                        .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(factory.newEntryBuilder().withName("tier_prices").withType(Schema.Type.ARRAY)// .withNullable(true)
                        .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .withEntry(factory.newEntryBuilder().withName("custom_attributes").withType(Schema.Type.ARRAY)// .withNullable(true)
                        .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()).build())
                .build();
        return schema;
    }

    public Record jsonObjectToRecord(JsonObject jsonObject, SelectionType selectionType) {
        Schema schema = getSchema(selectionType);

        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder();
        for (Schema.Entry schemaEntry : schema.getEntries()) {
            String schemaName = schemaEntry.getName();
            JsonValue jsonValue = jsonObject.get(schemaName);
            if (jsonValue == null) {
                continue;
            }
            switch (schemaEntry.getType()) {
            case RECORD:
            case ARRAY:
                recordBuilder.withString(schemaName, jsonObject.get(schemaName).toString());
                break;
            case STRING:
            case DATETIME:
                recordBuilder.withString(schemaName, jsonObject.getString(schemaName));
                break;
            case BYTES:
                String val = jsonObject.getString(schemaName);
                byte[] payload = Base64.getDecoder().decode(val);
                recordBuilder.withBytes(schemaName, payload);
                break;
            case INT:
                recordBuilder.withInt(schemaName, jsonObject.getInt(schemaName));
                break;
            case LONG:
                recordBuilder.withLong(schemaName, jsonObject.getJsonNumber(schemaName).longValue());
                break;
            case FLOAT:
                recordBuilder.withFloat(schemaName, jsonObject.getJsonNumber(schemaName).numberValue().floatValue());
                break;
            case DOUBLE:
                recordBuilder.withDouble(schemaName, jsonObject.getJsonNumber(schemaName).doubleValue());
                break;
            case BOOLEAN:
                recordBuilder.withBoolean(schemaName, jsonObject.getBoolean(schemaName));
                break;
            default:
                recordBuilder.withString(schemaName, jsonObject.get(schemaName).toString());
            }
        }
        return recordBuilder.build();
    }
}