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
package org.talend.components.dynamicscrm.source;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.olingo.client.api.domain.ClientComplexValue;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.client.api.uri.FilterFactory;
import org.apache.olingo.client.api.uri.URIFilter;
import org.apache.olingo.client.core.uri.FilterFactoryImpl;
import org.apache.olingo.commons.api.Constants;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.apache.olingo.commons.core.edm.primitivetype.EdmPrimitiveTypeFactory;
import org.talend.components.dynamicscrm.service.DynamicsCrmException;
import org.talend.components.dynamicscrm.service.I18n;
import org.talend.components.dynamicscrm.source.DynamicsCrmInputMapperConfiguration.Operator;
import org.talend.components.dynamicscrm.source.OrderByCondition.Order;
import org.talend.ms.crm.odata.QueryOptionConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InputHelper {

    private final I18n i18n;

    public InputHelper(final I18n i18n) {
        this.i18n = i18n;
    }

    public QueryOptionConfig createQueryOptionConfig(Schema schema, DynamicsCrmInputMapperConfiguration configuration) {
        QueryOptionConfig config = new QueryOptionConfig();
        final String[] names = schema.getEntries().stream().map(Schema.Entry::getName).toArray(String[]::new);
        config.setReturnEntityProperties(names);
        String filterString = getFilterQuery(configuration);
        if (filterString != null) {
            config.setFilter(filterString);
        }
        String orderByString = getOrderByQuery(configuration);
        if (orderByString != null) {
            config.setOrderBy(orderByString);
        }
        return config;
    }

    public String getOrderByQuery(DynamicsCrmInputMapperConfiguration configuration) {
        if (configuration.getOrderByConditionsList() != null && !configuration.getOrderByConditionsList().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (OrderByCondition orderBy : configuration.getOrderByConditionsList()) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                if (orderBy.getField() != null && !orderBy.getField().isEmpty()) {
                    sb.append((orderBy.getOrder() == Order.DESC) ? orderBy.getField() + " desc" : orderBy.getField());
                }
            }
            String result = sb.toString();
            return result.isEmpty() ? null : result;
        }
        return null;
    }

    public String getFilterQuery(DynamicsCrmInputMapperConfiguration configuration) {
        if (configuration.isCustomFilter() && configuration.getFilter() != null && !configuration.getFilter().isEmpty()) {
            return configuration.getFilter();
        } else if (!configuration.isCustomFilter() && configuration.getFilterConditions() != null
                && !configuration.getFilterConditions().isEmpty()) {
            return convertFilterConditionsTableToString(configuration.getFilterConditions(), configuration.getOperator());
        }
        return null;
    }

    public String convertFilterConditionsTableToString(List<FilterCondition> filterConditions, Operator operator) {
        FilterFactory filterFactory = new FilterFactoryImpl();
        List<URIFilter> uriFilters = new ArrayList<>();
        for (FilterCondition condition : filterConditions) {
            switch (condition.getFilterOperator()) {
            case EQUAL:
                uriFilters.add(filterFactory.eq(condition.getField(), condition.getValue()));
                break;
            case NOTEQUAL:
                uriFilters.add(filterFactory.ne(condition.getField(), condition.getValue()));
                break;
            case GREATER_THAN:
                uriFilters.add(filterFactory.gt(condition.getField(), condition.getValue()));
                break;
            case GREATER_OR_EQUAL:
                uriFilters.add(filterFactory.ge(condition.getField(), condition.getValue()));
                break;
            case LESS_THAN:
                uriFilters.add(filterFactory.lt(condition.getField(), condition.getValue()));
                break;
            case LESS_OR_EQUAL:
                uriFilters.add(filterFactory.le(condition.getField(), condition.getValue()));
                break;
            default:
                throw new IllegalArgumentException("Unsupported condition operator:" + condition.getFilterOperator());
            }

        }
        String operatorString = Optional.ofNullable(operator).orElse(Operator.AND).toString().toLowerCase();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < uriFilters.size(); i++) {
            if (i != 0) {
                sb.append(" ").append(operatorString).append(" ");
            }
            sb.append(uriFilters.get(i).build());
        }
        return sb.toString();
    }

    public Schema getSchemaFromMetadata(Edm metadata, String entitySetName, List<String> columnNames,
            RecordBuilderFactory builderFactory) {
        return parseSchema(metadata, entitySetName, columnNames, builderFactory);
    }

    private Schema parseSchema(Edm edm, String entitySetName, List<String> columnNames, RecordBuilderFactory builderFactory) {
        EdmEntityContainer container = edm.getEntityContainer();
        EdmEntitySet entitySet = container.getEntitySet(entitySetName);
        EdmEntityType type = entitySet.getEntityType();
        if (columnNames == null || columnNames.isEmpty()) {
            columnNames = type.getPropertyNames();
        }
        Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(Schema.Type.RECORD);
        columnNames.forEach(f -> schemaBuilder
                .withEntry(builderFactory.newEntryBuilder().withName(f).withType(getTckType((EdmProperty) type.getProperty(f)))
                        .withElementSchema(getSubSchema(edm, (EdmProperty) type.getProperty(f), builderFactory))
                        .withNullable(((EdmProperty) type.getProperty(f)).isNullable()).build()));
        return schemaBuilder.build();
    }

    private Schema parseSchema(Edm edm, EdmStructuredType type, RecordBuilderFactory builderFactory) {
        Schema.Builder schemaBuilder = builderFactory.newSchemaBuilder(Schema.Type.RECORD);
        type.getPropertyNames()
                .forEach(f -> schemaBuilder.withEntry(
                        builderFactory.newEntryBuilder().withName(f).withType(getTckType((EdmProperty) type.getProperty(f)))
                                .withElementSchema(getSubSchema(edm, (EdmProperty) type.getProperty(f), builderFactory))
                                .withNullable(((EdmProperty) type.getProperty(f)).isNullable()).build()));
        return schemaBuilder.build();
    }

    private Schema getSubSchema(Edm edm, EdmProperty edmElement, RecordBuilderFactory builderFactory) {
        if (edmElement.getType().getKind() != EdmTypeKind.COMPLEX) {
            return builderFactory.newSchemaBuilder(getElementType(edmElement.getType())).build();
        }

        return parseSchema(edm, edm.getComplexType(edmElement.getType().getFullQualifiedName()), builderFactory);
    }

    private Schema.Type getTckType(EdmProperty edmElement) {
        if (edmElement.isCollection()) {
            return Schema.Type.ARRAY;
        }
        return getElementType(edmElement.getType());
    }

    private Schema.Type getElementType(EdmType edmType) {
        if (edmType.getKind() == EdmTypeKind.COMPLEX) {
            return Schema.Type.RECORD;
        }
        switch (edmType.getFullQualifiedName().getFullQualifiedNameAsString()) {
        case "Edm.Boolean":
            return Schema.Type.BOOLEAN;
        case "Edm.Binary":
            return Schema.Type.BYTES;
        case "Edm.Byte":
        case "Edm.SByte":
        case "Edm.Int16":
        case "Edm.Int32":
            return Schema.Type.INT;
        case "Edm.Int64":
            return Schema.Type.LONG;
        case "Edm.DateTime":
        case "Edm.DateTimeOffset":
        case "Edm.Time":
            return Schema.Type.DATETIME;
        case "Edm.Double":
            return Schema.Type.DOUBLE;
        case "Edm.Float":
        case "Edm.Decimal":
            return Schema.Type.FLOAT;
        case "Edm.Guid":
        case "Edm.String":
        default:
            return Schema.Type.STRING;
        }
    }

    public Record createRecord(ClientEntity entity, Schema schema, RecordBuilderFactory builderFactory) {
        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
        schema.getEntries()
                .forEach(entry -> setValue(entity.getProperty(entry.getName()).getValue(), entry, recordBuilder, builderFactory));
        return recordBuilder.build();
    }

    private Record createRecord(ClientComplexValue value, Schema schema, RecordBuilderFactory builderFactory) {
        final Record.Builder recordBuilder = builderFactory.newRecordBuilder(schema);
        schema.getEntries()
                .forEach(entry -> setValue(value.get(entry.getName()).getValue(), entry, recordBuilder, builderFactory));
        return recordBuilder.build();
    }

    private void setValue(ClientValue value, Schema.Entry entry, Record.Builder recordBuilder,
            RecordBuilderFactory builderFactory) {
        if (value == null) {
            return;
        }
        Object convertedValue = getValue(value, entry, builderFactory);
        if (convertedValue == null) {
            return;
        }
        final Schema.Entry.Builder entryBuilder = builderFactory.newEntryBuilder();
        Schema.Type type = entry.getType();
        entryBuilder.withNullable(entry.isNullable()).withName(entry.getName()).withType(type);

        switch (type) {
        case ARRAY:
            Schema elementSchema = entry.getElementSchema();
            entryBuilder.withElementSchema(elementSchema);
            Collection<Object> objects = (Collection<Object>) convertedValue;
            recordBuilder.withArray(entryBuilder.build(), objects);
            break;
        case INT:
            recordBuilder.withInt(entryBuilder.build(), (Integer) convertedValue);
            break;
        case LONG:
            recordBuilder.withLong(entryBuilder.build(), (Long) convertedValue);
            break;
        case BOOLEAN:
            recordBuilder.withBoolean(entryBuilder.build(), (Boolean) convertedValue);
            break;
        case FLOAT:
            recordBuilder.withFloat(entryBuilder.build(), (Float) convertedValue);
            break;
        case DOUBLE:
            recordBuilder.withDouble(entryBuilder.build(), (Double) convertedValue);
            break;
        case BYTES:
            recordBuilder.withBytes(entryBuilder.build(), (byte[]) convertedValue);
            break;
        case DATETIME:
            recordBuilder.withDateTime(entryBuilder.build(), (Timestamp) convertedValue);
            break;
        case RECORD:
            entryBuilder.withElementSchema(entry.getElementSchema());
            recordBuilder.withRecord(entryBuilder.build(), (Record) convertedValue);
            break;
        case STRING:
        default:
            recordBuilder.withString(entryBuilder.build(), (String) convertedValue);
            break;
        }
    }

    private Object getValue(ClientValue value, Schema.Entry entry, RecordBuilderFactory builderFactory) {
        return getValue(value, entry.getType(), entry.getElementSchema(), builderFactory);
    }

    private Object getValue(ClientValue value, Schema schema, RecordBuilderFactory builderFactory) {
        return getValue(value, schema.getType(), schema, builderFactory);
    }

    private Object getValue(ClientValue value, Schema.Type type, Schema elementSchema, RecordBuilderFactory builderFactory) {
        if (value == null || (value.isPrimitive() && value.asPrimitive().toValue() == null)) {
            return null;
        }

        if (value.isEnum()) {
            return value.asEnum().getValue();
        }
        switch (type) {
        case ARRAY:
            final Collection<Object> objects = new ArrayList<>();
            value.asCollection().forEach(val -> objects.add(getValue(val, elementSchema, builderFactory)));
            return objects;
        case INT:
        case LONG:
        case DOUBLE:
        case DATETIME:
        case BOOLEAN:
            return value.asPrimitive().toValue();
        case FLOAT:
            float floatValue;
            if (value.getTypeName().equals("Edm.Decimal")) {
                floatValue = ((BigDecimal) value.asPrimitive().toValue()).floatValue();
            } else {
                floatValue = (Float) value.asPrimitive().toValue();
            }
            return floatValue;
        case BYTES:
            byte[] bytesValue;
            if ("Edm.Binary".equals(value.getTypeName())) {
                bytesValue = (byte[]) value.asPrimitive().toValue();
            } else {
                EdmPrimitiveType binaryType = EdmPrimitiveTypeFactory.getInstance(EdmPrimitiveTypeKind.Binary);
                try {
                    bytesValue = binaryType.valueOfString(value.toString(), null, null, Constants.DEFAULT_PRECISION,
                            Constants.DEFAULT_SCALE, null, byte[].class);
                } catch (EdmPrimitiveTypeException e) {
                    String errorMessage = i18n.failedParsingBytesValue(e.getMessage());
                    log.error(errorMessage);
                    throw new DynamicsCrmException(errorMessage, e);
                }
            }
            return bytesValue;
        case RECORD:
            if (value.asComplex() == null) {
                return null;
            }
            return createRecord(value.asComplex(), elementSchema, builderFactory);
        case STRING:
        default:
            return value.toString();
        }
    }

}
