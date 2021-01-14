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
package org.talend.components.dynamicscrm.output;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.domain.ClientCollectionValue;
import org.apache.olingo.client.api.domain.ClientComplexValue;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmElement;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmKeyPropertyRef;
import org.apache.olingo.commons.api.edm.EdmNavigationPropertyBinding;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.EdmReferentialConstraint;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind;
import org.talend.components.dynamicscrm.service.I18n;
import org.talend.ms.crm.odata.DynamicsCRMClient;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import javax.naming.ServiceUnavailableException;

public abstract class AbstractToEntityRecordProcessor implements RecordProcessor {

    protected final DynamicsCRMClient client;

    protected final I18n i18n;

    protected final EdmEntitySet entitySet;

    protected final Map<String, String> lookupMapping;

    protected final DynamicsCrmOutputConfiguration configuration;

    protected final Edm metadata;

    protected Set<String> columnNames;

    protected Map<String, List<String>> possibleEntitySetsMap;

    public AbstractToEntityRecordProcessor(final DynamicsCRMClient client, final I18n i18n, final EdmEntitySet entitySet,
            final DynamicsCrmOutputConfiguration configuration, final Edm metadata, final List<String> columnNames) {
        this.client = client;
        this.i18n = i18n;
        this.entitySet = entitySet;
        this.configuration = configuration;
        this.lookupMapping = Optional.ofNullable(configuration.getLookupMapping()).orElse(Collections.emptyList()).stream()
                .collect(Collectors.toMap(LookupMapping::getInputColumn, LookupMapping::getReferenceEntitySet));
        this.metadata = metadata;
        this.columnNames = new HashSet<>(columnNames);
        this.possibleEntitySetsMap = initNavigationEntitySetsMap();
    }

    // We can read navigation links from entity set and get properties, which are actually navigation links.
    protected Map<String, List<String>> initNavigationEntitySetsMap() {
        Map<String, String> navPropMap = entitySet.getNavigationPropertyBindings().stream()
                .collect(Collectors.toMap(EdmNavigationPropertyBinding::getPath, EdmNavigationPropertyBinding::getTarget));
        final Map<String, List<String>> possibleEntitySets = new HashMap<>();
        entitySet.getEntityType().getNavigationPropertyNames().stream().map(entitySet.getEntityType()::getNavigationProperty)
                .forEach(c -> {
                    c.getReferentialConstraints().stream().map(EdmReferentialConstraint::getPropertyName).forEach(
                            s -> possibleEntitySets.computeIfAbsent(s, k -> new ArrayList<>()).add(navPropMap.get(c.getName())));
                });
        return possibleEntitySets;
    }

    @Override
    public void processRecord(Record record) throws ServiceUnavailableException {
        Set<String> keys = entitySet.getEntityType().getKeyPropertyRefs().stream().map(EdmKeyPropertyRef::getName)
                .collect(Collectors.toSet());
        columnNames.removeAll(keys);
        ClientEntity entity = createEntity(columnNames, record);
        doProcessRecord(entity, record);
    }

    protected abstract void doProcessRecord(ClientEntity entity, Record record) throws ServiceUnavailableException;

    protected ClientEntity createEntity(Set<String> columnNames, Record record) {
        ClientEntity entity = client.getClient().getObjectFactory().newEntity(entitySet.getEntityType().getFullQualifiedName());
        for (Schema.Entry entry : record.getSchema().getEntries()) {
            // Need to filter not updateable/insertable fields and navigation properties.
            // Navigation properties cannot be inserted as a common property.
            if (!columnNames.contains(entry.getName()) || possibleEntitySetsMap.containsKey(entry.getName())) {
                continue;
            }
            ClientProperty property = convertToProperty(record, entry, entitySet.getEntityType());
            if (property != null) {
                entity.getProperties().add(property);
            }
        }
        return entity;
    }

    protected ClientProperty convertRecordToProperty(Record record, EdmStructuredType valueType, String name) {
        return client.getClient().getObjectFactory().newComplexProperty(name, getComplexValueFromRecord(record, valueType));
    }

    protected ClientComplexValue getComplexValueFromRecord(Record record, EdmStructuredType valueType) {
        ClientComplexValue value = client.getClient().getObjectFactory()
                .newComplexValue(valueType.getFullQualifiedName().getFullQualifiedNameAsString());
        record.getSchema().getEntries().forEach(e -> {
            ClientProperty property = convertToProperty(record, e, valueType);
            if (property != null || !configuration.isIgnoreNull()) {
                value.add(property);
            }
        });
        return value;
    }

    protected ClientProperty convertToProperty(Record fromRecord, Schema.Entry entry, EdmStructuredType entityType) {
        String name = entry.getName();
        switch (entry.getType()) {
        case ARRAY:
            return processArray(fromRecord, entityType, name, entry);
        case RECORD:
            Record subRecord = fromRecord.getRecord(name);
            EdmStructuredType subEntityType = metadata
                    .getComplexType(entityType.getProperty(name).getType().getFullQualifiedName());
            return convertRecordToProperty(subRecord, subEntityType, name);
        case STRING:
            return processString(fromRecord, entityType.getProperty(name), name);
        case INT:
            Object intOrDateValue;
            if (fromRecord.getOptionalInt(name).isPresent()) {
                intOrDateValue = fromRecord.getOptionalInt(name).getAsInt();
                // This is for Tacokit Date type - it's INT with number of days since epoch
                EdmPrimitiveTypeKind primitiveTypeKind = EdmPrimitiveTypeKind
                        .valueOfFQN(entityType.getProperty(name).getType().getFullQualifiedName());
                if (primitiveTypeKind == EdmPrimitiveTypeKind.Date || primitiveTypeKind == EdmPrimitiveTypeKind.DateTimeOffset) {
                    LocalDate date = LocalDate.ofEpochDay((Integer) intOrDateValue);
                    intOrDateValue = Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant());
                }
            } else {
                intOrDateValue = null;
            }
            return processPrimitive(entityType, name, intOrDateValue);
        case DOUBLE:
            Double doubleValue;
            if (fromRecord.getOptionalDouble(name).isPresent()) {
                doubleValue = fromRecord.getOptionalDouble(name).getAsDouble();
            } else {
                doubleValue = null;
            }
            return processPrimitive(entityType, name, doubleValue);
        case LONG:
            Long longValue;
            if (fromRecord.getOptionalLong(name).isPresent()) {
                longValue = fromRecord.getOptionalLong(name).getAsLong();
            } else {
                longValue = null;
            }
            return processPrimitive(entityType, name, longValue);
        case FLOAT:
            Double floatValue;
            if (fromRecord.getOptionalFloat(name).isPresent()) {
                floatValue = fromRecord.getOptionalFloat(name).getAsDouble();
            } else {
                floatValue = null;
            }
            return processPrimitive(entityType, name, floatValue);
        case BOOLEAN:
            Boolean boolValue = fromRecord.getOptionalBoolean(name).orElse(null);
            return processPrimitive(entityType, name, boolValue);
        case BYTES:
            byte[] bytesValue = fromRecord.getOptionalBytes(name).orElse(null);
            return processPrimitive(entityType, name, bytesValue);
        case DATETIME:
            Date dateValue = null;
            if (fromRecord.getOptionalDateTime(name).isPresent()) {
                dateValue = Date.from(fromRecord.getOptionalDateTime(name).get().toInstant());
            }
            return processPrimitive(entityType, name, dateValue);
        default:
            return null;
        }
    }

    private ClientProperty processArray(Record fromRecord, EdmStructuredType entityType, String name, Schema.Entry entry) {
        ODataClient odataClient = client.getClient();
        ClientCollectionValue<ClientValue> collectionValue = odataClient.getObjectFactory()
                .newCollectionValue(entityType.getProperty(name).getType().getName());
        Schema elementSchema = entry.getElementSchema();
        Schema.Type subType = elementSchema.getType();
        switch (subType) {
        case RECORD:
            fromRecord.getOptionalArray(Record.class, name).orElse(Collections.emptyList())
                    .forEach(r -> collectionValue.add(getComplexValueFromRecord(r,
                            metadata.getComplexType(entityType.getProperty(name).getType().getFullQualifiedName()))));
            break;
        case STRING:
            fromRecord.getOptionalArray(String.class, name).orElse(Collections.emptyList())
                    .forEach(s -> collectionValue.add(getStringValue(s, odataClient, entityType.getProperty(name))));
            break;
        case INT:
            fromRecord.getOptionalArray(Integer.class, name).orElse(Collections.emptyList())
                    .forEach(s -> collectionValue.add(getPrimitiveValue(s, entityType.getProperty(name), odataClient)));
            break;
        case DOUBLE:
            fromRecord.getOptionalArray(Double.class, name).orElse(Collections.emptyList())
                    .forEach(s -> collectionValue.add(getPrimitiveValue(s, entityType.getProperty(name), odataClient)));
            break;
        case LONG:
            fromRecord.getOptionalArray(Long.class, name).orElse(Collections.emptyList())
                    .forEach(s -> collectionValue.add(getPrimitiveValue(s, entityType.getProperty(name), odataClient)));
            break;
        case FLOAT:
            fromRecord.getOptionalArray(Float.class, name).orElse(Collections.emptyList())
                    .forEach(s -> collectionValue.add(getPrimitiveValue(s, entityType.getProperty(name), odataClient)));
            break;
        case BOOLEAN:
            fromRecord.getOptionalArray(Boolean.class, name).orElse(Collections.emptyList())
                    .forEach(s -> collectionValue.add(getPrimitiveValue(s, entityType.getProperty(name), odataClient)));
            break;
        case BYTES:
            fromRecord.getOptionalArray(byte[].class, name).orElse(Collections.emptyList())
                    .forEach(s -> collectionValue.add(getPrimitiveValue(s, entityType.getProperty(name), odataClient)));
            break;
        case DATETIME:
            fromRecord.getOptionalArray(ZonedDateTime.class, name).orElse(Collections.emptyList()).forEach(s -> {
                Date dateValue = Date.from(s.toInstant());
                collectionValue.add(getPrimitiveValue(dateValue, entityType.getProperty(name), odataClient));
            });
            break;
        }
        return odataClient.getObjectFactory().newCollectionProperty(name, collectionValue);
    }

    private <T> ClientProperty processPrimitive(EdmStructuredType entityType, String name, T value) {
        if (value == null && configuration.isIgnoreNull()) {
            return null;
        } else {
            EdmPrimitiveTypeKind primitiveTypeKind = EdmPrimitiveTypeKind
                    .valueOfFQN(entityType.getProperty(name).getType().getFullQualifiedName());
            return client.getClient().getObjectFactory().newPrimitiveProperty(name, client.getClient().getObjectFactory()
                    .newPrimitiveValueBuilder().setType(primitiveTypeKind).setValue(value).build());
        }
    }

    private ClientProperty processString(Record fromRecord, EdmElement propType, String name) {
        String value = fromRecord.getOptionalString(name).orElse(null);
        ODataClient odataClient = client.getClient();
        if (value == null && configuration.isIgnoreNull()) {
            return null;
        } else if (propType.getType().getKind() == EdmTypeKind.ENUM) {
            return odataClient.getObjectFactory().newEnumProperty(name,
                    odataClient.getObjectFactory().newEnumValue(propType.getType().getName(), value));
        } else if (propType.getType().getKind() == EdmTypeKind.PRIMITIVE) {
            EdmPrimitiveTypeKind primitiveTypeKind = EdmPrimitiveTypeKind.valueOfFQN(propType.getType().getFullQualifiedName());
            if (primitiveTypeKind == EdmPrimitiveTypeKind.Guid) {
                if (value == null) {
                    return odataClient.getObjectFactory().newPrimitiveProperty(name, odataClient.getObjectFactory()
                            .newPrimitiveValueBuilder().setType(EdmPrimitiveTypeKind.Guid).setValue(null).build());
                }
                return odataClient.getObjectFactory().newPrimitiveProperty(name,
                        odataClient.getObjectFactory().newPrimitiveValueBuilder().setType(EdmPrimitiveTypeKind.Guid)
                                .setValue(java.util.UUID.fromString(value)).build());
            } else {
                return odataClient.getObjectFactory().newPrimitiveProperty(name, odataClient.getObjectFactory()
                        .newPrimitiveValueBuilder().setType(primitiveTypeKind).setValue(value).build());
            }
        }
        return null;
    }

    private ClientValue getStringValue(String value, ODataClient odataClient, EdmElement propType) {
        if (value == null && configuration.isIgnoreNull()) {
            return null;
        } else if (propType.getType().getKind() == EdmTypeKind.ENUM) {
            return odataClient.getObjectFactory().newEnumValue(propType.getType().getName(), value);
        } else if (propType.getType().getKind() == EdmTypeKind.PRIMITIVE) {
            EdmPrimitiveTypeKind primitiveTypeKind = EdmPrimitiveTypeKind.valueOfFQN(propType.getType().getFullQualifiedName());
            if (primitiveTypeKind == EdmPrimitiveTypeKind.Guid) {
                if (value == null) {
                    return odataClient.getObjectFactory().newPrimitiveValueBuilder().setType(EdmPrimitiveTypeKind.Guid)
                            .setValue(null).build();
                }
                return odataClient.getObjectFactory().newPrimitiveValueBuilder().setType(EdmPrimitiveTypeKind.Guid)
                        .setValue(java.util.UUID.fromString(value)).build();
            } else {
                return odataClient.getObjectFactory().newPrimitiveValueBuilder().setType(primitiveTypeKind).setValue(value)
                        .build();
            }
        }
        return null;
    }

    private <T> ClientValue getPrimitiveValue(T value, EdmElement prop, ODataClient odataClient) {
        if (value == null && configuration.isIgnoreNull()) {
            return null;
        } else {
            EdmPrimitiveTypeKind primitiveTypeKind = EdmPrimitiveTypeKind.valueOfFQN(prop.getType().getFullQualifiedName());
            return odataClient.getObjectFactory().newPrimitiveValueBuilder().setType(primitiveTypeKind).setValue(value).build();
        }
    }

}
