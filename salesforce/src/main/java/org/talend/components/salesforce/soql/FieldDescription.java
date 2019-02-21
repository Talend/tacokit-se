package org.talend.components.salesforce.soql;

import java.util.List;

/**
 * Describes SOQL query field, contains
 * simple field name - name how it appears in Salesforce entity
 * full name - name which should be used in component avro schema
 * entity names - ordered list of Salesforce entities names to query to get field type
 */
public class FieldDescription {

    private String fullName;

    private String simpleName;

    private List<String> entityNames;

    public FieldDescription(String fullName, String simpleName, List<String> entityNames) {
        super();
        this.fullName = fullName;
        this.simpleName = simpleName;
        this.entityNames = entityNames;
    }

    public String getFullName() {
        return fullName;
    }

    public String getSimpleName() {
        return simpleName;
    }

    public List<String> getEntityNames() {
        return entityNames;
    }

}
