package org.talend.components.solr.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.apache.solr.common.params.CommonParams;
import org.talend.components.solr.common.FilterCriteria;
import org.talend.components.solr.source.SolrInputMapperConfiguration;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Service
public class SolrConnectorUtils {

    private static final String SOLR_FIELD_PROPERTY_STORED = "stored";

    private static final String SOLR_FIELD_PROPERTY_NAME = "name";

    private static final String SOLR_FIELD_PROPERTY_TYPE = "type";

    private static final String SOLR_PARAM_QUERY = "q";

    private static final String SOLR_PARAM_FILTER_QUERY = "fq";

    private static final String SOLR_PARAM_START = "start";

    private static final String SOLR_PARAM_ROWS = "rows";

    private static final Set<String> SOLR_FIELD_PROPERTY_TYPES_DOUBLE = Stream.of("pdouble", "pfloat")
            .collect(Collectors.toSet());

    private static final Set<String> SOLR_FIELD_PROPERTY_TYPES_INT = Stream.of("plong", "pint").collect(Collectors.toSet());

    private static final Set<String> SOLR_FIELD_PROPERTY_TYPES_BOOL = Stream.of("boolean").collect(Collectors.toSet());

    public String trimQuotes(String value) {
        int length = value.length();
        if (length >= 2 && (value.charAt(0) == '"' || value.charAt(0) == '\'')
                && (value.charAt(length - 1) == '"' || value.charAt(length - 1) == '\'')) {
            return value.substring(1, length - 1);
        }
        return value;
    }

    public String createQueryFromRecord(JsonObject record) {
        StringBuilder query = new StringBuilder();
        Set<String> keySet = record.keySet();
        boolean isFirst = true;
        for (String key : keySet) {
            String value = getStringValue(key, record);
            if (StringUtils.isNotBlank(checkQuotes(value))) {
                String subQuery = (isFirst ? "" : " AND ") + key + ":" + checkQuotes(value);
                query.append(subQuery);
                isFirst = false;
            }
        }

        return query.toString();
    }

    public SolrQuery generateQuery(List<FilterCriteria> filterQuery, String start, String rows) {
        SolrInputMapperConfiguration configuration = new SolrInputMapperConfiguration();
        configuration.setFilterQuery(filterQuery);
        configuration.setStart(start);
        configuration.setRows(rows);
        return generateQuery(configuration);
    }

    public SolrQuery generateQuery(SolrInputMapperConfiguration configuration) {
        if (StringUtils.isNotBlank(configuration.getRawQuery())) {
            return generateQuery(configuration.getRawQuery());
        }
        return generateConfigQuery(configuration);
    }

    public SolrQuery generateConfigQuery(SolrInputMapperConfiguration configuration) {
        SolrQuery query = new SolrQuery("*:*");
        configuration.getFilterQuery().forEach(e -> addFilterQuery(e, query));
        query.setRows(parseInt(configuration.getRows()));
        query.setStart(parseInt(configuration.getStart()));
        return query;
    }

    public SolrQuery generateQuery(String rawQuery) {
        SolrQuery solrQuery = new SolrQuery("*:*");
        if (StringUtils.isBlank(rawQuery)) {
            return null;
        }
        Arrays.stream(rawQuery.split("&")).forEach(e -> setParameter(solrQuery, e));
        return solrQuery;
    }

    private String getStringValue(String key, JsonObject record) {
        return record.get(key) != null ? record.get(key).toString() : null;
    }

    private String checkQuotes(String value) {
        return addQuotes(trimQuotes(value));
    }

    private String addQuotes(String value) {
        int length = value.length();
        if (length >= 2 && !(value.charAt(0) == '"' && value.charAt(length - 1) == '"')
                && StringUtils.containsWhitespace(value)) {
            return "\"" + value + "\"";
        }
        return value;
    }

    public Schema getSchemaFromRepresentation(SchemaRepresentation representation,
            final RecordBuilderFactory recordBuilderFactory) {
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        if (representation == null) {
            return schemaBuilder.build();
        }
        List<Map<String, Object>> fields = representation.getFields();
        for (Map<String, Object> field : fields) {
            String fieldName = getFieldName(field);
            if (fieldName != null && checkStored(field)) {
                Schema.Entry schemaEntry = recordBuilderFactory.newEntryBuilder().withName(fieldName)
                        .withType(getFieldType(field)).build();
                schemaBuilder.withEntry(schemaEntry);
            }
        }
        return schemaBuilder.build();
    }

    private boolean checkStored(Map<String, Object> field) {
        Object stored = field.get(SOLR_FIELD_PROPERTY_STORED);
        return (stored == null || stored.equals(true));
    }

    private String getFieldName(Map<String, Object> field) {
        Object name = field.get(SOLR_FIELD_PROPERTY_NAME);
        if (name == null) {
            return null;
        }
        return name.toString();
    }

    private Schema.Type getFieldType(Map<String, Object> field) {
        Object type = field.get(SOLR_FIELD_PROPERTY_TYPE);
        if (SOLR_FIELD_PROPERTY_TYPES_INT.contains(type)) {
            return Schema.Type.INT;
        } else if (SOLR_FIELD_PROPERTY_TYPES_BOOL.contains(type)) {
            return Schema.Type.BOOLEAN;
        } else if (SOLR_FIELD_PROPERTY_TYPES_DOUBLE.contains(type)) {
            return Schema.Type.DOUBLE;
        } else {
            return Schema.Type.STRING;
        }
    }

    public Collection<String> getCoreListFromResponse(CoreAdminResponse cores) {
        if (cores != null) {
            return IntStream.range(0, cores.getCoreStatus().size()).mapToObj(i -> cores.getCoreStatus().getName(i))
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }

    public void addFilterQuery(FilterCriteria row, SolrQuery query) {
        String field = row.getField();
        String value = row.getValue();
        if (StringUtils.isNotBlank(field) && StringUtils.isNotBlank(value)) {
            query.addFilterQuery(field + ":" + value);
        }
    }

    public Integer parseInt(String value) {
        Integer result = 0;
        try {
            result = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn(e.getMessage());
        }
        return result;
    }

    public String getMessages(Throwable e) {
        Set<String> messages = new LinkedHashSet<>();
        while (e != null) {
            if (StringUtils.isNotBlank(e.getMessage())) {
                messages.add(e.getMessage().trim());
            }
            e = e.getCause();
        }
        return messages.stream().collect(Collectors.joining("\n"));
    }

    public String getCustomLocalizedMessage(String message, Messages i18n) {
        if (message.contains("Bad credentials")) {
            return i18n.badCredentials();
        }
        return message;
    }

    private void setParameter(SolrQuery solrQuery, String param) {
        int idx = param.indexOf("=");
        String key = idx > 0 ? param.substring(0, idx) : param;
        String value = idx > 0 && param.length() > idx + 1 ? param.substring(idx + 1) : null;
        switch (key) {
        case SOLR_PARAM_QUERY:
            solrQuery.set(CommonParams.Q, value);
            break;
        case SOLR_PARAM_FILTER_QUERY:
            solrQuery.addFilterQuery(value);
            break;
        case SOLR_PARAM_ROWS:
            solrQuery.setRows(parseInt(value));
            break;
        case SOLR_PARAM_START:
            solrQuery.setStart(parseInt(value));
            break;
        }
    }

}
