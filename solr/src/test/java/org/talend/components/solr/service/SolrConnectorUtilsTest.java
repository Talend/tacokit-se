package org.talend.components.solr.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.schema.SchemaRepresentation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.solr.common.FilterCriteria;
import org.talend.components.solr.output.SolrActionExecutorFactory;
import org.talend.components.solr.output.UnsupportedSolrActionException;
import org.talend.components.solr.source.SolrInputMapperConfiguration;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@WithComponents("org.talend.components.solr")
public class SolrConnectorUtilsTest {

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private SolrConnectorUtils util;

    private final static Messages messages = new TestMessages();

    @BeforeEach
    public void init() {
        util = new SolrConnectorUtils();
    }

    @Test
    public void testTrimQuotesNoQuotes() {
        String result = util.trimQuotes("testString");
        assertEquals("testString", result);
    }

    @Test
    public void testTrimQuotesWithQuotes() {
        String result = util.trimQuotes("\"testString\"");
        assertEquals("testString", result);
    }

    @Test
    public void testTrimQuotesPrefix() {
        String result = util.trimQuotes("\"testString");
        assertEquals("\"testString", result);
    }

    @Test
    public void testTrimQuotesSuffix() {
        String result = util.trimQuotes("testString\"");
        assertEquals("testString\"", result);
    }

    @Test
    public void testTrimSingleQuote() {
        String result = util.trimQuotes("\'testString\'");
        assertEquals("testString", result);
    }

    @Test
    public void testCreateQueryFromPositive() {
        JsonObject record = Json.createObjectBuilder().add("field1", "value1").add("field2", "value2").add("field3", "value3")
                .build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field1:value1 AND field2:value2 AND field3:value3", result);
    }

    @Test
    public void testCreateQueryFromRecordWithWhiteSpaces() {
        JsonObject record = Json.createObjectBuilder().add("field1", "value 1").add("field2", "value 2").add("field3", "value 3")
                .build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field1:\"value 1\" AND field2:\"value 2\" AND field3:\"value 3\"", result);
    }

    @Test
    public void testCreateQueryFromRecordWithEmptyValue() {
        JsonObject record = Json.createObjectBuilder().add("field1", "").add("field2", "value2").add("field3", "value3").build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field2:value2 AND field3:value3", result);
    }

    @Test
    public void testCreateQueryFromRecordShortString() {
        JsonObject record = Json.createObjectBuilder().add("field1", "1").add("field2", "2").add("field3", "3").build();
        String result = util.createQueryFromRecord(record);

        assertEquals("field1:1 AND field2:2 AND field3:3", result);
    }

    @Test
    public void testGetSchemaFromRepresentationNullRepresentation() {
        assertTrue(util.getSchemaFromRepresentation(null, recordBuilderFactory).getEntries().isEmpty());
    }

    @Test
    public void testGetSchemaFromRepresentationEmpty() {
        SchemaRepresentation representation = new SchemaRepresentation();
        representation.setFields(Arrays.asList(new HashMap<>()));
        assertTrue(util.getSchemaFromRepresentation(representation, recordBuilderFactory).getEntries().isEmpty());
    }

    @Test
    public void testGetSchemaFromRepresentation() {
        InputStream fis;
        Reader reader = null;
        try {
            fis = new FileInputStream("src/test/resources/test-get-schema.json");
            reader = new InputStreamReader(fis);
        } catch (FileNotFoundException e) {
            log.error(e.getMessage(), e);
            fail();
        }
        Gson gson = new GsonBuilder().create();
        SchemaRepresentation representation = gson.fromJson(reader, SchemaRepresentation.class);

        Map<String, Schema.Type> schemaTypes = new TreeMap<>();
        schemaTypes.put("_src_", Schema.Type.STRING);
        schemaTypes.put("author", Schema.Type.STRING);
        schemaTypes.put("cat", Schema.Type.STRING);
        schemaTypes.put("category", Schema.Type.STRING);
        schemaTypes.put("comments", Schema.Type.STRING);
        schemaTypes.put("content", Schema.Type.STRING);
        schemaTypes.put("inStock", Schema.Type.BOOLEAN);
        schemaTypes.put("popularity", Schema.Type.INT);
        schemaTypes.put("price", Schema.Type.DOUBLE);

        Schema.Builder schemaBuilder = recordBuilderFactory
                .newSchemaBuilder(org.talend.sdk.component.api.record.Schema.Type.RECORD);
        schemaTypes.forEach((key, value) -> {
            schemaBuilder.withEntry(recordBuilderFactory.newEntryBuilder().withName(key).withType(value).build());
        });
        Schema expected = schemaBuilder.build();

        assertEquals(expected, util.getSchemaFromRepresentation(representation, recordBuilderFactory));
    }

    @Test
    public void testGetCoresEmpty() {
        Collection<String> cores = util.getCoreListFromResponse(null);
        assertTrue(cores.isEmpty());
    }

    @Test
    public void testAddFilterQuery() {
        FilterCriteria fc = new FilterCriteria();
        fc.setField("field");
        fc.setValue("value");
        SolrQuery query = new SolrQuery("*:*");
        util.addFilterQuery(fc, query);
        assertEquals("q=*:*&fq=field:value", query.toString());
    }

    @Test
    public void testParseInt() {
        assertEquals(new Integer(1234567), util.parseInt("1234567"));
    }

    @Test
    public void testParseIntNegative() {
        assertEquals(new Integer(0), util.parseInt("1234f567"));
    }

    @Test
    public void testGenerateQuery() {
        String expected = "q=*:*&fq=id:apple&fq=title:Apple&rows=100&start=4";
        FilterCriteria idCriteria = new FilterCriteria();
        idCriteria.setField("id");
        idCriteria.setValue("apple");
        FilterCriteria titleCriteria = new FilterCriteria();
        titleCriteria.setField("title");
        titleCriteria.setValue("Apple");
        String actual = util.generateQuery(Arrays.asList(idCriteria, titleCriteria), "4", "100").toString();
        assertEquals(expected, actual);
    }

    @Test
    public void testCustomLocalizedMessage() {
        assertEquals("Bad credentials message",
                util.getCustomLocalizedMessage("Some text with Bad credentials message", messages));
    }

    @Test
    public void testGenerateQueryFromRawQuery() {
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        String query = "q=*:*&fq=id:apple&fq=title:Apple&rows=100&start=4";
        config.setRawQuery(query);
        SolrQuery actual = util.generateQuery(query);
        SolrQuery expected = new SolrQuery("*:*");
        expected.addFilterQuery("id:apple", "title:Apple");
        expected.setRows(100);
        expected.setStart(4);
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testGenerateQueryFromRawQueryNegative() {
        SolrInputMapperConfiguration config = new SolrInputMapperConfiguration();
        String query = "q=*:*&fq=id:apple&fq=title:Apple&rows=100&start=4";
        config.setRawQuery(query);
        SolrQuery actual = util.generateQuery(query);
        SolrQuery expected = new SolrQuery("*:*");
        expected.addFilterQuery("id:aple", "title:Aple");
        expected.setRows(100);
        expected.setStart(4);
        assertNotEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testSolrActionExecutorFactoryNull() {
        SolrActionExecutorFactory factory = new SolrActionExecutorFactory(null, null, null, messages);
        assertThrows(UnsupportedSolrActionException.class, factory::getSolrActionExecutor);
    }
}
