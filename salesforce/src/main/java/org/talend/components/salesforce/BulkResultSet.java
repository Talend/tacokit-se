package org.talend.components.salesforce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class BulkResultSet {

    private final com.csvreader.CsvReader reader;

    private final List<String> header;

    private JsonBuilderFactory jsonBuilderFactory;

    public BulkResultSet(com.csvreader.CsvReader reader, List<String> header, final JsonBuilderFactory jsonBuilderFactory) {
        this.reader = reader;
        this.header = header;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    public JsonObject next() {
        try {
            boolean hasNext = reader.readRecord();
            String[] row;
            if (hasNext) {
                if ((row = reader.getValues()) != null) {
                    Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (int i = 0; i < this.header.size(); i++) {
                        // We replace the . with _ to add support of relationShip Queries
                        // The relationShip Queries Use . in Salesforce and we use _ in Talend (Studio)
                        // So Account.Name in SF will be Account_Name in Talend
                        result.put(header.get(i).replace('.', '_'), row[i]);
                    }
                    final JsonObjectBuilder jsonBuilder = jsonBuilderFactory.createObjectBuilder();
                    result.entrySet().stream().filter(it -> it.getValue() != null)
                            .forEach(e -> jsonBuilder.add(e.getKey(), e.getValue()));
                    return jsonBuilder.build();
                } else {
                    return next();
                }
            } else {
                this.reader.close();
            }
            return null;
        } catch (IOException e) {
            this.reader.close();
            throw new IllegalStateException(e);
        }
    }

}