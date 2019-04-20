package org.talend.components.solr.output;

import javax.json.JsonObject;

@FunctionalInterface
public interface SolrActionExecutor {

    void execute(JsonObject record);
}
