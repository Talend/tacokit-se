package org.talend.components.solr.output;

import lombok.Data;
import org.talend.components.solr.common.SolrDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "action" }) })
@Documentation("Configuration for Solr Output component")
public class SolrProcessorOutputConfiguration implements Serializable {

    @Option
    @Documentation("Connection to Solr Data Collection")
    private SolrDataset dataset;

    @Option
    @Documentation("Solr Action. Allows to choose an action to add or to delete a document from Solr collection")
    private SolrAction action = SolrAction.UPSERT;

}