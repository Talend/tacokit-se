package org.talend.components.solr.common;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@WithComponents("org.talend.components.solr")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SolrDatasetTest {

    @Test
    public void testNoSlashSuffix() {
        SolrDataset solrDataset = prepareSolrConnectionConfiguration("http://localhost:8983/solr", "testCore");
        assertEquals("http://localhost:8983/solr/testCore", solrDataset.getFullUrl());
    }

    @Test
    public void testSlashSuffix() {
        SolrDataset solrDataset = prepareSolrConnectionConfiguration("http://localhost:8983/solr/", "testCore");
        assertEquals("http://localhost:8983/solr/testCore", solrDataset.getFullUrl());
    }

    @Test
    public void testInverseSlashSuffix() {
        SolrDataset solrDataset = prepareSolrConnectionConfiguration("http://localhost:8983/solr\\", "testCore");
        assertEquals("http://localhost:8983/solr\\testCore", solrDataset.getFullUrl());
    }

    private SolrDataset prepareSolrConnectionConfiguration(String url, String core) {
        SolrDataset solrDataset = new SolrDataset();
        SolrDataStore solrDataStore = new SolrDataStore();
        solrDataStore.setUrl(url);
        solrDataset.setDataStore(solrDataStore);
        solrDataset.setCore(core);
        return solrDataset;
    }

}
