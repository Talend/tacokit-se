package org.talend.components.adlsgen2.runtime.output;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.adlsgen2")
public class BlobWriterTest extends AdlsGen2TestBase{

    static BlobWriter blobWriter;

    @BeforeEach
    public void setup() throws Exception {
        super.setUp();
        blobWriter = new ParquetBlobWriter(outputConfiguration, recordBuilderFactory, jsonBuilderFactory, service,
                tokenProviderService);
    }

    @Test
    public void testCompleteOnZeroInputRecords() {
        /* 
         * There was a NPE issue.
         * No batch was created, since the {@link BlobWriter#newBatch()} was not called.
        */
        Assertions.assertDoesNotThrow(blobWriter::complete);
    }
}
