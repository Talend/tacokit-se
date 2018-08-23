package org.talend.components.fileio.hdfs;

import java.util.ArrayList;
import java.util.List;

import org.talend.components.fileio.configuration.SimpleFileIOFormat;
import org.talend.components.fileio.s3.S3DataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.components.fileio.hdfs.SimpleFileIODataStore;

@Service
public class SimpleFileIOService {

    @Suggestions("findOptions")
    public SuggestionValues findOptions(@Option("datastore") final SimpleFileIODataStore dataStore,
            @Option("format") final SimpleFileIOFormat format) {
        List<SuggestionValues.Item> list = new ArrayList<>();
        list.add(new SuggestionValues.Item("option1", "option1"));
        list.add(new SuggestionValues.Item("option2", "option2"));
        // list.add(new SuggestionValues.Item(format.name(), format.name()));
        return new SuggestionValues(true, list);
    }

}
