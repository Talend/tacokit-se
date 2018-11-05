package org.talend.components.zendesk.sources.get;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.helpers.JsonHelper;

import javax.json.JsonObject;
import javax.json.JsonReaderFactory;
import java.util.Iterator;

@Slf4j
@RequiredArgsConstructor
public class InputIterator implements Iterator<JsonObject> {

    private final Iterator<?> dataListIterator;

    private final JsonReaderFactory jsonReaderFactory;

    @Override
    public boolean hasNext() {
        return dataListIterator.hasNext();
    }

    @Override
    public JsonObject next() {
        if (!dataListIterator.hasNext()) {
            return null;
        }
        Object obj = dataListIterator.next();
        return JsonHelper.toJsonObject(obj, jsonReaderFactory);
    }
}
