package org.talend.components.magentocms.input;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.magentocms.common.MagentoCmsConfigurationBase;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.service.http.BadCredentialsException;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;

import javax.json.JsonObject;
import java.io.IOException;
import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class InputIterator implements Iterator<JsonObject> {

    private final String magentoUrl;

    private final Map<String, String> queryParameters;

    private final MagentoHttpClientService magentoHttpClientService;

    private final MagentoCmsConfigurationBase magentoCmsConfigurationBase;

    private Iterator<JsonObject> dataListIterator;

    private int currentPage = 0;

    private int pageSize = 200;

    private HashSet<Integer> previousIds = new HashSet<>();

    @Override
    public boolean hasNext() {
        if (dataListIterator != null && dataListIterator.hasNext()) {
            return true;
        } else {
            try {
                reloadIterator();
                return dataListIterator.hasNext();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public JsonObject next() {
        if (dataListIterator.hasNext()) {
            JsonObject res = dataListIterator.next();
            previousIds.add(res.getInt("id"));
            return res;
        } else {
            try {
                reloadIterator();
                JsonObject res = dataListIterator.next();
                previousIds.add(res.getInt("id"));
                return res;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void reloadIterator()
            throws UnknownAuthenticationTypeException, BadRequestException, IOException, BadCredentialsException {
        currentPage++;
        queryParameters.put("searchCriteria[currentPage]", String.valueOf(currentPage));
        queryParameters.put("searchCriteria[pageSize]", String.valueOf(pageSize));
        List<JsonObject> dataList = magentoHttpClientService.getRecords(magentoUrl, queryParameters);
        // check if new data are not same as previous
        if (!dataList.isEmpty() && previousIds.contains(dataList.get(0).getInt("id"))) {
            dataList = new ArrayList<>();
        }
        dataListIterator = dataList.iterator();
    }
}
