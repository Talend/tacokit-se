package org.talend.components.jdbc.service;

import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.dataset.TableNameDataset;

public interface ActionOnDataProvider {

    OutputConfig.ActionOnData[] getActions(final TableNameDataset dataset);
}
