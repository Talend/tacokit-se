package org.talend.components.jdbc.output.statement;

import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface QueryManager extends Serializable {

    List<Reject> execute(List<Record> records, JdbcService.JdbcDatasource dataSource) throws SQLException, IOException;
}
