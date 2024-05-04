package org.yuyun.dbtool.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface RelationalDB {
    String getTableDDL(Connection conn, String tableName) throws SQLException;
}
