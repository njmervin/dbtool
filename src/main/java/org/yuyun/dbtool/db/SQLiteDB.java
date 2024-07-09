package org.yuyun.dbtool.db;

import java.sql.Connection;
import java.sql.SQLException;

public class SQLiteDB implements RelationalDB {
    @Override
    public String getTableDDL(Connection conn, String tableName) throws SQLException {
        return null;
    }
}
