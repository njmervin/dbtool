package org.yuyun.dbtool.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.yuyun.dbtool.processor.Processor.isStringIn;

public class OracleDB implements RelationalDB {
    @Override
    public String getTableDDL(Connection conn, String tableName) throws SQLException {
        String sql, tableOwner;
        StringBuilder sb = new StringBuilder();
        ResultSet rs;
        int flag;
        Statement stmt = conn.createStatement();

        //获取表名和所有者
        String[] parts = tableName.split("\\.");
        if(parts.length == 2) {
            tableOwner = parts[0];
            tableName = parts[1];
        }
        else {
            rs = stmt.executeQuery("SELECT user FROM dual");
            rs.next();
            tableOwner = rs.getString(1);
            rs.close();
        }

        sql = String.format("SELECT owner, table_name FROM all_tables WHERE OWNER = '%s' AND TABLE_NAME = '%s'",
                tableOwner.toUpperCase(),
                tableName.toUpperCase());
        rs = stmt.executeQuery(sql);
        if(rs.next()) {
            tableOwner = rs.getString(1);
            tableName = rs.getString(2);
        }
        else
            return null;

        sb.append(String.format("CREATE TABLE \"%s\".\"%s\"", tableOwner, tableName));
        sb.append("\n(");

        //查询数据列
        sql = String.format("SELECT * FROM ALL_TAB_COLUMNS WHERE OWNER = '%s' AND TABLE_NAME = '%s' ORDER BY COLUMN_ID", tableOwner, tableName);
        rs = stmt.executeQuery(sql);
        flag = 0;
        while (rs.next()) {
            flag += 1;
            String dataType = rs.getString("DATA_TYPE");
            int prec = rs.getInt("DATA_PRECISION");
            if(rs.wasNull())
                prec = -1;
            int scale = rs.getInt("DATA_SCALE");
            if(rs.wasNull())
                scale = -1;
            String nullable = rs.getString("NULLABLE");
            String data_default = rs.getString("DATA_DEFAULT");

            if(flag > 1)
                sb.append(",");
            sb.append("\n\t");
            sb.append("\"").append(rs.getString("COLUMN_NAME")).append("\"");
            sb.append(" ").append(dataType);

            if(isStringIn(dataType, true, Arrays.asList("NUMBER", "FLOAT"))) {
                if(prec != -1 || scale != -1) {
                    sb.append("(");
                    if (prec >= 0)
                        sb.append(prec);
                    else
                        sb.append("*");
                    if (scale >= 0) {
                        sb.append(",");
                        sb.append(scale);
                    }
                    sb.append(")");
                }
            }
            else if(isStringIn(dataType, true, Arrays.asList("CHAR", "VARCHAR2", "NCHAR", "NVARCHAR2"))) {
                sb.append("(").append(rs.getInt("CHAR_LENGTH"));
                if(isStringIn(dataType, true, Arrays.asList("CHAR", "VARCHAR2"))
                        && rs.getString("CHAR_USED").equalsIgnoreCase("C"))
                    sb.append(" CHAR");
                sb.append(")");
            }
            else if(isStringIn(dataType, true, Arrays.asList("RAW"))) {
                sb.append("(").append(rs.getInt("DATA_LENGTH")).append(")");
            }

            if(data_default != null && !data_default.isEmpty())
                sb.append(" DEFAULT").append(" ").append(data_default.trim());

            if(nullable.equalsIgnoreCase("N"))
                sb.append(" NOT NULL");
        }
        rs.close();

        //检查主键
        sql = String.format("SELECT * FROM ALL_CONSTRAINTS  WHERE  OWNER = '%s' AND TABLE_NAME = '%s' AND CONSTRAINT_TYPE = 'P'", tableOwner, tableName);
        rs = stmt.executeQuery(sql);
        if(rs.next()) {
            String indexOwer = rs.getString("INDEX_OWNER");
            String indexName = rs.getString("INDEX_NAME");
            sb.append(",").append("\n\t");
            sb.append(String.format("CONSTRAINT \"%s\" PRIMARY KEY (", rs.getString("CONSTRAINT_NAME")));
            rs.close();

            if(indexOwer == null)
                indexOwer = tableOwner;
            sql = String.format("SELECT * FROM ALL_IND_COLUMNS WHERE INDEX_OWNER = '%s' AND INDEX_NAME = '%s' ORDER BY COLUMN_POSITION", indexOwer, indexName);
            rs = stmt.executeQuery(sql);
            flag = 0;
            while (rs.next()) {
                flag += 1;
                if(flag > 1)
                    sb.append(", ");
                sb.append("\"").append(rs.getString("COLUMN_NAME")).append("\"");
            }
            rs.close();

            sb.append(")");
        }

        sb.append("\n);");

        //字段注释
        sql = String.format("SELECT * FROM ALL_COL_COMMENTS WHERE OWNER = '%s' AND TABLE_NAME = '%s' AND COMMENTS IS NOT NULL", tableOwner, tableName);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            sb.append("\n");
            sb.append("COMMENT ON COLUMN ");
            sb.append(tableOwner).append(".").append(tableName).append(".").append(rs.getString("COLUMN_NAME"));
            sb.append(" IS '");
            sb.append(rs.getString("COMMENTS").replace("'", "''"));
            sb.append("';");
        }
        rs.close();

        //检查索引
        List<String[]> indexList = new ArrayList<>();
        sql = String.format("SELECT * FROM ALL_INDEXES WHERE TABLE_OWNER = '%s' AND TABLE_NAME = '%s'", tableOwner, tableName);
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            indexList.add(new String[]{rs.getString("OWNER"), rs.getString("INDEX_NAME"), rs.getString("UNIQUENESS")});
        }
        rs.close();

        if(!indexList.isEmpty()) {
            for (String[] index : indexList) {
                sb.append("\n");
                sb.append("CREATE");
                if(index[2].equalsIgnoreCase("UNIQUE"))
                    sb.append(" UNIQUE");
                sb.append(String.format(" INDEX \"%s\".\"%s\" ON \"%s\".\"%s\"(", index[0], index[1], tableOwner, tableName));

                sql = String.format("SELECT * FROM ALL_IND_COLUMNS WHERE INDEX_OWNER = '%s' AND INDEX_NAME = '%s' ORDER BY COLUMN_POSITION", index[0], index[1]);
                rs = stmt.executeQuery(sql);
                flag = 0;
                while (rs.next()) {
                    flag += 1;
                    if(flag > 1)
                        sb.append(", ");
                    sb.append("\"").append(rs.getString("COLUMN_NAME")).append("\"");
                    if(rs.getString("DESCEND").equalsIgnoreCase("DESC"))
                        sb.append(" DESC");
                }
                rs.close();

                sb.append(");");
            }
        }

        return sb.toString();
    }
}
