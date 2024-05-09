package org.yuyun.dbtool.processor;

import org.yuyun.dbtool.LogLevel;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

public class ExportPrimaryKeyProcessor extends Processor{
    private String argTableName;
    private String argSQL;
    private String argField;
    private String argWhere;
    private int argFeedback;
    private String argOutputFile;

    @Override
    public String getActionName() {
        return "export_pk";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    /**
     * export_pk
     *     --table S    <可选>表名
     *     --sql S      <可选>完整的SQL语句
     *     --field S    <可选>要导出的主键字段
     *     --where S    <可选>导出数据的条件和排序，如果未设置，则导出所有行
     *     --feedback N <可选>每多少行显示进度提示，默认为10000行
     *     --output S   目标数据文件路径
     */
    @Override
    protected void parseArguments(Map<String, String> args) {
        this.argTableName = checkOptionalArgumentString(args, "table", "");
        this.argSQL = checkOptionalArgumentString(args, "sql", "");
        this.argField = checkOptionalArgumentString(args, "field", "");
        this.argWhere = checkOptionalArgumentString(args, "where", "");
        this.argFeedback = checkOptionalArgumentInt(args, "feedback", 10000);
        this.argOutputFile = checkMandatoryArgumentString(args, "output");

        if(argTableName.isEmpty() && argSQL.isEmpty())
            throw new RuntimeException("Parameter \"table\" or \"sql\" must be specified");
    }

    @Override
    protected void process() throws Exception {
        String sql = argSQL;

        //生成SQL语句
        if(sql.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("select ");
            sb.append(argField);
            sb.append(" from ");
            sb.append(argTableName);

            if (!argWhere.isEmpty()) {
                if(argWhere.length() < 5 || !argWhere.substring(0, 5).equalsIgnoreCase("where"))
                    sb.append(" where");
                sb.append(" ").append(argWhere);
            }
            sql = sb.toString();
        }
        sql += " order by 1";
        printMsg(LogLevel.INFO, String.format("SQL: %s", sql));

        exportData(sql);
    }

    private void exportData(String sql) throws SQLException, IOException {
        //导出
        Statement stmt = this.getConnection().createStatement();
        printMsg(LogLevel.INFO, "Execute query ...");
        ResultSet rs = stmt.executeQuery(sql);

        //删除文件
        boolean isNumber;
        new File(argOutputFile).delete();
        PrintWriter file = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(argOutputFile)), StandardCharsets.UTF_8));

        switch (rs.getMetaData().getColumnType(1)) {
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                isNumber = true;
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
                isNumber = false;
                break;
            default:
                throw new RuntimeException(String.format("Unsupport field '%s' data type: %d: %s",
                        rs.getMetaData().getColumnLabel(1), rs.getMetaData().getColumnType(1), rs.getMetaData().getColumnTypeName(1)));
        }

        printMsg(LogLevel.INFO, "Start ...");

        file.println(isNumber ? "*number" : "*string");

        int rows = 0;
        while (rs.next()) {
            if(isNumber)
                file.println(rs.getLong(1));
            else
                file.println(rs.getString(1));

            rows += 1;
            if((rows % argFeedback) == 0)
                printMsg(LogLevel.INFO, String.format("%d rows ...", rows));
        }

        file.close();

        printMsg(LogLevel.INFO, String.format("Total: %d rows", rows));

        this.setResultInfo("rows", rows);

        rs.close();
        stmt.close();
    }
}
