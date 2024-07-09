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
import java.util.Map;

public class ExportSingleColumnProcessor extends Processor{
    private String argTableName;
    private String argSQL;
    private String argFields;
    private String argWhere;
    private int argLimit;
    private int argFeedback;
    private String argOutputFile;

    @Override
    public String getActionName() {
        return "export_single_column";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    @Override
    protected void parseArguments(Map<String, String> args) {
        this.argTableName = checkOptionalArgumentString(args, "table", "");
        this.argSQL = checkOptionalArgumentString(args, "sql", "");
        this.argFields = checkOptionalArgumentString(args, "fields", "");
        this.argWhere = checkOptionalArgumentString(args, "where", "");
        this.argLimit = checkOptionalArgumentInt(args, "limit", Integer.MAX_VALUE);
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
            sb.append(argFields.isEmpty() ? "*" : argFields);
            sb.append(" from ");
            sb.append(argTableName);

            if (!argWhere.isEmpty()) {
                if(argWhere.length() < 5 || !argWhere.substring(0, 5).equalsIgnoreCase("where"))
                    sb.append(" where");
                sb.append(" ").append(argWhere);
            }
            sql = sb.toString();
        }
        printMsg(LogLevel.INFO, String.format("SQL: %s", sql));

        exportData(sql);
    }

    private void exportData(String sql) throws SQLException, IOException {
        //导出
        Statement stmt = this.getConnection().createStatement();
        printMsg(LogLevel.INFO, "Execute query ...");
        ResultSet rs = stmt.executeQuery(sql);

        //删除文件
        new File(argOutputFile).delete();
        PrintWriter file = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(argOutputFile)), StandardCharsets.UTF_8));

        printMsg(LogLevel.INFO, "Start ...");

        int rows = 0;

        while (rs.next()) {
            String text = rs.getString(1);
            if(text != null) {
                file.println(text);
            }

            rows += 1;

            if((rows % argFeedback) == 0) {
                printMsg(LogLevel.INFO, String.format("%d rows ...", rows));
            }
            if(rows >= argLimit)
                break;
        }

        file.close();

        printMsg(LogLevel.INFO, String.format("Total: %d rows", rows));

        this.setResultInfo("rows", rows);

        rs.close();
        stmt.close();
    }
}
