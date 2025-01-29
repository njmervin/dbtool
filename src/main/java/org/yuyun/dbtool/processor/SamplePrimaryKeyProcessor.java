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

public class SamplePrimaryKeyProcessor extends Processor{
    private String argTableName;
    private String argField;
    private String argWhere;
    private int argFeedback;
    private String argOutputFile;
    private int argSampleCount;

    @Override
    public String getActionName() {
        return "sample";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    /**
     * sample
     *     --table S    <可选>表名
     *     --field S    <可选>要导出的主键字段
     *     --where S    <可选>导出数据的条件和排序，如果未设置，则导出所有行
     *     --count N    <必选>采样的间隔数量
     *     --feedback N <可选>每多少行显示进度提示，默认为10000行
     *     --output S   目标数据文件路径
     */
    @Override
    protected void parseArguments(Map<String, String> args) {
        this.argTableName = checkMandatoryArgumentString(args, "table");
        this.argField = checkMandatoryArgumentString(args, "field");
        this.argWhere = checkOptionalArgumentString(args, "where", "");
        this.argFeedback = checkOptionalArgumentInt(args, "feedback", 10000);
        this.argSampleCount = checkMandatoryArgumentInt(args, "count");
        this.argOutputFile = checkMandatoryArgumentString(args, "output");
    }

    @Override
    protected void process() throws Exception {
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

        sb.append(" order by 1");

        printMsg(LogLevel.INFO, String.format("SQL: %s", sb));

        exportData(sb.toString());
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

        long start = 0, end = 0, total = 0;
        int rows = 0;
        while (rs.next()) {
            total += 1;
            if(total == 1)
                start = rs.getLong(1);

            end = rs.getLong(1);
            if(total == this.argSampleCount) {
                file.println(String.format("%d,%d,%d", start, end, total));
                total = 0;
            }

            rows += 1;
            if((rows % argFeedback) == 0)
                printMsg(LogLevel.INFO, String.format("%d rows ...", rows));
        }

        if(total > 0) {
            file.println(String.format("%d,%d,%d", start, end, total));
        }

        file.close();

        printMsg(LogLevel.INFO, String.format("Total: %d rows", rows));

        this.setResultInfo("rows", rows);

        rs.close();
        stmt.close();
    }
}
