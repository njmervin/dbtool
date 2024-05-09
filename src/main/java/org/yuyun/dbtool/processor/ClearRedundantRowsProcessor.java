package org.yuyun.dbtool.processor;

import org.yuyun.dbtool.LogLevel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.util.Map;

public class ClearRedundantRowsProcessor extends Processor{
    private String argTableName;
    private String argField;
    private int argBatch;
    private int argFeedback;
    private String argInputFile;

    @Override
    public String getActionName() {
        return "clear_pk";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    /**
     * clear_pk
     *     --table S    表名
     *     --field S    主键字段名
     *     --feedback N <可选> 每多少行显示进度提示，默认为 10000
     *     --batch N    <可选> 批量提交的行数，默认为 10000
     *     --input S    要处理的数据文件路径
     */
    @Override
    protected void parseArguments(Map<String, String> args) {
        this.argTableName = checkMandatoryArgumentString(args, "table");
        this.argField = checkMandatoryArgumentString(args, "field");
        this.argBatch = checkOptionalArgumentInt(args, "batch", 10000);
        this.argFeedback = checkOptionalArgumentInt(args, "feedback", 10000);
        this.argInputFile = checkMandatoryArgumentString(args, "input");
    }

    @Override
    protected void process() throws Exception {
        BufferedReader file = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(this.argInputFile)), StandardCharsets.UTF_8));
        PreparedStatement ps = this.getConnection().prepareStatement(String.format("delete from %s where %s = ?", this.argTableName, this.argField));
        String flag = file.readLine();

        boolean isNumber = flag.equals("*number");
        int batch = 0;
        int rows = 0;

        this.getConnection().setAutoCommit(false);
        while(true) {
            String line = file.readLine();
            if(line == null)
                break;
            if(!line.endsWith(",-"))
                continue;

            String id = line.substring(0, line.length() - 2);
            if(isNumber)
                ps.setLong(1, Long.parseLong(id));
            else
                ps.setString(1, id);
            ps.addBatch();
            batch += 1;
            rows += 1;

            if(batch >= argBatch) {
                ps.executeBatch();
                this.getConnection().commit();
                batch = 0;
            }

            if((rows % argFeedback) == 0)
                printMsg(LogLevel.INFO, String.format("%d rows ...", rows));
        }

        if(batch > 0) {
            ps.executeBatch();
            this.getConnection().commit();
        }

        getConnection().setAutoCommit(true);
        printMsg(LogLevel.INFO, String.format("Total: %d rows", rows));

        this.setResultInfo("rows", rows);

        file.close();
    }
}
