package org.yuyun.dbtool.processor;

import org.yuyun.dbtool.LogLevel;

import java.sql.Statement;
import java.util.Map;

public class ExecProcessor extends Processor{
    private String sql;

    @Override
    public String getActionName() {
        return "exec";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    /**
     * exec
     *     --sql S  要执行的SQL语句
     * @param args 原始命令参数
     */
    @Override
    protected void parseArguments(Map<String, String> args) {
        this.sql = checkMandatoryArgumentString(args, "sql");
    }

    @Override
    protected void process() throws Exception {
        Statement stmt = this.getConnection().createStatement();
        printMsg(LogLevel.INFO, String.format("Start execute: %s...", sql));
        int affected = stmt.executeUpdate(sql);
        printMsg(LogLevel.INFO, String.format("Affect %d rows.", affected));
        this.setResultInfo("rows", affected);
        stmt.close();
    }
}
