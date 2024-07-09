package org.yuyun.dbtool.processor;

import org.yuyun.dbtool.LogLevel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class ExecScriptProcessor extends Processor{
    private int argFeedback;
    private int argBatch;
    private String argInputFile;
    private int argStart;

    @Override
    public String getActionName() {
        return "exec_script";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    @Override
    protected void parseArguments(Map<String, String> args) {
        this.argFeedback = checkOptionalArgumentInt(args, "feedback", 10000);
        this.argBatch = checkOptionalArgumentInt(args, "batch", 10000);
        this.argInputFile = checkMandatoryArgumentString(args, "input");
        this.argStart = checkOptionalArgumentInt(args, "start", 1);
    }

    @Override
    protected void process() throws Exception {
        int lineno = 0, rows = 0;

        try {
            Statement stmt = this.getConnection().createStatement();
            this.getConnection().setAutoCommit(false);

            int batch = 0;
            String sql;
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(this.argInputFile)), StandardCharsets.UTF_8));
            while (true) {
                String line = reader.readLine();
                lineno += 1;
                if((lineno % argFeedback) == 0) {
                    printMsg(LogLevel.INFO, String.format("%d lines ...", lineno));
                }
                if(line == null)
                    break;

                if(sb.length() > 0)
                    sb.append('\n');

                if(!line.endsWith(";"))
                    sb.append(line);
                else {
                    sb.append(line, 0, line.length() - 1);

                    sql = sb.toString().trim();
                    if(!sql.isEmpty()) {
                        rows += 1;
                        if(rows >= this.argStart) {
                            batch += 1;
                            stmt.executeUpdate(sql);
                            if (batch >= argBatch) {
                                this.getConnection().commit();
                                batch = 0;
                            }
                        }
                    }

                    sb.setLength(0);
                }
            }
            reader.close();

            sql = sb.toString().trim();
            if(!sql.isEmpty()) {
                rows += 1;
                if(rows > this.argStart) {
                    stmt.executeUpdate(sql);
                    this.getConnection().commit();
                }
            }

            stmt.close();
            this.getConnection().setAutoCommit(true);
        }
        catch (SQLException e) {
            printMsg(LogLevel.ERROR, String.format("Import failed at line #%d", lineno));
            printMsg(e);
            throw new RuntimeException(e);
        }
    }
}
