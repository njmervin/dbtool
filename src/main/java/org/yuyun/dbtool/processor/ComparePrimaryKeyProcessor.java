package org.yuyun.dbtool.processor;

import org.yuyun.dbtool.LogLevel;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Map;

public class ComparePrimaryKeyProcessor extends Processor{
    private String argSrc;
    private String argDest;
    private int argFeedback;
    private String argOutputFile;
    private int rows = 0;
    private int redundantRows = 0, missingRows = 0;

    class Lines {
        private final BufferedReader reader;
        private boolean isFilled = false;
        private String line = null;

        public Lines(BufferedReader reader) {
            this.reader = reader;
        }

        public String fill() throws IOException {
            if(!this.isFilled) {
                this.isFilled = true;
                this.line = this.reader.readLine();
                if(this.line != null && this.line.isEmpty())
                    this.line = null;
            }
            return this.line;
        }

        public void skip() {
            this.isFilled = false;
            this.line = null;
        }
    }

    @Override
    public String getActionName() {
        return "compare_pk";
    }

    @Override
    protected boolean isConnectionUsed() {
        return false;
    }

    /**
     * compare_pk
     *     --src S      源主键文件
     *     --dest S     目标主键文件
     *     --feedback N <可选>每多少行显示进度提示，默认为10000行
     *     --output S   目标数据文件路径
     */
    @Override
    protected void parseArguments(Map<String, String> args) {
        this.argSrc = checkMandatoryArgumentString(args, "src");
        this.argDest = checkMandatoryArgumentString(args, "dest");
        this.argFeedback = checkOptionalArgumentInt(args, "feedback", 10000);
        this.argOutputFile = checkMandatoryArgumentString(args, "output");
    }

    @Override
    protected void process() throws Exception {
        BufferedReader src = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(this.argSrc)), StandardCharsets.UTF_8));
        BufferedReader dest = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(this.argDest)), StandardCharsets.UTF_8));
        PrintWriter file = new PrintWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(argOutputFile)), StandardCharsets.UTF_8));

        String flag1 = src.readLine();
        String flag2 = dest.readLine();
        if(flag1.equals("*number") && flag1.equals(flag2))
            compareNumber(src, dest, file);
        else if(flag1.equals("*string") && flag1.equals(flag2))
            compareString(src, dest, file);
        else
            throw new RuntimeException(String.format("Primary key type is not same or unsupported: '%s' and '%s'", flag1, flag2));

        src.close();
        dest.close();
        file.close();

        printMsg(LogLevel.INFO, String.format("Total: %d rows", rows));
        this.setResultInfo("rows", rows);
        this.setResultInfo("missingRows", missingRows);
        this.setResultInfo("redundantRows", redundantRows);
    }

    private void compareNumber(BufferedReader src, BufferedReader dest, PrintWriter file) throws IOException {
        Lines lines1 = new Lines(src);
        Lines lines2 = new Lines(dest);

        while (true) {
            String s1 = lines1.fill();
            String s2 = lines2.fill();
            if(s1 == null && s2 == null)
                break;
            else if(s1 == null) {
                redundantRows += 1;
                file.printf("%s,-%n", s2);
                lines2.skip();
            }
            else if(s2 == null) {
                missingRows += 1;
                file.printf("%s,+%n", s1);
                lines1.skip();
            }
            else {
                long v1 = Long.parseLong(s1);
                long v2 = Long.parseLong(s2);
                if(v1 == v2) {
                    lines1.skip();
                    lines2.skip();
                }
                else if(v1 < v2) {
                    missingRows += 1;
                    file.printf("%s,+%n", s1);
                    lines1.skip();
                }
                else {
                    redundantRows += 1;
                    file.printf("%s,-%n", s2);
                    lines2.skip();
                }
            }

            rows += 1;
            if((rows % argFeedback) == 0)
                printMsg(LogLevel.INFO, String.format("%d rows ...", rows));
        }
    }

    private void compareString(BufferedReader src, BufferedReader dest, PrintWriter file) throws IOException {
        Lines lines1 = new Lines(src);
        Lines lines2 = new Lines(dest);

        while (true) {
            String s1 = lines1.fill();
            String s2 = lines2.fill();
            if(s1 == null && s2 == null)
                break;
            else if(s1 == null) {
                redundantRows += 1;
                file.printf("%s,-%n", s2);
                lines2.skip();
            }
            else if(s2 == null) {
                missingRows += 1;
                file.printf("%s,+%n", s1);
                lines1.skip();
            }
            else {
                if(s1.equals(s2)) {
                    lines1.skip();
                    lines2.skip();
                }
                else if(s1.compareTo(s2) < 0) {
                    missingRows += 1;
                    file.printf("%s,+%n", s1);
                    lines1.skip();
                }
                else {
                    redundantRows += 1;
                    file.printf("%s,-%n", s2);
                    lines2.skip();
                }
            }

            rows += 1;
            if((rows % argFeedback) == 0)
                printMsg(LogLevel.INFO, String.format("%d rows ...", rows));
        }
    }
}
