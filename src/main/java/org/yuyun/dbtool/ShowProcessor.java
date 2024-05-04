package org.yuyun.dbtool;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

public class ShowProcessor extends Processor implements DataFileProcessor{
    private static final String SECTION_TEXT = "================================================================================";
    private static final String EMPTY_TEXT = "                                                                                ";
    private String argInput;
    private int argRow;
    private int argFeedback;
    private String[] fieldNames;
    private FieldType[] fieldTypes;

    @Override
    public String getActionName() {
        return "show";
    }

    @Override
    protected boolean isConnectionUsed() {
        return false;
    }

    /**
     * <show>
     *     --input S    查看的数据文件名
     *     --row N      <可选>查看第N行数据
     *     --feedback N <可选>每多少行显示进度提示，默认为10000行
     */
    @Override
    protected void parseArguments(Map<String, String> args) {
        argInput = checkMandatoryArgumentString(args, "input");
        argRow = checkOptionalArgumentInt(args, "row", 0);
        argFeedback = checkOptionalArgumentInt(args, "feedback", 10000);
    }

    @Override
    protected void process() throws Exception {
        processDataFile(argInput, this, argFeedback);
    }

    public static void printSection(String msg) {
        int n = 80 - 2 - msg.length();
        if(n <= 0)
            System.out.println(msg);
        else {
            if(n / 2 > 0)
                System.out.print(SECTION_TEXT.substring(0, n / 2));
            System.out.print(" ");
            System.out.print(msg);
            System.out.print(" ");
            n -= n / 2;
            if(n > 0)
                System.out.print(SECTION_TEXT.substring(0, n));
        }
        System.out.println();
    }

    public static String paddingLeft(String msg, int length) {
        if(msg.length() > length)
            return msg;
        else
            return EMPTY_TEXT.substring(0, length - msg.length()) + msg;
    }

    @Override
    public boolean onSummary(String ddl, int fields, String[] names, FieldType[] fieldTypes, String[] fieldTypeNames, int totalRows, long actualBytes) {
        this.fieldNames = names;
        this.fieldTypes = fieldTypes;

        DecimalFormat df = new DecimalFormat("#,###");
        printSection("Summary");
        System.out.printf("        Total Rows: %s%n", df.format(totalRows));
        System.out.printf("Total Actual Bytes: %s%n", df.format(actualBytes));

        if(ddl != null) {
            printSection("DDL Start");
            System.out.println(ddl);
        }
        printSection("Fields");
        int len = String.format("Field #%d", fieldTypes.length + 1).length();
        for(int i=0; i<fieldTypes.length; i++) {
            System.out.print(paddingLeft(String.format("Field #%d", i + 1), len));
            System.out.printf(": %s %s%n", names[i], fieldTypeNames[i]);
        }

        if(argRow <= 0)
            return false;

        printSection(String.format("Row #%d", argRow));
        if(argRow > totalRows) {
            System.out.println("No Row");
            return false;
        }

        return true;
    }

    @Override
    public int getStartRow() {
        return argRow;
    }

    @Override
    public boolean onRow(int row, Object[] fields) {
        if(row == argRow) {
            for(int i=0; i<fieldNames.length; i++) {
                if(i > 0)
                    System.out.print(", ");
                System.out.print(fieldNames[i]);
                System.out.print("=");
                Object o = fields[i];
                if(o == null)
                    System.out.print("<null>");
                else {
                    switch (fieldTypes[i]) {
                        case Integer:
                        case Long:
                        case Double:
                            System.out.print(o);
                            break;
                        case SmallString:
                        case MediumString:
                        case LongString:
                            System.out.print('"');
                            System.out.print(o);
                            System.out.print('"');
                            break;
                        case Date:
                            System.out.print(new SimpleDateFormat("yyyy-MM-dd").format((java.util.Date)o));
                            break;
                        case DateTime:
                            System.out.print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((java.util.Date)o));
                            break;
                        case SmallBinary:
                        case MediumBinary:
                        case LongBinary:
                            System.out.printf("<byte[%d]>", ((byte[]) o).length);
                            break;
                        case Null:
                            System.out.print("<null>");
                            break;
                    }
                }
            }
        }
        return row < argRow;
    }

    @Override
    public void onRowEnd() {
    }
}
