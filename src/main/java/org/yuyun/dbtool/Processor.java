package org.yuyun.dbtool;

import org.yuyun.dbtool.db.MySQLDB;
import org.yuyun.dbtool.db.OracleDB;
import org.yuyun.dbtool.db.PostgreSQLDB;

import java.io.*;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public abstract class Processor {
    protected static final int MAGIC_CODE = 0x89ABCDEF;
    protected static final short FILE_FORMAT = 0x2;

    private DBType dbType = DBType.None;
    private Connection conn = null; //数据库连接
    private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void process(Map<String, String> args) throws Exception {
        //初始化处理器
        Map<String, Processor> processors = findAllProcesser();
        Processor processor = processors.get(args.get("action"));
        if(processor == null)
            throw new RuntimeException(String.format("Unsupported processing instruction: %s", args.get("action")));

        //处理数据库连接命令行
        if(processor.isConnectionUsed())
            processor.parseConnectionArguments(args);

        //处理命令行
        processor.parseArguments(args);

        //处理
        try {
            processor.process();
        } finally {
            if(processor.conn != null) {
                processor.conn.close();
            }
        }

    }

    /**
     * 初始化处理器类
     * @return 处理器类列表
     */
    public static Map<String, Processor> findAllProcesser() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Map<String, Processor> map = new HashMap<>();
        String packageName = Main.class.getPackage().getName();
        InputStream stream = ClassLoader.getSystemClassLoader()
                .getResourceAsStream(packageName.replaceAll("[.]", "/"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        Set<String> classFileNames = reader.lines().filter(line -> line.endsWith(".class")).collect(Collectors.toSet());
        for(String classFileName : classFileNames) {
            String className = packageName + "." + classFileName.substring(0, classFileName.lastIndexOf('.'));
            Class<?> _class = Class.forName(className);
            if(!Modifier.isAbstract(_class.getModifiers()) && Processor.class.isAssignableFrom(_class)) {
                Processor processor = (Processor) _class.newInstance();
                map.put(processor.getActionName(), processor);
            }
        }
        return map;
    }

    /**
     * 获取参数值
     * @param args 原始参数
     * @param name 参数名
     * @param fallback 缺失默认值
     * @return 参数值
     */
    protected String checkOptionalArgumentString(Map<String, String> args, String name, String fallback) {
        String s = args.get(name);
        if(s == null)
            return fallback;
        s = s.trim();
        if(s.isEmpty())
            return fallback;
        else
            return s;
    }

    /**
     * 获取参数值
     * @param args 原始参数
     * @param name 参数名
     * @return 参数值，如果未设置，返回空字符串
     */
    protected String checkMandatoryArgumentString(Map<String, String> args, String name) {
        String s = checkOptionalArgumentString(args, name, null);
        if(s == null)
            throw new RuntimeException(String.format("Parameter \"%s\" is not set", name));
        else
            return s;
    }

    /**
     * 获取参数值
     * @param args 原始参数
     * @param name 参数名
     * @param fallback 缺失默认值
     * @return 参数值
     */
    protected int checkOptionalArgumentInt(Map<String, String> args, String name, int fallback) {
        String s = checkOptionalArgumentString(args, name, String.valueOf(fallback));
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new RuntimeException(String.format("The parameter %s is not a valid integer: %s", name, s), e);
        }
    }

    protected boolean checkOptionalArgumentBool(Map<String, String> args, String name, boolean fallback) {
        String s = checkOptionalArgumentString(args, name, fallback ? "yes" : "no");
        if(isStringIn(s, true, Arrays.asList("y", "yes", "on", "true", "1")))
            return true;
        else if(isStringIn(s, true, Arrays.asList("n", "no", "off", "false", "0")))
            return false;
        else
            throw new RuntimeException(String.format("The parameter %s is not a valid bool: %s", name, s));
    }

    /**
     * 解析数据库连接参数
     * <command>
     *     --type   数据库类型，DBType
     *     --jdbc   JDBC连接串
     *     --host   数据库地址
     *     --db     数据库名称或Oracle服务名
     *     --sid    Oracle数据库SID
     *     --user   数据库用户名
     *     --pass   数据库密码
     * @param args 原始参数
     */
    private void parseConnectionArguments(Map<String, String> args) throws ClassNotFoundException, SQLException {
        String dbTypeName = args.get("type");
        if(dbTypeName == null || dbTypeName.isEmpty())
            throw new RuntimeException("Database type not specified");

        dbType = DBType.fromName(dbTypeName);
        if(dbType == null)
            throw new RuntimeException("Unsupported database type");

        //生成JDBC
        String jdbc = checkOptionalArgumentString(args, "jdbc", "");
        if(jdbc.isEmpty()) {
            String host = checkMandatoryArgumentString(args, "host");
            if (dbType.equals(DBType.Oracle)) {
                String db = checkOptionalArgumentString(args, "db", "");
                String sid = checkOptionalArgumentString(args, "sid", "");
                if (!db.isEmpty())
                    jdbc = String.format("jdbc:oracle:thin:@//%s/%s", host, db);
                else if (!sid.isEmpty())
                    jdbc = String.format("jdbc:oracle:thin:@%s:%s", host, sid);
                else
                    throw new RuntimeException("Parameter \"db\" or \"sid\" must be specified");
                Class.forName("oracle.jdbc.OracleDriver");
            } else if (dbType.equals(DBType.MySQL)) {
                String db = checkMandatoryArgumentString(args, "db");
                jdbc = String.format("jdbc:mysql://%s/%s?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull",
                        args.get("host"),
                        args.get("db"));
                Class.forName("com.mysql.cj.jdbc.Driver");
            } else if (dbType.equals(DBType.PostgreSQL)) {
                String db = checkMandatoryArgumentString(args, "db");
                jdbc = String.format("jdbc:postgresql://%s/%s", args.get("host"), args.get("db"));
                Class.forName("org.postgresql.Driver");
            }
        }

        //获取用户名密码
        String user = null, pass = null;
        switch (dbType) {
            case Oracle:
            case MySQL:
            case PostgreSQL:
                user = checkMandatoryArgumentString(args, "user");
                pass = checkMandatoryArgumentString(args, "pass");
                break;
        }

        //建立连接
        this.conn = DriverManager.getConnection(jdbc, user, pass);
    }

    public static boolean isStringIn(String text, boolean caseinsensitive, List<String> list) {
        for(String s : list) {
            if(text == null && s == null)
                return true;
            else if(text == null || s == null)
                return false;

            if(caseinsensitive) {
                if(text.equalsIgnoreCase(s))
                    return true;
            }
            else {
                if(text.equals(s))
                    return true;
            }
        }
        return false;
    }

    /**
     * 获取数据表DDL
     * @return 数据表DDL
     */
    protected String getTableDDL(String tableName) throws SQLException {
        switch (dbType) {
            case Oracle:
                return new OracleDB().getTableDDL(conn, tableName);
            case MySQL:
                return new MySQLDB().getTableDDL(conn, tableName);
            case PostgreSQL:
                return new PostgreSQLDB().getTableDDL(conn, tableName);
        }
        return null;
    }

    public static void printMsg(LogLevel level, String msg) {
        PrintStream out = null;

        switch (level) {
            case DEBUG:
            case INFO:
                out = System.out;
                break;
            case WARN:
            case ERROR:
                out = System.err;
                break;
        }

        out.print(df.format(Calendar.getInstance().getTime()));
        out.printf(" [%s] ", level.name());
        out.println(msg);
    }

    public static void printMsg(Throwable e) {
        printMsg(LogLevel.ERROR, e.getMessage());
        e.printStackTrace(System.err);
    }

    protected Connection getConnection() {
        return this.conn;
    }

    protected DBType getDbType() {
        return this.dbType;
    }

    protected void processDataFile(String filename, DataFileProcessor fp, int feedback) throws IOException {
        int rows = 0, totalRows = 0, startRow = 0;
        long actualBytes = 0;
        String ddl = null;
        FieldType[] fieldTypes = null;
        String[] names = null, fieldTypeNames = null;
        byte flag;
//        HashMap<Integer, Integer> fieldsMap = new HashMap<>();
//        if(args.containsKey("start"))
//            start_row = Integer.parseInt(args.get("start").toString());

        DataFile in = new DataFile(filename, "r");
        in.checkFileHeader();

        //读取ddl
        flag = (byte) in.readByte();
        if(flag == StartFlag.DDL.ordinal()) {
            ddl = in.readString(FieldType.MediumString);
            flag = (byte) in.readByte();
        }

        //读取列信息
        fieldTypes = new FieldType[in.readShort()];
        names = new String[fieldTypes.length];
        fieldTypeNames = new String[fieldTypes.length];

        //读列类型和列名
        for(int i=0; i<fieldTypes.length; i++) {
            fieldTypes[i] = FieldType.fromInt(in.readByte());
            fieldTypeNames[i] = in.readString(FieldType.SmallString);
            names[i] = in.readString(FieldType.SmallString);
        }

        //读取总行数
        totalRows = in.readInteger();
        actualBytes = in.readLong();

        if(!fp.onSummary(ddl, fieldTypes.length, names, fieldTypes, fieldTypeNames, totalRows, actualBytes))
            return;

        startRow = fp.getStartRow();

        try {
            int totalProcRows = 0;
            Object[] rowData = new Object[fieldTypeNames.length];
            ByteArrayInputStream bytesChuck = new ByteArrayInputStream(new byte[StartFlag.EOF.ordinal()]);
            DataFile chunk = new DataFile(new DataInputStream(bytesChuck));

            while (true){
                //读取缓冲区
                flag = (byte) chunk.readByte();
                if (flag != StartFlag.DataRow.ordinal()) {
                    //缓冲区为空，从文件读取下一缓冲区
                    flag = (byte)in.readByte();
                    if(flag != StartFlag.DataRow.ordinal())
                        break;

                    bytesChuck = new ByteArrayInputStream(in.readCompressBinary());
                    chunk = new DataFile(new DataInputStream(bytesChuck));

                    //读取缓冲区
                    flag = (byte) chunk.readByte();
                }

                rows += 1;

//                if(rows >= startRow)
//                    batch += 1;

                for(int i=0; i<fieldTypes.length; i++) {
                    if (rows >= startRow)
                        rowData[i] = null;
                    switch (fieldTypes[i]) {
                        case Null:
                            break;
                        case Integer:
                            if (chunk.readByte() == 0) {
                                int iVal = chunk.readInteger();
                                if (rows >= startRow)
                                    rowData[i] = iVal;
                            }
                            break;
                        case Long:
                            if (chunk.readByte() == 0) {
                                long lVal = chunk.readLong();
                                if (rows >= startRow)
                                    rowData[i] = lVal;
                            }
                            break;
                        case Double:
                            if (chunk.readByte() == 0) {
                                double fVal = chunk.readDouble();
                                if (rows >= startRow)
                                    rowData[i] = fVal;
                            }
                            break;
                        case SmallString:
                        case MediumString:
                        case LongString:
                            String sVal = chunk.readString(fieldTypes[i]);
                            if (rows >= startRow) {
                                if(sVal != null) {
                                    int index = sVal.indexOf('\u0000');
                                    if (index != -1)
                                        sVal = sVal.substring(0, index);
                                }
                                rowData[i] = sVal;
                            }
                            break;
                        case Date:
                            java.util.Date dateVal = chunk.readDate();
                            if (rows >= startRow)
                                rowData[i] = dateVal;
                            break;
                        case DateTime:
                            java.util.Date dtVal = chunk.readDateTime();
                            if (rows >= startRow)
                                rowData[i] = dtVal;
                            break;
                        case SmallBinary:
                        case MediumBinary:
                        case LongBinary:
                            byte[] blob = chunk.readBinary(fieldTypes[i]);
                            if (rows >= startRow)
                                rowData[i] = blob;
                            break;
                    }
                }

                if (rows >= startRow) {
                    if ((rows % feedback) == 0) {
                        printMsg(LogLevel.INFO, String.format("%d rows, %.4g%% ...", rows, ((int)(rows * 10000.0 / totalRows)) / 100.0));
                    }

                    totalProcRows += 1;
                    if (!fp.onRow(rows, rowData)) {
                        fp.onRowEnd(totalProcRows);
                        return;
                    }
                }
            }

            printMsg(LogLevel.INFO, String.format("%d rows, %.4g%% ...", rows, ((int)(rows * 10000.0 / totalRows)) / 100.0));
            fp.onRowEnd(totalProcRows);
        }
        finally {
            in.close();
        }
    }

    /**
     * 获取动作名称
     * @return 动作名称
     */
    public abstract String getActionName();

    /**
     * 是否用到数据库连接
     * @return 是否用到
     */
    protected abstract boolean isConnectionUsed();

    /**
     * 解析命令参数
     * @param args 原始命令参数
     */
    protected abstract void parseArguments(Map<String, String> args);

    /**
     * 处理动作
     */
    protected abstract void process() throws Exception;
}
