package com.etingyu.dbtool;

import java.io.*;
import java.nio.ByteBuffer;
import java.sql.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Main {
    private static final int MAGIC_CODE = 0x89ABCDEF;
    private static final short FILE_FORMAT = 0x1;
    private static HashMap<String, Object> args = new HashMap<>();

    private static int _limit = Integer.MAX_VALUE;
    private static int _feedback = 10000;
    private static int _batch = 1000;
    private static int _debugrow = 0;

    enum FieldType {
        Integer,
        Long,
        Double,
        SmallString,
        MediumString,
        LongString,
        Date,
        DateTime,
        SmallBinary,
        MediumBinary,
        LongBinary,
        Null;

        public static FieldType fromInt(int v) {
            if(v == Integer.ordinal())
                return Integer;
            else if(v == Long.ordinal())
                return Long;
            else if(v == Double.ordinal())
                return Double;
            else if(v == SmallString.ordinal())
                return SmallString;
            else if(v == MediumString.ordinal())
                return MediumString;
            else if(v == LongString.ordinal())
                return LongString;
            else if(v == Date.ordinal())
                return Date;
            else if(v == DateTime.ordinal())
                return DateTime;
            else if(v == SmallBinary.ordinal())
                return SmallBinary;
            else if(v == MediumBinary.ordinal())
                return MediumBinary;
            else if(v == LongBinary.ordinal())
                return LongBinary;
            else if(v == Null.ordinal())
                return Null;
            else
                return null;
        }
    }

    enum StartFlag {
        FieldInfo,
        DataRow,
        EOF
    }

    /**
     * export
     *      --type oracle
     *      --tns (Description=...)
     *      --host 127.0.0.1:1521
     *      --db orcl
     *      --user test
     *      --pass test
     *      --table "table"
     *      --fields "field1,...,fieldN"
     *      --where "where clause"
     *      --limit N
     *      --feedback N
     *      --output destfile
     *
     * import
     *      --type oracle
     *      --host 127.0.0.1:1521
     *      --db orcl
     *      --user test
     *      --pass test
     *      --table "table"
     *      --fields "name->newname,name2->name2"
     *      --limit N
     *      --feedback N
     *      --batch N
     *      --debugrow N
     *      --input destfile
     *
     * exec
     *      --type oracle
     *      --host 127.0.0.1:1521
     *      --db orcl
     *      --user test
     *      --pass test
     *      --sql "update sql"
     */
    public static void main(String[] _args) throws Exception {
        for(int i=0; i<_args.length; i++) {
            if(i == 0) {
                args.put("action", _args[0]);
                continue;
            }

            if(_args[i].startsWith("--")) {
                if(!_args[i + 1].trim().isEmpty())
                    args.put(_args[i].substring(2), _args[i + 1].trim());
                i += 1;
            }
        }

        process();
    }

    private static void process() throws Exception {
        if(args.containsKey("limit"))
            _limit = Integer.valueOf(args.get("limit").toString());

        if(args.containsKey("feedback"))
            _feedback = Integer.valueOf(args.get("feedback").toString());

        String jdbc = null;
        if(args.get("type").toString().equalsIgnoreCase("oracle")) {
            Class.forName("oracle.jdbc.OracleDriver");
            if(args.containsKey("tns"))
                jdbc = String.format("jdbc:oracle:thin:@%s", args.get("tns"));
            else
                jdbc = String.format("jdbc:oracle:thin:@%s:%s", args.get("host"), args.get("db"));
//            System.out.println(String.format("Oracle: %s", jdbc));
        }
        else if(args.get("type").toString().equalsIgnoreCase("mysql")) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            jdbc = String.format("jdbc:mysql://%s/%s?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull", args.get("host"), args.get("db"));
//            System.out.println(String.format("MySQL: %s", jdbc));
        }
        else if(args.get("type").toString().equalsIgnoreCase("postgresql")) {
            Class.forName("org.postgresql.Driver");
            jdbc = String.format("jdbc:postgresql://%s/%s", args.get("host"), args.get("db"));
        }
        else
            throw new Exception(String.format("Unsupported db type: %s", args.get("type")));

        System.out.println(String.format("%s Connect to database ...", getTimestamp()));
        Connection conn = DriverManager.getConnection(jdbc, args.get("user").toString(), args.get("pass").toString());
        System.out.println(String.format("%s Connect success.", getTimestamp()));
        if(args.get("action").toString().equalsIgnoreCase("export"))
            export(conn);
        else if(args.get("action").toString().equalsIgnoreCase("import")) {
            if(args.containsKey("batch"))
                _batch = Integer.parseInt(args.get("batch").toString());

            if(args.containsKey("debugrow"))
                _debugrow = Integer.parseInt(args.get("debugrow").toString());

            _import(conn);
        }
        else if(args.get("action").toString().equalsIgnoreCase("exec"))
            exec(conn);
        conn.close();
    }

    private static void export(Connection conn) throws SQLException, IOException {
        int rows = 0;
        long rows_offset = 0;
        long actual_bytes = 0;
        FieldType[] fieldTypes = null;
        String[] fieldTypeNames = null;

        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        if(args.containsKey("fields"))
            sql.append(args.get("fields").toString());
        else
            sql.append("*");
        sql.append(" from ");
        sql.append(args.get("table").toString());
        if(args.containsKey("where")) {
            sql.append(" ");
            String s = args.get("where").toString();
            if(!s.startsWith("where") && !s.startsWith("WHERE"))
                sql.append("where ");
            sql.append(s);
        }
//        System.out.println(String.format("SQL: %s", sql.toString()));

        Statement stmt = conn.createStatement();
        System.out.println(String.format("%s Execute query ...", getTimestamp()));
        ResultSet rs = stmt.executeQuery(sql.toString());
        ResultSetMetaData md = rs.getMetaData();
        fieldTypes = new FieldType[md.getColumnCount()];
        fieldTypeNames = new String[md.getColumnCount()];

        String filename = args.get("output").toString();
        //删除文件
        new File(filename).delete();
        RandomAccessFile out = new RandomAccessFile(filename, "rw");

        //写标识
        writeInteger(out, MAGIC_CODE);

        //写文件格式版本
        writeShort(out, FILE_FORMAT);

        //写列数量
        writeByte(out, (byte) StartFlag.FieldInfo.ordinal());
        writeShort(out, (short) fieldTypes.length);


        for(int i=0; i<fieldTypes.length; i++) {
            int prec = md.getPrecision(i + 1);
            int scale = md.getScale(i + 1);

            switch (md.getColumnType(i + 1)) {
                case Types.NULL:
                    fieldTypes[i] = FieldType.Null;
                    fieldTypeNames[i] = "null";
                    break;
                case Types.INTEGER:
                case Types.TINYINT:
                    if(md.getColumnType(i + 1) == Types.INTEGER)
                        fieldTypeNames[i] = "int";
                    else if(md.getColumnType(i + 1) == Types.TINYINT)
                        fieldTypeNames[i] = "tinyint";
                    fieldTypes[i] = FieldType.Integer;
                    break;
                case Types.BIGINT:
                    fieldTypes[i] = FieldType.Long;
                    fieldTypeNames[i] = "bigint";
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    if(scale > 0) {
                        if (md.getColumnType(i + 1) == Types.NUMERIC)
                            fieldTypeNames[i] = String.format("numeric(%d.%d)", prec, scale);
                        else if (md.getColumnType(i + 1) == Types.DECIMAL)
                            fieldTypeNames[i] = String.format("decimal(%d.%d)", prec, scale);
                    }
                    else if(prec > 0) {
                        if (md.getColumnType(i + 1) == Types.NUMERIC)
                            fieldTypeNames[i] = String.format("numeric(%d)", prec);
                        else if (md.getColumnType(i + 1) == Types.DECIMAL)
                            fieldTypeNames[i] = String.format("decimal(%d)", prec);
                    }
                    else {
                        if (md.getColumnType(i + 1) == Types.NUMERIC)
                            fieldTypeNames[i] = "numeric";
                        else if (md.getColumnType(i + 1) == Types.DECIMAL)
                            fieldTypeNames[i] = "decimal";
                    }

                    if(scale == 0) {
                        if(prec <= 9)
                            fieldTypes[i] = FieldType.Integer;
                        else
                            fieldTypes[i] = FieldType.Long;
                    }
                    else {
                        fieldTypes[i] = FieldType.Double;
                    }
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                    if (md.getColumnType(i + 1) == Types.CHAR)
                        fieldTypeNames[i] = String.format("char(%d)", prec);
                    else if (md.getColumnType(i + 1) == Types.VARCHAR)
                        fieldTypeNames[i] = String.format("varchar(%d)", prec);

                    if(prec <= Byte.MAX_VALUE)
                        fieldTypes[i] = FieldType.SmallString;
                    else if(prec <= Short.MAX_VALUE)
                        fieldTypes[i] = FieldType.MediumString;
                    else
                        fieldTypes[i] = FieldType.LongString;
                    break;
                case Types.LONGVARCHAR:
                case Types.CLOB:
                case Types.NCLOB:
                    if (md.getColumnType(i + 1) == Types.LONGVARCHAR)
                        fieldTypeNames[i] = String.format("varchar(%d)", prec);
                    else if (md.getColumnType(i + 1) == Types.CLOB)
                        fieldTypeNames[i] = "clob";
                    else if (md.getColumnType(i + 1) == Types.NCLOB)
                        fieldTypeNames[i] = "nclob";

                    fieldTypes[i] = FieldType.LongString;
                    break;
                case Types.DATE:
                    fieldTypeNames[i] = "date";

                    fieldTypes[i] = FieldType.Date;
                    break;
                case Types.TIMESTAMP:
                    fieldTypeNames[i] = "datetime";

                    fieldTypes[i] = FieldType.DateTime;
                    break;
                case Types.VARBINARY:
                    fieldTypeNames[i] = String.format("varbinary(%d)", prec);

                    if(prec <= Byte.MAX_VALUE)
                        fieldTypes[i] = FieldType.SmallBinary;
                    else if(prec <= Short.MAX_VALUE)
                        fieldTypes[i] = FieldType.MediumBinary;
                    else
                        fieldTypes[i] = FieldType.LongBinary;
                    break;
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    fieldTypeNames[i] = String.format("varbinary(%d)", prec);

                    fieldTypes[i] = FieldType.LongBinary;
                    break;
                default:
                    System.out.println(String.format("Unsupport field '%s' data type: %d: %s", md.getColumnLabel(i + 1), md.getColumnType(i + 1), md.getColumnTypeName(i + 1)));
            }

            //写列类型
            writeByte(out, (byte) fieldTypes[i].ordinal());
            writeString(out, FieldType.SmallString, fieldTypeNames[i]);

            //写列名
            writeString(out, FieldType.SmallString, md.getColumnName(i + 1));
        }

        //准备写行数和数据量
        rows_offset = out.getFilePointer();
        writeInteger(out, 0);
        writeLong(out, 0);

        System.out.println(String.format("%s Start ...", getTimestamp()));

        int batch = 0;
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();

        while (rs.next()) {
            writeByte(chunk, (byte) StartFlag.DataRow.ordinal());

            for(int i=0; i<fieldTypes.length; i++) {
                switch (fieldTypes[i]) {
                    case Null:
                        break;
                    case Integer:
                        int iVal = rs.getInt(i + 1);
                        if(rs.wasNull())
                            chunk.write(1);
                        else {
                            chunk.write(0);
                            writeInteger(chunk, iVal);
                        }
                        break;
                    case Long:
                        long lVal = rs.getLong(i + 1);
                        if(rs.wasNull())
                            chunk.write(1);
                        else {
                            chunk.write(0);
                            writeLong(chunk, lVal);
                        }
                        break;
                    case Double:
                        double fVal = rs.getDouble(i + 1);
                        if(rs.wasNull())
                            chunk.write(1);
                        else {
                            chunk.write(0);
                            writeDouble(chunk, fVal);
                        }
                        break;
                    case SmallString:
                    case MediumString:
                    case LongString:
                        writeString(chunk, fieldTypes[i], rs.getString(i + 1));
                        break;
                    case Date:
                        writeDate(chunk, rs.getTimestamp(i + 1));
                        break;
                    case DateTime:
                        writeDateTime(chunk, rs.getTimestamp(i + 1));
                        break;
                    case SmallBinary:
                    case MediumBinary:
                    case LongBinary:
                        writeBinary(chunk, fieldTypes[i], rs.getBlob(i + 1));
                        break;
                    default:
                        assert false;
                }

            }

            batch += 1;
            rows += 1;

            if(batch >= 1000 || chunk.size() >= 4 * 1024 * 1024) {
                writeByte(chunk, (byte) StartFlag.EOF.ordinal());
                actual_bytes += chunk.size();
                byte[] data = compress(chunk.toByteArray());
                writeByte(out, (byte) StartFlag.DataRow.ordinal());
                writeInteger(out, data.length);
                out.write(data);

                batch = 0;
                chunk.reset();
            }

            if((rows % _feedback) == 0) {
                System.out.println(String.format("%s %d rows ...", getTimestamp(), rows));
            }
            if(rows >= _limit)
                break;
        }

        if(batch > 0) {
            writeByte(chunk, (byte) StartFlag.EOF.ordinal());
            actual_bytes += chunk.size();
            byte[] data = compress(chunk.toByteArray());
            writeByte(out, (byte) StartFlag.DataRow.ordinal());
            writeInteger(out, data.length);
            out.write(data);
        }

        writeByte(out, (byte) StartFlag.EOF.ordinal());
        out.seek(rows_offset);
        writeInteger(out, rows);
        writeLong(out, actual_bytes);
        out.close();

        System.out.println(String.format("%s Total: %d rows", getTimestamp(), rows));
        DecimalFormat df = new DecimalFormat("#,###");
        System.out.println(String.format("Total original size: %s bytes", df.format(actual_bytes)));

        rs.close();
        stmt.close();
    }

    private static void _import(Connection conn) throws SQLException, IOException {
        int rows = 0, total_rows = 0;
        long actual_bytes = 0;
        FieldType[] fieldTypes = null;
        String[] names = null, fieldTypeNames = null;
        byte flag;
        HashMap<Integer, Integer> fieldsMap = new HashMap<>();

        FileInputStream ins = new FileInputStream(args.get("input").toString());

        //读标识符
        if(readInteger(ins) != MAGIC_CODE) {
            System.err.println("Invalid data format");
            ins.close();
            return;
        }

        //写文件格式版本
        short format = (short) readShort(ins);
        if(format != FILE_FORMAT) {
            System.err.println(String.format("File format version '%d' is not support, expect '%d'.", format, FILE_FORMAT));
            ins.close();
            return;
        }

        //读列数量
        flag = (byte) readByte(ins);
        fieldTypes = new FieldType[readShort(ins)];
        names = new String[fieldTypes.length];
        fieldTypeNames = new String[fieldTypes.length];

        //读列类型和列名
        for(int i=0; i<fieldTypes.length; i++) {
            fieldTypes[i] = FieldType.fromInt(readByte(ins));
            fieldTypeNames[i] = readString(ins, FieldType.SmallString);
            names[i] = readString(ins, FieldType.SmallString);
        }

        //读取总行数
        total_rows = readInteger(ins);
        System.out.printf("Total rows: %d%n", total_rows);
        actual_bytes = readLong(ins);
        DecimalFormat df = new DecimalFormat("#,###");
        System.out.printf("Total original size: %s bytes%n", df.format(actual_bytes));

        StringBuilder sql = new StringBuilder();
        if(!args.containsKey("fields") || args.get("fields").toString().isEmpty()) {
            PreparedStatement ps = conn.prepareStatement(String.format("select * from %s where 1=0", args.get("table").toString()));
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            StringBuilder sbuf = new StringBuilder();
            for(int i=0; i<meta.getColumnCount(); i++) {
                if(i > 0)
                    sbuf.append(",");
                sbuf.append(meta.getColumnName(i + 1).toLowerCase());
            }
            args.put("fields", sbuf.toString());
            rs.close();
            ps.close();
        }

        if(true){
            HashMap<String, Integer> map = new HashMap<>();
            for(int i=0; i<names.length; i++)
                map.put(names[i].toLowerCase(), i);

            int field_count = 0;
            sql.append("insert into ");
            if(_limit > 0)
                sql.append(args.get("table").toString());
            sql.append("(");
            for(String s : args.get("fields").toString().split(",")) {
                String[] parts =  s.trim().split("->");
                parts[0] = parts[0].trim();
                if(parts.length  == 1 && parts[0].isEmpty())
                    continue;

                if(parts.length > 1)
                    parts[1] = parts[1].trim();

                if(map.containsKey(parts[0])) {
                    fieldsMap.put(map.get(parts[0].toLowerCase()), field_count);
                    if (field_count > 0)
                        sql.append(",");
                    if(parts.length == 1)
                        sql.append(parts[0]);
                    else
                        sql.append(parts[1]);
                    field_count += 1;
                }
                else {
                    System.err.printf("Warn: The data file not contain field \"%s\", ignored.%n", parts[0]);
                    ins.close();
                    return;
                }
            }

            sql.append(")values(");
            for(int i=0; i<field_count; i++) {
                if (i > 0)
                    sql.append(",");
                sql.append("?");
            }
            sql.append(")");
        }

        if(_limit <= 0) {
            ins.close();
            return;
        }

//        System.out.println(String.format("SQL: %s", sql.toString()));
        System.out.println(String.format("%s Start ...", getTimestamp()));

        PreparedStatement ps = conn.prepareStatement(sql.toString());
        conn.setAutoCommit(false);

        try {
            int batch = 0;
            ByteArrayInputStream in = new ByteArrayInputStream(new byte[StartFlag.EOF.ordinal()]);

            for (; ; ) {

                //读取缓冲区
                flag = (byte) readByte(in);
                if (flag != StartFlag.DataRow.ordinal()) {
                    //缓冲区为空，从文件读取下一缓冲区
                    flag = (byte)readByte(ins);
                    if(flag != StartFlag.DataRow.ordinal())
                        break;

                    int comp_data_len = readInteger(ins);
                    byte[] comp_data = new byte[comp_data_len];
                    ins.read(comp_data);
                    byte[] umcomp_data = uncompress(comp_data);
                    in = new ByteArrayInputStream(umcomp_data);

                    //读取缓冲区
                    flag = (byte) readByte(in);
                }

                rows += 1;
                batch += 1;

                int wasNull;
                for(int i=0; i<fieldTypes.length; i++) {
                    switch (fieldTypes[i]) {
                        case Null:
                            if (rows == _debugrow)
                                System.out.println(String.format("[%d]%s: null", i + 1, names[i]));
                            if (fieldsMap.containsKey(i))
                                ps.setNull(fieldsMap.get(i) + 1, Types.NULL);
                            break;
                        case Integer:
                            wasNull = readByte(in);
                            if (wasNull == 0) {
                                int iVal = readInteger(in);
                                if (rows == _debugrow)
                                    System.out.println(String.format("[%d]%s: %d", i + 1, names[i], iVal));
                                if (fieldsMap.containsKey(i))
                                    ps.setInt(fieldsMap.get(i) + 1, iVal);
                            } else {
                                if (rows == _debugrow)
                                    System.out.println(String.format("[%d]%s: null", i + 1, names[i]));
                                if (fieldsMap.containsKey(i))
                                    ps.setNull(fieldsMap.get(i) + 1, Types.INTEGER);
                            }
                            break;
                        case Long:
                            wasNull = readByte(in);
                            if (wasNull == 0) {
                                long lVal = readLong(in);
                                if (rows == _debugrow)
                                    System.out.println(String.format("[%d]%s: %d", i + 1, names[i], lVal));
                                if (fieldsMap.containsKey(i))
                                    ps.setLong(fieldsMap.get(i) + 1, lVal);
                            } else {
                                if (rows == _debugrow)
                                    System.out.println(String.format("[%d]%s: null", i + 1, names[i]));
                                if (fieldsMap.containsKey(i))
                                    ps.setNull(fieldsMap.get(i) + 1, Types.BIGINT);
                            }
                            break;
                        case Double:
                            wasNull = readByte(in);
                            if (wasNull == 0) {
                                double fVal = readDouble(in);
                                if (rows == _debugrow)
                                    System.out.println(String.format("[%d]%s: %f", i + 1, names[i], fVal));
                                if (fieldsMap.containsKey(i))
                                    ps.setDouble(fieldsMap.get(i) + 1, fVal);
                            } else {
                                if (rows == _debugrow)
                                    System.out.println(String.format("[%d]%s: null", i + 1, names[i]));
                                if (fieldsMap.containsKey(i))
                                    ps.setNull(fieldsMap.get(i) + 1, Types.DECIMAL);
                            }
                            break;
                        case SmallString:
                        case MediumString:
                        case LongString:
                            String sVal = readString(in, fieldTypes[i]);
                            if (rows == _debugrow)
                                System.out.println(String.format("[%d]%s: %s", i + 1, names[i], sVal));
                            if (fieldsMap.containsKey(i))
                                ps.setString(fieldsMap.get(i) + 1, sVal);
                            break;
                        case Date:
                            Date dateVal = readDate(in);
                            if (rows == _debugrow) {
                                if (dateVal != null)
                                    System.out.println(String.format("[%d]%s: %s", i + 1, names[i], new SimpleDateFormat("yyyy-MM-dd").format(dateVal)));
                                else
                                    System.out.println(String.format("[%d]%s: null", i + 1, names[i]));
                            }
                            if (fieldsMap.containsKey(i))
                                ps.setDate(fieldsMap.get(i) + 1, dateVal != null ? new java.sql.Date(dateVal.getTime()) : null);
                            break;
                        case DateTime:
                            Date dtVal = readDateTime(in);
                            if (rows == _debugrow) {
                                if (dtVal != null)
                                    System.out.println(String.format("[%d]%s: %s", i + 1, names[i], new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dtVal)));
                                else
                                    System.out.println(String.format("[%d]%s: null", i + 1, names[i]));
                            }
                            if (fieldsMap.containsKey(i))
                                ps.setTimestamp(fieldsMap.get(i) + 1, dtVal != null ? new Timestamp(dtVal.getTime()) : null);
                            break;
                        case SmallBinary:
                        case MediumBinary:
                        case LongBinary:
                            byte[] blob_data = readBinary(in, fieldTypes[i]);
                            if (rows == _debugrow) {
                                if (blob_data != null)
                                    System.out.println(String.format("[%d]%s: <%d bytes>", i + 1, names[i], blob_data.length));
                                else
                                    System.out.println(String.format("[%d]%s: null", i + 1, names[i]));
                            }
                            if (fieldsMap.containsKey(i)) {
                                if (blob_data == null)
                                    ps.setNull(fieldsMap.get(i) + 1, Types.VARBINARY);
                                else {
                                    Blob blob = conn.createBlob();
                                    blob.setBytes(1, blob_data);
                                    ps.setBlob(fieldsMap.get(i) + 1, blob);
                                }
                            }
                            break;
                    }
                }

                ps.addBatch();

                if(batch >= _batch) {
                    ps.executeBatch();
                    conn.commit();
                    batch = 0;
                }

                if ((rows % _feedback) == 0) {
                    System.out.println(String.format("%s %d rows, %.4g%% ...", getTimestamp(), rows, ((int)(rows * 10000.0 / total_rows)) / 100.0));
                }
                if (rows >= _limit)
                    break;
            }

            if(batch > 0) {
                ps.executeBatch();
                conn.commit();
            }
        }
        catch (Exception e) {
            System.err.println(String.format("Failed: row=%d", rows));
            throw e;
        }
        finally {
            conn.setAutoCommit(true);
            ps.close();
        }

        ins.close();

        System.out.println(String.format("%s Total: %d rows", getTimestamp(), rows));
    }

    private static void exec(Connection conn) throws SQLException {
        String sql = (String) args.get("sql");
        if(sql == null) {
            System.out.println(String.format("%s no sql.", getTimestamp()));
            return;
        }

        System.out.println(String.format("%s Start ...", getTimestamp()));
        System.out.println(String.format("%s SQL: %s", getTimestamp(), sql));
        Statement stmt = conn.createStatement();
        int rows = stmt.executeUpdate(sql);
        System.out.println(String.format("%s Affect %d rows.", getTimestamp(), rows));
    }

    private static void writeByte(OutputStream out, byte value) throws IOException {
        out.write(value);
    }

    private static void writeByte(RandomAccessFile out, byte value) throws IOException {
        out.write(value);
    }

    private static int readByte(InputStream in) throws IOException {
        return in.read();
    }

    private static void writeShort(OutputStream out, short value) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.asShortBuffer().put(value);
        out.write(bb.array());
    }

    private static void writeShort(RandomAccessFile out, short value) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.asShortBuffer().put(value);
        out.write(bb.array());
    }

    private static int readShort(InputStream in) throws IOException {
        byte[] b = new byte[2];
        in.read(b);
        return ByteBuffer.wrap(b).getShort();
    }

    private static void writeInteger(OutputStream out, int value) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.asIntBuffer().put(value);
        out.write(bb.array());
    }

    private static void writeInteger(RandomAccessFile out, int value) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.asIntBuffer().put(value);
        out.write(bb.array());
    }

    private static int readInteger(InputStream in) throws IOException {
        byte[] b = new byte[4];
        in.read(b);
        return ByteBuffer.wrap(b).getInt();
    }

    private static void writeLong(OutputStream out, long value) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.asLongBuffer().put(value);
        out.write(bb.array());
    }

    private static void writeLong(RandomAccessFile out, long value) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.asLongBuffer().put(value);
        out.write(bb.array());
    }

    private static long readLong(InputStream in) throws IOException {
        byte[] b = new byte[8];
        in.read(b);
        return ByteBuffer.wrap(b).getLong();
    }

    private static void writeDouble(OutputStream out, double value) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.asDoubleBuffer().put(value);
        out.write(bb.array());
    }

    private static double readDouble(InputStream in) throws IOException {
        byte[] b = new byte[8];
        in.read(b);
        return ByteBuffer.wrap(b).getDouble();
    }

    private static void writeString(OutputStream out, FieldType type, String value) throws IOException {
        int len = 0;
        byte[] data = null;
        if(value != null) {
            data = value.getBytes("utf-8");
            len = data.length;
        }

        switch (type) {
            case SmallString:
                writeByte(out, (byte) len);
                break;
            case MediumString:
                writeShort(out, (short) len);
                break;
            case LongString:
                writeInteger(out, len);
                break;
        }

        if(len > 0)
            out.write(data);
    }

    private static void writeString(RandomAccessFile out, FieldType type, String value) throws IOException {
        int len = 0;
        byte[] data = null;
        if(value != null) {
            data = value.getBytes("utf-8");
            len = data.length;
        }

        switch (type) {
            case SmallString:
                writeByte(out, (byte) len);
                break;
            case MediumString:
                writeShort(out, (short) len);
                break;
            case LongString:
                writeInteger(out, len);
                break;
        }

        if(len > 0)
            out.write(data);
    }

    private static String readString(InputStream in, FieldType type) throws IOException {
        int len = 0;
        byte[] data = null;

        switch (type) {
            case SmallString:
                len = readByte(in);
                break;
            case MediumString:
                len = readShort(in);
                break;
            case LongString:
                len = readInteger(in);
                break;
        }

        if(len == 0)
            return null;
        else {
            data = new byte[len];
            in.read(data);
            return new String(data, "utf-8");
        }
    }

    private static void writeDate(OutputStream out, Date value) throws IOException {
        long x = 0;
        if(value != null) {
            x = value.getTime();
            x /= 3600 * 1000;
        }
        writeInteger(out, (int) x);
    }

    private static Date readDate(InputStream in) throws IOException {
        long x = readInteger(in);
        if(x == 0)
            return null;

        x *= 3600 * 1000;
        return new Date(x);
    }

    private static void writeDateTime(OutputStream out, Date value) throws IOException {
        long x = 0;
        if(value != null)
            x = value.getTime();
        writeLong(out, x);
    }

    private static Date readDateTime(InputStream in) throws IOException {
        long x = readLong(in);
        if(x == 0)
            return null;
        else
            return new Date(x);
    }

    private static void writeBinary(OutputStream out, FieldType type, Blob blob) throws IOException, SQLException {
        if(blob == null) {
            switch (type) {
                case SmallBinary:
                    writeByte(out, (byte) 0);
                    break;
                case MediumBinary:
                    writeShort(out, (short) 0);
                    break;
                case LongBinary:
                    writeInteger(out, 0);
                    break;
            }
        }
        else {
            InputStream is = blob.getBinaryStream();
            int len = is.available();
            switch (type) {
                case SmallBinary:
                    writeByte(out, (byte) len);
                    break;
                case MediumBinary:
                    writeShort(out, (short) len);
                    break;
                case LongBinary:
                    writeInteger(out, len);
                    break;
            }

            if(len > 0) {
                byte[] bytes = new byte[4096];
                while(len > 0) {
                    int n = is.read(bytes);
                    if(n == -1)
                        break;
                    if(n > 0) {
                        len -= n;
                        out.write(bytes, 0, n);
                    }
                }
            }
        }
    }

    private static byte[] readBinary(InputStream in, FieldType type) throws IOException {
        int len = 0;
        switch (type) {
            case SmallBinary:
                len = readByte(in);
                break;
            case MediumBinary:
                len = readShort(in);
                break;
            case LongBinary:
                len = readInteger(in);
                break;
        }

        if(len == 0)
            return null;

        byte[] bytes = new byte[len];
        in.read(bytes);
        return bytes;
    }

    private static String getTimestamp() {
        return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
    }


    public static byte[] compress(byte[] bytes) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(bytes);
            gzip.close();
        } catch (IOException e) {
            throw e;
        }
        return out.toByteArray();
    }

    public static byte[] uncompress(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        try {
            GZIPInputStream ungzip = new GZIPInputStream(in);
            byte[] buffer = new byte[256];
            int n;
            while ((n = ungzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
        } catch (IOException e) {
            throw e;
        }

        return out.toByteArray();
    }
}

