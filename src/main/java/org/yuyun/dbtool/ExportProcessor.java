package org.yuyun.dbtool;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Map;

public class ExportProcessor extends Processor{
    private String argTableName;
    private String argSQL;
    private String argFields;
    private String argWhere;
    private int argLimit;
    private int argFeedback;
    private String argOutputFile;

    @Override
    public String getActionName() {
        return "export";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    /**
     * export
     *     --table S    <可选>表名
     *     --sql S      <可选>完整的SQL语句
     *     --fields S   <可选>要导出的字段列表，如果未设置，则导出所有列
     *     --where S    <可选>导出数据的条件和排序，如果未设置，则导出所有行
     *     --limit N    <可选>限制导出的行数，如果未设置，则导出所有行
     *     --feedback N <可选>每多少行显示进度提示，默认为10000行
     *     --output S   目标数据文件路径
     */
    @Override
    public void parseArguments(Map<String, String> args) {
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
    public void process() throws Exception {
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

//        System.out.println(getTableDDL("rpt_researchreport"));
//        System.out.println(getTableDDL("TEMP_IDS"));
//        System.out.println(this.getTableDDL("scott.emp"));
    }

    private void exportData(String sql) throws SQLException, IOException {
        FieldType[] fieldTypes;
        String[] fieldTypeNames;

        //导出
        Statement stmt = this.getConnection().createStatement();
        printMsg(LogLevel.INFO, "Execute query ...");
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        fieldTypes = new FieldType[md.getColumnCount()];
        fieldTypeNames = new String[md.getColumnCount()];

        //删除文件
        new File(argOutputFile).delete();
        DataFile file = new DataFile(argOutputFile, "rw");

        //写标识
        file.writeInteger(MAGIC_CODE);

        //写文件格式版本
        file.writeShort(FILE_FORMAT);

        //写建表语句
        if(!argTableName.isEmpty()) {
            String ddl = getTableDDL(argTableName);
            if(ddl != null) {
                file.writeByte((byte) StartFlag.DDL.ordinal());
                file.writeString(FieldType.MediumString, ddl);
            }
        }

        //写列数量
        file.writeByte((byte) StartFlag.FieldInfo.ordinal());
        file.writeShort((short) fieldTypes.length);


        for(int i=0; i<fieldTypes.length; i++) {
            int prec = md.getPrecision(i + 1);
            int scale = md.getScale(i + 1);
//            System.out.println(md.getColumnName(i + 1) + "=>" + md.getColumnType(i + 1));
            switch (md.getColumnType(i + 1)) {
                case Types.NULL:
                    fieldTypes[i] = FieldType.Null;
                    fieldTypeNames[i] = "null";
                    break;
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    if(md.getColumnType(i + 1) == Types.INTEGER)
                        fieldTypeNames[i] = "int";
                    else if(md.getColumnType(i + 1) == Types.SMALLINT)
                        fieldTypeNames[i] = "smallint";
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
                            fieldTypeNames[i] = String.format("numeric(%d,%d)", prec, scale);
                        else if (md.getColumnType(i + 1) == Types.DECIMAL)
                            fieldTypeNames[i] = String.format("decimal(%d,%d)", prec, scale);
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
                case Types.NCHAR:
                case Types.NVARCHAR:
                    if (md.getColumnType(i + 1) == Types.CHAR)
                        fieldTypeNames[i] = String.format("char(%d)", prec);
                    else if (md.getColumnType(i + 1) == Types.VARCHAR)
                        fieldTypeNames[i] = String.format("varchar(%d)", prec);
                    else if (md.getColumnType(i + 1) == Types.NVARCHAR)
                        fieldTypeNames[i] = String.format("nvarchar(%d)", prec);

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
                case Types.ROWID:
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
                    throw new RuntimeException(String.format("Unsupport field '%s' data type: %d: %s", md.getColumnLabel(i + 1), md.getColumnType(i + 1), md.getColumnTypeName(i + 1)));
            }

            //写列类型
            file.writeByte((byte) fieldTypes[i].ordinal());
            //写列类型名称
            file.writeString(FieldType.SmallString, fieldTypeNames[i]);
            //写列名
            file.writeString(FieldType.SmallString, md.getColumnLabel(i + 1));
        }

        //准备写行数和数据量
        long rows_offset = file.getFilePointer();
        file.writeInteger(0); //总行数
        file.writeLong(0); //实际文件大小

        printMsg(LogLevel.INFO, "Start ...");

        int batch = 0;
        int rows = 0;
        long actual_bytes = 0;

        ByteArrayOutputStream bytesChunk = new ByteArrayOutputStream();
        DataFile chunk = new DataFile(new DataOutputStream(bytesChunk));

        while (rs.next()) {
            chunk.writeByte((byte) StartFlag.DataRow.ordinal());

            for(int i=0; i<fieldTypes.length; i++) {
                switch (fieldTypes[i]) {
                    case Null:
                        break;
                    case Integer:
                        int iVal = rs.getInt(i + 1);
                        if(rs.wasNull())
                            chunk.writeByte((byte) 1);
                        else {
                            chunk.writeByte((byte) 0);
                            chunk.writeInteger(iVal);
                        }
                        break;
                    case Long:
                        long lVal = rs.getLong(i + 1);
                        if(rs.wasNull())
                            chunk.writeByte((byte) 1);
                        else {
                            chunk.writeByte((byte) 0);
                            chunk.writeLong(lVal);
                        }
                        break;
                    case Double:
                        double fVal = rs.getDouble(i + 1);
                        if(rs.wasNull())
                            chunk.writeByte((byte) 1);
                        else {
                            chunk.writeByte((byte) 0);
                            chunk.writeDouble(fVal);
                        }
                        break;
                    case SmallString:
                    case MediumString:
                    case LongString:
                        chunk.writeString(fieldTypes[i], rs.getString(i + 1));
                        break;
                    case Date:
                        chunk.writeDate(rs.getTimestamp(i + 1));
                        break;
                    case DateTime:
                        chunk.writeDateTime(rs.getTimestamp(i + 1));
                        break;
                    case SmallBinary:
                    case MediumBinary:
                    case LongBinary:
                        chunk.writeBinary(fieldTypes[i], rs.getBlob(i + 1));
                        break;
                    default:
                        assert false;
                }

            }

            batch += 1;
            rows += 1;

            if(batch >= 1000 || bytesChunk.size() >= 4 * 1024 * 1024) {
                chunk.writeByte((byte) StartFlag.EOF.ordinal());
                actual_bytes += bytesChunk.size();
                file.writeByte((byte) StartFlag.DataRow.ordinal());
                file.writeCompressBinary(bytesChunk.toByteArray());

                batch = 0;
                bytesChunk.reset();
            }

            if((rows % argFeedback) == 0) {
                printMsg(LogLevel.INFO, String.format("%d rows ...", rows));
            }
            if(rows >= argLimit)
                break;
        }

        if(batch > 0) {
            chunk.writeByte((byte) StartFlag.EOF.ordinal());
            actual_bytes += bytesChunk.size();
            file.writeByte((byte) StartFlag.DataRow.ordinal());
            file.writeCompressBinary(bytesChunk.toByteArray());
        }

        file.writeByte((byte) StartFlag.EOF.ordinal());
        file.seek(rows_offset);
        file.writeInteger(rows);
        file.writeLong(actual_bytes);
        file.close();

        printMsg(LogLevel.INFO, String.format("Total: %d rows", rows));
        DecimalFormat df = new DecimalFormat("#,###");
        printMsg(LogLevel.INFO, String.format("Total original size: %s bytes", df.format(actual_bytes)));

        rs.close();
        stmt.close();
    }
}
