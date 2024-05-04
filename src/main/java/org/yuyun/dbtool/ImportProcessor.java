package org.yuyun.dbtool;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImportProcessor extends Processor implements DataFileProcessor{
    private String argTableName;
    private int argLimit;
    private int argFeedback;
    private int argBatch;
    private String argInputFile;
    private int argStart;
    private String argUpset;
    private Map<String, String> argFieldMap;

    private Map<Integer, Integer> fieldPosMap;
    private PreparedStatement ps;
    private int batch;
    private FieldType[] fieldTypes;

    @Override
    public String getActionName() {
        return "import";
    }

    @Override
    protected boolean isConnectionUsed() {
        return true;
    }

    /**
     * import
     *     --table S    表名
     *     --fields S   <可选>字段映射，eg."name->newname,name3"。如果未设置，则按照原始列名导入所有原始列；
     *                      如果设置"name"，则导入指定的原始列；
     *                      如果设置"name->newname"，则按照新名称newname导入原始列name
     *     --limit N    <可选> 限制导入的行数，如果未设置，则导入所有行
     *     --feedback N <可选> 每多少行显示进度提示，默认为 10000
     *     --batch N    <可选> 批量提交的行数，默认为 10000
     *     --input S    要导入的数据文件路径
     *     --start N    <可选> 从第N行开始导入，默认为1
     *     --upset S    <可选> 根据唯一约束进行更新，需设置约束字段
     */
    @Override
    protected void parseArguments(Map<String, String> args) {
        this.argTableName = checkMandatoryArgumentString(args, "table");
        this.argLimit = checkOptionalArgumentInt(args, "limit", Integer.MAX_VALUE);
        this.argFeedback = checkOptionalArgumentInt(args, "feedback", 10000);
        this.argBatch = checkOptionalArgumentInt(args, "batch", 10000);
        this.argInputFile = checkMandatoryArgumentString(args, "input");
        this.argStart = checkOptionalArgumentInt(args, "start", 1);
        this.argUpset = checkOptionalArgumentString(args, "upset", null);
        this.argFieldMap = new HashMap<>();
        String as = checkOptionalArgumentString(args, "fields", null);
        if(as != null && !as.isEmpty()) {
            for(String s1 : as.split(",")) {
                s1 = s1.trim();
                if(s1.isEmpty())
                    throw new RuntimeException(String.format("Bad field argument: %s", as));

                String[] ss = s1.split("->");
                if(ss.length == 1)
                    this.argFieldMap.put(ss[0].toLowerCase(), ss[0]);
                else if(ss.length == 2) {
                    String k = ss[0].trim();
                    String v = ss[1].trim();
                    if(!k.isEmpty() && !v.isEmpty())
                        this.argFieldMap.put(k.toLowerCase(), v);
                    else
                        throw new RuntimeException(String.format("Bad field argument: %s", as));
                }
                else
                    throw new RuntimeException(String.format("Bad field argument: %s", as));
            }
        }
    }

    @Override
    protected void process() throws Exception {
        processDataFile(this.argInputFile, this, argFeedback);
    }

    private void checkTargetFields(String[] names) throws SQLException {
        List<String> targetFieldNames = new ArrayList<>();
        this.fieldPosMap = new HashMap<>();
        Map<String, String> targetFieldsMap = new HashMap<>();
        Map<String, Integer> sourceFieldsMap = new HashMap<>();
        List<String> targetFieldsList = new ArrayList<>();

        //源表字段
        for(int i=0; i<names.length; i++)
            sourceFieldsMap.put(names[i].toLowerCase(), i);

        //目标表字段
        Statement stmt = this.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(String.format("select * from %s where 1 = 0", argTableName));
        ResultSetMetaData meta = rs.getMetaData();
        for(int i=0; i<meta.getColumnCount(); i++) {
            String s = meta.getColumnName(i + 1);
            targetFieldsMap.put(s.toLowerCase(), s);
            targetFieldsList.add(s.toLowerCase());
        }
        rs.close();
        stmt.close();

        if(this.argFieldMap.isEmpty()) {
            for(String name : targetFieldsList) {
                if(sourceFieldsMap.containsKey(name)) {
                    fieldPosMap.put(sourceFieldsMap.get(name), targetFieldNames.size());
                    targetFieldNames.add(targetFieldsMap.get(name));
                }
            }
        }
        else {
            //处理*
            for(Map.Entry<String, String> entry : argFieldMap.entrySet()) {
                if(entry.getKey().equals("*")) {
                    for (String s : targetFieldsList) {
                        if (sourceFieldsMap.containsKey(s)) {
                            fieldPosMap.put(sourceFieldsMap.get(s), targetFieldNames.size());
                            targetFieldNames.add(targetFieldsMap.get(s));
                        }
                    }
                    break;
                }
            }

            for(String name : targetFieldsList) {
                //处理其他
                for(Map.Entry<String, String> entry : argFieldMap.entrySet()) {
                    if(entry.getValue().equalsIgnoreCase(name)) {
                        fieldPosMap.put(sourceFieldsMap.get(entry.getKey()), targetFieldNames.size());
                        targetFieldNames.add(targetFieldsMap.get(name));
                        break;
                    }
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(argTableName).append("(");
        for(int i=0; i<targetFieldNames.size(); i++) {
            if(i > 0)
                sb.append(",");

            if(this.getDbType().equals(DBType.MySQL))
                sb.append("`").append(targetFieldNames.get(i)).append("`");
            else
                sb.append(targetFieldNames.get(i));
        }
        sb.append(")\nVALUES(");
        for(int i=0; i<targetFieldNames.size(); i++) {
            if (i > 0)
                sb.append(",");
            sb.append("?");
        }
        sb.append(")");
        if(argUpset != null && !argUpset.isEmpty()) {
            if(this.getDbType().equals(DBType.PostgreSQL)) {
                sb.append("\nON CONFLICT(").append(argUpset).append(") DO UPDATE SET");
                for(int i=0; i<targetFieldNames.size(); i++) {
                    if(i > 0)
                        sb.append(",");
                    sb.append(String.format("%s=EXCLUDED.%s", targetFieldNames.get(i), targetFieldNames.get(i)));
                }
            }
            else
                throw new RuntimeException(String.format("Database %s not support upset.", this.getDbType().name()));
        }

        //准备更新
        String sql = sb.toString();
        printMsg(LogLevel.INFO, String.format("SQL: %s", sql));
        this.ps = this.getConnection().prepareStatement(sql);
        this.getConnection().setAutoCommit(false);
        this.batch = 0;
        printMsg(LogLevel.INFO, "Start ...");
    }

    @Override
    public boolean onSummary(String ddl, int fields, String[] names, FieldType[] fieldTypes, String[] fieldTypeNames, int totalRows, long actualBytes) {
        try {
            this.fieldTypes = fieldTypes;
            checkTargetFields(names);
        } catch (SQLException e) {
            printMsg(e);
            return false;
        }

        return true;
    }

    @Override
    public boolean onRow(int row, Object[] fields) {
        batch += 1;

        try {
            for(int i=0; i<this.fieldTypes.length; i++) {
                if(!fieldPosMap.containsKey(i))
                    continue;
                switch (fieldTypes[i]) {
                    case Integer:
                        if(fields[i] == null)
                            ps.setNull(fieldPosMap.get(i) + 1, Types.INTEGER);
                        else
                            ps.setInt(fieldPosMap.get(i) + 1, ((Number)fields[i]).intValue());
                        break;
                    case Long:
                        if(fields[i] == null)
                            ps.setNull(fieldPosMap.get(i) + 1, Types.BIGINT);
                        else
                            ps.setLong(fieldPosMap.get(i) + 1, ((Number)fields[i]).longValue());
                        break;
                    case Double:
                        if(fields[i] == null)
                            ps.setNull(fieldPosMap.get(i) + 1, Types.DECIMAL);
                        else
                            ps.setDouble(fieldPosMap.get(i) + 1, ((Number)fields[i]).doubleValue());
                        break;
                    case SmallString:
                    case MediumString:
                    case LongString:
                        ps.setString(fieldPosMap.get(i) + 1, (String)fields[i]);
                        break;
                    case Date:
                        ps.setDate(fieldPosMap.get(i) + 1,
                                fields[i] != null ? new java.sql.Date(((java.util.Date)fields[i]).getTime()) : null);
                        break;
                    case DateTime:
                        ps.setTimestamp(fieldPosMap.get(i) + 1,
                                fields[i] != null ? new java.sql.Timestamp(((java.util.Date)fields[i]).getTime()) : null);
                        break;
                    case SmallBinary:
                    case MediumBinary:
                    case LongBinary:
                        if(fields[i] == null)
                            ps.setNull(fieldPosMap.get(i) + 1, Types.VARBINARY);
                        else {
                            Blob blob = this.getConnection().createBlob();
                            blob.setBytes(1, (byte[])fields[i]);
                            ps.setBlob(fieldPosMap.get(i) + 1, blob);
                        }
                        break;
                    case Null:
                        ps.setNull(fieldPosMap.get(i) + 1, Types.NULL);
                        break;
                }
            }

            ps.addBatch();
            if(batch >= argBatch) {
                ps.executeBatch();
                this.getConnection().commit();
                batch = 0;
            }
        } catch (SQLException e) {
            printMsg(LogLevel.ERROR, String.format("Import failed at row #%d", row));
            printMsg(e);
            throw new RuntimeException(e);
        }

        return row < this.argStart + this.argLimit - 1;
    }

    @Override
    public void onRowEnd(int rows) {
        try {
            if(batch > 0) {
                ps.executeBatch();
                getConnection().commit();
            }
            getConnection().setAutoCommit(true);
            printMsg(LogLevel.INFO, String.format("Total: %d rows", rows));
        } catch (SQLException e) {
            printMsg(LogLevel.ERROR, "Import failed after last row");
            printMsg(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getStartRow() {
        return this.argStart;
    }
}
