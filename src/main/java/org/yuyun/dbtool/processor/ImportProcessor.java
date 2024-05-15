package org.yuyun.dbtool.processor;

import org.yuyun.dbtool.DBType;
import org.yuyun.dbtool.DataFileProcessor;
import org.yuyun.dbtool.FieldType;
import org.yuyun.dbtool.LogLevel;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImportProcessor extends Processor implements DataFileProcessor {
    class FieldMapItem {
        String  targetFieldName;
        String  expression;
    }

    private String argTableName;
    private int argLimit;
    private int argFeedback;
    private int argBatch;
    private String argInputFile;
    private int argStart;
    private String argUpset;
    private Map<String, FieldMapItem> argFieldMap;

    private PreparedStatement ps;
    private int batch;
    private FieldType[] fieldTypes;
    private Map<Integer, Integer> bindPosMap;

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
     *                      如果设置"name:expression"，expression可以含有参数#{原始字段名}
     *                      如果设置"name->newname:expression"
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
            for(String s1 : as.split(";")) {
                s1 = s1.trim();
                if(s1.isEmpty())
                    throw new RuntimeException(String.format("Bad field argument: %s", as));

                String[] ss = s1.split("->",2);
                if(ss.length == 1) {
                    FieldMapItem item = new FieldMapItem();
                    String[] ss2 = ss[0].split(":", 2);
                    item.targetFieldName = ss2[0];
                    item.expression = ss2.length == 2 ? ss2[1].trim() : null;
                    if(item.expression != null && item.expression.isEmpty())
                        item.expression = null;
                    this.argFieldMap.put(ss2[0].toLowerCase(), item);
                }
                else if(ss.length == 2) {
                    String k = ss[0].trim();
                    String v = ss[1].trim();
                    if(!k.isEmpty() && !v.isEmpty()) {
                        FieldMapItem item = new FieldMapItem();
                        String[] ss2 = v.split(":", 2);
                        item.targetFieldName = ss2[0];
                        item.expression = ss2.length == 2 ? ss2[1].trim() : null;
                        if(item.expression != null && item.expression.isEmpty())
                            item.expression = null;
                        this.argFieldMap.put(k.toLowerCase(), item);
                    }
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

    private static List<String> parseExpression(String exp) {
        Pattern pattern = Pattern.compile("#\\{([a-zA-Z0-9_]+)}");
        List<String> result = new ArrayList<>();
        result.add(exp.replaceAll("#\\{([a-zA-Z0-9_]+)}", "?"));

        for(;;) {
            Matcher matcher = pattern.matcher(exp);
            if(!matcher.find())
                break;

            result.add(matcher.group(1).toLowerCase());

            exp = exp.substring(matcher.end());
        }

        return result;
    }

    private void checkTargetFields(String[] names) throws SQLException {
        List<String> targetFieldNames = new ArrayList<>(); // 最终导入表的字段名
        List<String> targetFieldExpressions = new ArrayList<>(); //
        Map<Integer, Integer> fieldPosMap = new HashMap<>(); // 数据文件字段位置序号 => 最终导入表的字段名序号（targetFieldNames中的位置）
        Map<String, String> targetFieldsMap = new HashMap<>(); // 目标数据表字段名（小写）=> 目标数据表字段名
        Map<String, Integer> sourceFieldsMap = new HashMap<>(); // 数据文件字段名（小写）=> 数据文件字段位置序号
        List<String> targetFieldsList = new ArrayList<>(); // 目标数据表字段名（小写）

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
                    targetFieldExpressions.add(null);
                }
            }
        }
        else {
            //处理*
            for(Map.Entry<String, FieldMapItem> entry : argFieldMap.entrySet()) {
                if(entry.getKey().equals("*")) {
                    for (String s : targetFieldsList) {
                        if (sourceFieldsMap.containsKey(s)) {
                            fieldPosMap.put(sourceFieldsMap.get(s), targetFieldNames.size());
                            targetFieldNames.add(targetFieldsMap.get(s));
                            targetFieldExpressions.add(null);
                        }
                    }
                    break;
                }
            }

            for(String name : targetFieldsList) {
                //处理其他
                for(Map.Entry<String, FieldMapItem> entry : argFieldMap.entrySet()) {
                    if(entry.getValue().targetFieldName.equalsIgnoreCase(name)) {
                        fieldPosMap.put(sourceFieldsMap.get(entry.getKey()), targetFieldNames.size());
                        targetFieldNames.add(targetFieldsMap.get(name));
                        targetFieldExpressions.add(entry.getValue().expression);
                        break;
                    }
                }
            }
        }

        this.bindPosMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();

        sb.append("INSERT INTO ").append(argTableName).append("(");
        int index = 0;
        for(Map.Entry<Integer, Integer> entry : fieldPosMap.entrySet()) {
            if(index > 0)
                sb.append(",");
            index += 1;
            if(this.getDbType().equals(DBType.MySQL))
                sb.append("`");
            sb.append(targetFieldNames.get(entry.getValue()));
            if(this.getDbType().equals(DBType.MySQL))
                sb.append("`");
        }
        sb.append(")\nVALUES(");

        index = 0;
        for(Map.Entry<Integer, Integer> entry : fieldPosMap.entrySet()) {
            String exp = targetFieldExpressions.get(entry.getValue());
            if(exp == null) {
                if(index > 0)
                    sb.append(",");
                index += 1;
                sb.append("?");
                this.bindPosMap.put(index, entry.getKey());
            }
            else {
                if(index > 0)
                    sb.append(",");
                List<String> list = parseExpression(exp);
                sb.append(list.get(0));
                for(int i=1; i<list.size(); i++) {
                    index += 1;
                    Integer pos = sourceFieldsMap.get(list.get(i));
                    if(pos == null)
                        throw new RuntimeException(String.format("Data file not contain field \"%s\"", list.get(i)));
                    this.bindPosMap.put(index, pos);
                }
            }
        }
        sb.append(")");
        if(argUpset != null && !argUpset.isEmpty()) {
            if(this.getDbType().equals(DBType.PostgreSQL)) {
                sb.append("\nON CONFLICT(").append(argUpset).append(") DO UPDATE SET ");
                index = 0;
                for(Map.Entry<Integer, Integer> entry : fieldPosMap.entrySet()) {
                    if(index > 0)
                        sb.append(",");
                    index += 1;
                    sb.append(String.format("%s=EXCLUDED.%s", targetFieldNames.get(entry.getValue()), targetFieldNames.get(entry.getValue())));
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
            for(Map.Entry<Integer, Integer> entry : this.bindPosMap.entrySet()) {
                int pos = entry.getKey();
                int i = entry.getValue();

                switch (fieldTypes[i]) {
                    case Integer:
                        if (fields[i] == null)
                            ps.setNull(pos, Types.INTEGER);
                        else
                            ps.setInt(pos, ((Number) fields[i]).intValue());
                        break;
                    case Long:
                        if (fields[i] == null)
                            ps.setNull(pos, Types.BIGINT);
                        else
                            ps.setLong(pos, ((Number) fields[i]).longValue());
                        break;
                    case Double:
                        if (fields[i] == null)
                            ps.setNull(pos, Types.DECIMAL);
                        else
                            ps.setDouble(pos, ((Number) fields[i]).doubleValue());
                        break;
                    case SmallString:
                    case MediumString:
                    case LongString:
                        ps.setString(pos, (String) fields[i]);
                        break;
                    case Date:
                        ps.setDate(pos, fields[i] != null ? new java.sql.Date(((java.util.Date) fields[i]).getTime()) : null);
                        break;
                    case DateTime:
                        ps.setTimestamp(pos, fields[i] != null ? new java.sql.Timestamp(((java.util.Date) fields[i]).getTime()) : null);
                        break;
                    case SmallBinary:
                    case MediumBinary:
                    case LongBinary:
                        if (fields[i] == null)
                            ps.setNull(pos, Types.VARBINARY);
                        else {
                            Blob blob = this.getConnection().createBlob();
                            blob.setBytes(1, (byte[]) fields[i]);
                            ps.setBlob(pos, blob);
                        }
                        break;
                    case Null:
                        ps.setNull(pos, Types.NULL);
                        break;
                }
            }

            ps.addBatch();
            if(batch >= argBatch) {
                ps.executeBatch();
                this.getConnection().commit();
                batch = 0;
            }

            if((row % argFeedback) == 0)
                printMsg(LogLevel.INFO, String.format("%d rows ...", row));
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

            this.setResultInfo("rows", rows);
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
