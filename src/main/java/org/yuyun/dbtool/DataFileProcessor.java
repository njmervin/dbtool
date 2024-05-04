package org.yuyun.dbtool;

public interface DataFileProcessor {
    boolean onSummary(String ddl,
                   int fields,
                   String[] names,
                   FieldType[] fieldTypes,
                   String[] fieldTypeNames,
                   int totalRows,
                   long actualBytes);
    boolean onRow(int row, Object[] fields);
    void onRowEnd(int rows);
    int getStartRow();
}
