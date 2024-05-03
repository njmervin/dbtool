package org.yuyun.dbtool;

public enum DBType {
    None(""),
    Oracle("Oracle"),
    MySQL("MySQL"),
    PostgreSQL("PostgreSQL");

    private final String name;

    DBType(String name) {
        this.name = name;
    }

    public static DBType fromName(String name) {
        for (DBType b : DBType.values()) {
            if (b.name.equalsIgnoreCase(name)) {
                return b;
            }
        }
        return null;
    }
}
