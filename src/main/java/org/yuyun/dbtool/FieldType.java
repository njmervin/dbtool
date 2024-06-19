package org.yuyun.dbtool;

public enum FieldType {
    Integer,
    Long,
    Double,
    String,
    Date,
    DateTime,
    Binary,
    Null;

    public static FieldType fromInt(int v) {
        if(v == Integer.ordinal())
            return Integer;
        else if(v == Long.ordinal())
            return Long;
        else if(v == Double.ordinal())
            return Double;
        else if(v == String.ordinal())
            return String;
        else if(v == Date.ordinal())
            return Date;
        else if(v == DateTime.ordinal())
            return DateTime;
        else if(v == Binary.ordinal())
            return Binary;
        else if(v == Null.ordinal())
            return Null;
        else
            return null;
    }
}
