package org.yuyun.dbtool;

public enum FieldType {
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
