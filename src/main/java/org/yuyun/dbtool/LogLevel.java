package org.yuyun.dbtool;

public enum LogLevel {
    DEBUG("DEBUG"),
    INFO("INFO"),
    WARN("WARN"),
    ERROR("ERROR");

    private String name;

    LogLevel(String name) {
        this.name = name;
    }
}
