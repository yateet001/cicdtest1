package com.visa.Embed.Model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ReportTypeEnum {
    ALL("all"),
    REPORT("report"),
    PAGINATED("paginated");

    private final String value;

    ReportTypeEnum(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
