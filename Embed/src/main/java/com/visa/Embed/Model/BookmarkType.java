package com.visa.Embed.Model;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum for bookmark type.
 */
public enum BookmarkType {
    PERSONAL("personal"),
    SHARED("shared");

    private final String value;

    BookmarkType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}