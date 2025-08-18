package com.visa.Embed.Model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Java representation of the C# BookmarkView model.
 */
@Getter
@Setter
@JsonInclude(Include.NON_NULL)
public class BookmarkView {
    /**
     * A public property of type string that has both a getter and a setter method,
     * which allows for getting and setting the value of the BookmarkState property.
     */
    private String BookmarkState;

    /**
     * A public property of type string that has both a getter and a setter method,
     * which allows for getting and setting the value of the DisplayName property.
     */
    private String DisplayName;

    /**
     * A public property of type BookmarkType that has both a getter and a setter
     * method,
     * which allows for getting and setting the value of the Type property.
     * Marked as required for JSON input.
     */
    @JsonProperty(required = true)
    private BookmarkType Type;
}
