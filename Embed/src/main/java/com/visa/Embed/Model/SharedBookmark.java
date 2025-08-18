package com.visa.Embed.Model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Class for storing information about shared bookmarks.
 */
@Getter
@Setter
@JsonInclude(Include.NON_NULL)
public class SharedBookmark {
    public UUID ReportBookmarkId;
    public UUID ReportId;
    public String ShareWithUser;
    public String ModifiedByUserId;
    public String SuccessStatus;
}