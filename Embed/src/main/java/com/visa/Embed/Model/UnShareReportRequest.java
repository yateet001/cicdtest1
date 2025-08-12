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
 * Class for un-share/revoke access of the report.
 */
@Getter
@Setter
@JsonInclude(Include.NON_NULL)
public class UnShareReportRequest {
    private List<SharedUser> sharedWith;

    @JsonProperty(required = true)
    private boolean notifyByEmail;
}