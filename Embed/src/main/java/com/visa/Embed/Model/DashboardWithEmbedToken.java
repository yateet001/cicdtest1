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
 * Class for storing information about a dashboard along with an embed token.
 */
@Getter
@Setter
@JsonInclude(Include.NON_NULL)
public class DashboardWithEmbedToken {
    private UUID id;
    private String displayName;
    private String webUrl;
    private String embedUrl;
    private String embedToken;
}