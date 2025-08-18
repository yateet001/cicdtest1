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
 * Class for representing details about an asset.
 */
@Getter
@Setter
@JsonInclude(Include.NON_NULL)
public class AssetModel {
    private String tenantAssetId;
    private UUID tenantId;
    private UUID assetId;
    private String assetType;
    private Boolean isEffectiveIdentityRolesRequired;
    private String assetName;
    private String embedUrl;
    private String webUrl;
    private String datasetId;
    private String reportType;
    private String reportPages;
    private List<String> reportParameter;
    private Boolean isEffectiveIdentityRequired;
    private String rolesSupported;
    private Boolean isRefreshable;
    private UUID createdBy;
    private UUID modifiedBy;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime createdOn;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modifiedOn;
}