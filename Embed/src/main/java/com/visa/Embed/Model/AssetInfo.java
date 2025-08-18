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
 * Class for storing information about an asset.
 */
@Getter
@Setter
@JsonInclude(Include.NON_NULL)
public class AssetInfo {
    private String assetName;
    private UUID assetId;
    private UUID tenantId;
    private boolean canCreate;
    private boolean canEdit;
    private boolean canExport;
    private String[] rlsRole;
    private String assetType;
    private boolean isEffectiveIdentityRolesRequired;
    private String reportType;
    private UUID datasetId;
    private String bindedDatasetId;
    private String reportPages;
    private boolean isEffectiveIdentityRequired;
    private List<ParameterMapping> reportParameter;
}
