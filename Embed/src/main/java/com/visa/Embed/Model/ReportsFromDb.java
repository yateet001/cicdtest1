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
 * Class for storing details about assets fetched from database.
 */
@Getter
@Setter
@JsonInclude(Include.NON_NULL)
public class ReportsFromDb {
    private UUID tenantId;
    private UUID assetId;
    private String assetType;
    private String assetName;
    private String embedUrl;
    private String webUrl;
    private String datasetId;
    private String bindedDatasetId;
    private String reportType;
    private List<Object> reportPages; // ExpandoObject → Object in Java
    private String isEffectiveIdentityRolesRequired;
    private String isEffectiveIdentityRequired;
    private boolean isReportTemplate;
    private boolean canView;
    private boolean canCreate;
    private boolean canEdit;
    private boolean canExport;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime createdOn;
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime modifiedOn;
    private String createdBy;
    private String modifiedBy;
    private List<ParameterMapping> reportParameter;
}