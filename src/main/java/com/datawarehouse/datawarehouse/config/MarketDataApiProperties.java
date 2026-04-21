package com.datawarehouse.datawarehouse.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
//stores external api config in one place
@Data
@ConfigurationProperties(prefix = "marketdata.api")
public class MarketDataApiProperties {
    private String baseUrl;
    private String key;
    private String dataset;
}
