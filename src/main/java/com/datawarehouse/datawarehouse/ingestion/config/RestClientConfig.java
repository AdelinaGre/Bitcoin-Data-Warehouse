package com.datawarehouse.datawarehouse.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
//creates reusable http client bean
@Configuration
public class RestClientConfig {
    @Bean
    public RestClient restClient(){
        return RestClient.builder().build(); // se defineste manual un obiect si se transmite sa fie gestionat de Spring
    }
}
