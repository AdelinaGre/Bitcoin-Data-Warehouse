package com.datawarehouse.datawarehouse;

import com.datawarehouse.datawarehouse.ingestion.config.MarketDataApiProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(MarketDataApiProperties.class)
public class DatawarehouseApplication {

	public static void main(String[] args) {
		SpringApplication.run(DatawarehouseApplication.class, args);
	}

}
