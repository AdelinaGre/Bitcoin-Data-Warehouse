package com.datawarehouse.datawarehouse;

import org.springframework.boot.SpringApplication;

public class TestDatawarehouseApplication {

	public static void main(String[] args) {
		SpringApplication.from(DatawarehouseApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
