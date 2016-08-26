package com.flytxt;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import com.flytxt.commons.voltdb.VoltDbInteractor;
import com.flytxt.commons.voltdb.VoltDbInteractorImpl;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
@EnableConfigurationProperties
public class TestVoltdPrioritization implements CommandLineRunner {

	@Bean
	VoltDbInteractor voltDbInteractor() {
		return new VoltDbInteractorImpl();
	}

	public static void main(String[] args) {

		SpringApplication.run(TestVoltdPrioritization.class, args);

	}

	@Override
	public void run(String... args) throws Exception {
		ProcedureCallback callback = new ProcedureCallback() {

			@Override
			public void clientCallback(ClientResponse response) {
				if (response.getStatus() != ClientResponse.SUCCESS) {
					log.error("Error from voltdb = {}", response.getStatusString());
				}
			}
		};
		
		log.info("Starting prioritization job");

		VoltDbInteractor voltdb = voltDbInteractor();
		voltdb.executeProcedure("InsertBcDetails", 1, 100, 0, 1, Integer.MAX_VALUE);
		voltdb.executeProcedure("InsertBcDetails", 2, 100, 0, 2, Integer.MAX_VALUE);
		voltdb.executeProcedure("InsertBcDetails", 3, 100, 0, 3, Integer.MAX_VALUE);
		voltdb.executeProcedure("InsertBcDetails", 4, 100, 0, 4, Integer.MAX_VALUE);
		voltdb.executeProcedure("InsertBcDetails", 5, 100, 0, 5, Integer.MAX_VALUE);
		voltdb.executeProcedure("FREQUENCY.upsert", 500, 100, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
		for (int i = 0; i < 2000; i++) {
			for (int j = 1; j <= 5; j++) {
				voltdb.executeProcedure(callback, "DAILY_BROADCAST.upsert", i, j, 100, 0, 0, 0, 0, 0, "");
			}
			voltdb.executeProcedure(callback, "Prioritize", i);
		}
		
		log.info("Finished prioritization job");

	}

}
