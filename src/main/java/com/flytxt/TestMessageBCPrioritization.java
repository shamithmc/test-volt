package com.flytxt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import com.flytxt.configuration.NeonVoltDBConfiguration;
import com.flytxt.voltdb.VoltDBConnection;

//import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableConfigurationProperties
@Configuration
@Import({ NeonVoltDBConfiguration.class })
@Profile("messaging")
public class TestMessageBCPrioritization implements CommandLineRunner {

    @Autowired
    @Qualifier("voltdb")
    VoltDBConnection voltdb;

    @Value("${partitionToUse:1}")
    Integer partitionToUse = 1;

    @Value("${bcCountPerPartition:10}")
    Integer bcCountPerPartition = 10;

    @Value("${subscriberCount:100}")
    Integer consumerCount = 100;

    @Value("${startMsisdn:919000000000}")
    Long consumerAddressStart = 919000000000l;

    List<Long> partitionIds = new ArrayList<Long>();

    Map<Long, List<Integer>> partitionToBcId = new HashMap<Long, List<Integer>>();

    public static void main(String[] args) {

        SpringApplication.run(TestMessageBCPrioritization.class, args).close();

    }

    public void run(String... args) throws Exception {
        ProcedureCallback callback = new ProcedureCallback() {

            public void clientCallback(ClientResponse response) {
                if (response.getStatus() != ClientResponse.SUCCESS) {
                    log.error("Error from voltdb = {}", response.getStatusString());
                }
            }
        };
        
        log.info("Getting partition keys");

        VoltTable partitionIdTable = voltdb.executeProcedure("@GetPartitionKeys", "integer").getResults()[0];
        for (int i = 0; i < partitionToUse; i++) {
            if(! partitionIdTable.advanceRow())
            {
                partitionIdTable.resetRowPosition();
            }
            if (partitionIdTable.getLong(1) == 0) {
                partitionIdTable.advanceRow();
            }
            partitionIds.add(partitionIdTable.getLong(1));
        }
        log.info("Allocated partition keys");
        
        for (Long partitionId : partitionIds) {
            int i = 1;
            List<Integer> bcIds = new ArrayList<Integer>();
            log.info("Inserting frequency {}", partitionId);
            voltdb.executeProcedure("FREQUENCY.upsert", partitionId, partitionId, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
            for (int bcId = (1000 * i); bcId < (1000 * i) + bcCountPerPartition; bcId++) {
                voltdb.executeProcedure(callback, "InsertBcDetails", partitionId, bcId, partitionId, 0, 1, Integer.MAX_VALUE, 0);
                bcIds.add(bcId);
            }
            log.info("Inserted BCs for freq {}", partitionId);
            partitionToBcId.put(partitionId, bcIds);
        }
        
        Thread.sleep(1000);
        log.info("Starting prioritization job");

        long startTime = System.currentTimeMillis();
        for (long msisdn = consumerAddressStart; msisdn < consumerAddressStart + consumerCount; msisdn++) {
            for (Long partitionId : partitionIds) {
                for (Integer bcId : partitionToBcId.get(partitionId)) {
                    voltdb.executeProcedure(callback, "DAILY_BROADCAST.insert", msisdn, bcId, partitionId, 0, 0, 0, 0, 0, "", partitionId);
                }

            }
            for (Long partitionId : partitionIds) {
                voltdb.executeProcedure(callback, "Prioritize", partitionId, msisdn);
            }

        }

        voltdb.drain();
        long endTime = System.currentTimeMillis();
        log.info("========================================================================================================");
        log.info("Partitions:{} | BCsPerPartition:{} |Unique SubscriberCount:{} |Total Time taken(in millsec):{}", partitionToUse, bcCountPerPartition, consumerCount, (endTime - startTime));
        log.info("========================================================================================================");

    }

}
