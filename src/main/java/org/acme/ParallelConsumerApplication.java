package org.acme;

import javax.inject.Inject;

import org.acme.handler.ParallelConsumerRecordHandler;
import org.acme.handler.annotation.Poll;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;

public class ParallelConsumerApplication implements QuarkusApplication {
    
    @Inject
    @Poll
    ParallelConsumerRecordHandler<String, String> recordHandler;

    @Override
    public int run(String... args) throws Exception {
        
        recordHandler.whoami();
        recordHandler.processRecords();
        
        Quarkus.waitForExit();
        recordHandler.close();
        return 0;
    }

}
