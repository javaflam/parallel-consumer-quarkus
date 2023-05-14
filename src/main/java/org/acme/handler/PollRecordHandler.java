package org.acme.handler;

import static io.confluent.parallelconsumer.ParallelStreamProcessor.createEosStreamProcessor;

import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;

import org.acme.handler.annotation.Poll;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.confluent.parallelconsumer.ParallelConsumerOptions.ParallelConsumerOptionsBuilder;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.quarkus.arc.DefaultBean;

@ApplicationScoped
@DefaultBean
@Poll
public class PollRecordHandler extends ParallelConsumerRecordHandler<String, String> {

    private static final Logger LOG = Logger.getLogger(PollRecordHandler.class);
    
    @ConfigProperty(name = "input.topic.name")
    protected String inputTopic;
    
    public PollRecordHandler() {
    }

    @Override
    protected ParallelStreamProcessor<String, String> setupParallelConsumer() {
        ParallelConsumerOptionsBuilder<String, String> builder = initOptionsBuilder();
        return createEosStreamProcessor(builder.build());
    }

    @Override
    protected void processRecordsImpl(final ParallelStreamProcessor<String, String> parallelConsumer) {
        
        parallelConsumer.subscribe(Collections.singletonList(inputTopic));
        parallelConsumer.poll(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            processBrokerRecord(consumerRecord);
        });
    }

    private void processBrokerRecord(ConsumerRecord<String, String> consumerRecord) {
        LOG.infof("Processing payload %s", consumerRecord.value());
    }
    
    @Override
    public void whoami() {
        LOG.infof("%s", "Poll");
    }
}
