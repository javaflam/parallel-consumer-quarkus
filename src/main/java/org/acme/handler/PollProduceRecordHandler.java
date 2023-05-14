package org.acme.handler;

import static io.confluent.parallelconsumer.ParallelStreamProcessor.createEosStreamProcessor;

import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;

import org.acme.handler.annotation.PollProduce;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.confluent.parallelconsumer.ParallelConsumerOptions.ParallelConsumerOptionsBuilder;
import io.confluent.parallelconsumer.ParallelStreamProcessor;

@ApplicationScoped
@PollProduce
public class PollProduceRecordHandler extends ParallelConsumerRecordHandler<String, String> {

    private static final Logger LOG = Logger.getLogger(PollProduceRecordHandler.class);
    
    @ConfigProperty(name = "input.topic.name")
    protected String inputTopic;

    @ConfigProperty(name = "output.topic.name")
    protected String outputTopic;
    
    public PollProduceRecordHandler() {
    }

    @Override
    protected ParallelStreamProcessor<String, String> setupParallelConsumer() {
        ParallelConsumerOptionsBuilder<String, String> builder = initProducingOptionsBuilder();
        return createEosStreamProcessor(builder.build());
    }

    @Override
    protected void processRecordsImpl(ParallelStreamProcessor<String, String> parallelConsumer) {
        parallelConsumer.subscribe(Collections.singletonList(inputTopic));
        parallelConsumer.pollAndProduce(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            String payload = processBrokerRecord(consumerRecord);
            return new ProducerRecord<>(outputTopic, consumerRecord.key(), payload);
        }, produceResult -> {
            LOG.debugf("Message %s saved to broker at offset %s", produceResult.getOut(), produceResult.getMeta().offset());
        });
    }

    private String processBrokerRecord(ConsumerRecord<String, String> consumerRecord) {
        String payload = consumerRecord.value();
        LOG.infof("Processing payload %s", payload);
        return payload;
    }
    
    @Override
    public void whoami() {
        LOG.infof("%s", "PollProduce");
    }
}
