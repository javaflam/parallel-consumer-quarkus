package org.acme.handler;

import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.jboss.logging.Logger;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ParallelConsumerOptionsBuilder;
import io.confluent.parallelconsumer.ParallelStreamProcessor;

public abstract class ParallelConsumerRecordHandler<K, V> {

    private static final Logger LOG = Logger.getLogger(ParallelConsumerRecordHandler.class);

    @ConfigProperty(name = "parallel.consumer.max.concurrency")
    protected int maxConcurrency;

    @ConfigProperty(name = "parallel.consumer.order")
    protected String ordering;

    @ConfigProperty(name = "parallel.consumer.commit.mode")
    protected String mode;

    Properties props = getConfigProps();
    private ParallelStreamProcessor<K, V> parallelConsumer;

    public ParallelConsumerRecordHandler() {
    }

    protected Consumer<K, V> getKafkaConsumer() {
        Random random = new Random();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "parallel-consumer-app-group-" + random.nextInt(1000));
        return new KafkaConsumer<>(props);
    }

    protected Producer<K, V> getKafkaProducer() {
        return new KafkaProducer<>(props);
    }

    protected ParallelConsumerOptionsBuilder<K, V> initOptionsBuilder() {

        ParallelConsumerOptions.ProcessingOrder processingOrder = ParallelConsumerOptions.ProcessingOrder.valueOf(ordering);

        ParallelConsumerOptions.CommitMode commitMode = ParallelConsumerOptions.CommitMode.valueOf(mode);

        Consumer<K, V> kafkaConsumer = getKafkaConsumer();
        return ParallelConsumerOptions.<K, V>builder()
            .ordering(processingOrder)
            .maxConcurrency(maxConcurrency)
            .consumer(kafkaConsumer)
            .commitMode(commitMode);
    }

    protected ParallelConsumerOptionsBuilder<K, V> initProducingOptionsBuilder() {

        ParallelConsumerOptions.ProcessingOrder processingOrder = ParallelConsumerOptions.ProcessingOrder.valueOf(ordering);

        ParallelConsumerOptions.CommitMode commitMode = ParallelConsumerOptions.CommitMode.valueOf(mode);

        Consumer<K, V> kafkaConsumer = getKafkaConsumer();
        Producer<K, V> kafkaProducer = getKafkaProducer();

        return ParallelConsumerOptions.<K, V>builder()
            .ordering(processingOrder)
            .maxConcurrency(maxConcurrency)
            .consumer(kafkaConsumer)
            .producer(kafkaProducer)
            .commitMode(commitMode);
    }

    final public void processRecords() {
        this.parallelConsumer = setupParallelConsumer();
        processRecordsImpl(this.parallelConsumer);
    }

    final public void close() {
        if (this.parallelConsumer != null) {
            LOG.info("Shutting down Parallel Consumer");
            this.parallelConsumer.close();
        }
    }

    final protected Properties getConfigProps() {
        Properties props = new Properties();
        Config config = ConfigProvider.getConfig();
        Iterator<ConfigSource> iter = config.getConfigSources().iterator();
        while (iter.hasNext()) {
            ConfigSource configSource = iter.next();
            if (configSource.getOrdinal() == 250) {
                props.putAll(configSource.getProperties());
                return props;
            }
        }
        return props;
    }

    protected abstract ParallelStreamProcessor<K, V> setupParallelConsumer();
    protected abstract void processRecordsImpl(final ParallelStreamProcessor<K, V> parallelConsumer);
    public abstract void whoami();
}