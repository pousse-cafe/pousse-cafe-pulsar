package poussecafe.pulsar;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poussecafe.exception.PousseCafeException;
import poussecafe.jackson.JacksonMessageAdapter;
import poussecafe.messaging.Message;
import poussecafe.messaging.MessageSender;

public abstract class PulsarMessageSender extends MessageSender {

    public static interface Builder {

        public Builder configuration(PulsarMessagingConfiguration configuration);

        public Builder client(PulsarClient client);

        public PulsarMessageSender build();
    }

    protected PulsarMessageSender() {
        super(new JacksonMessageAdapter());
    }

    protected PulsarMessagingConfiguration configuration;

    protected PulsarClient client;

    protected Producer<String> defaultTopicProducer;

    protected ProducerBuilder<String> createProducerBuilder(String topic) {
        return client.newProducer(Schema.STRING)
                .topic(topic);
    }

    protected Producer<String> createProducer(String topic) {
        try {
            return createProducerBuilder(topic).create();
        } catch (PulsarClientException e) {
            throw new PousseCafeException("Unable to send message to Pulsar broker", e);
        }
    }

    protected Producer<String> producer(Message message) {
        PublicationTopicChooser chooser = configuration.publicationTopicChooser();
        Optional<String> topic = chooser.chooseTopicForMessage(message);
        if(topic.isPresent()) {
            return getOrCreateProducer(topic.get());
        } else {
            return defaultTopicProducer;
        }
    }

    private Producer<String> getOrCreateProducer(String topic) {
        Producer<String> producer = producers.get(topic);
        if(producer == null) {
            producer = createProducer(topic);
            producers.put(topic, producer);
        }
        return producer;
    }

    private Map<String, Producer<String>> producers = new HashMap<>();

    public void close() {
        producers.values().forEach(this::closeAndCatch);
    }

    private void closeAndCatch(Producer<String> producer) {
        try {
            producer.close();
        } catch (PulsarClientException e) {
            logger.error("Unable to close producer", e);
        }
    }

    private Logger logger = LoggerFactory.getLogger(getClass());
}
