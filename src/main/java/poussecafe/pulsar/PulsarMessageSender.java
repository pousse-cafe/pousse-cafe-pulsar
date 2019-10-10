package poussecafe.pulsar;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import poussecafe.exception.PousseCafeException;
import poussecafe.jackson.JacksonMessageAdapter;
import poussecafe.messaging.Message;
import poussecafe.messaging.MessageSender;
import poussecafe.runtime.OriginalAndMarshaledMessage;

public class PulsarMessageSender extends MessageSender {

    public static class Builder {

        private PulsarMessageSender sender = new PulsarMessageSender();

        public Builder configuration(PulsarMessagingConfiguration configuration) {
            sender.configuration = configuration;
            return this;
        }

        public Builder client(PulsarClient client) {
            sender.client = client;
            return this;
        }

        public PulsarMessageSender build() {
            Objects.requireNonNull(sender.configuration);
            Objects.requireNonNull(sender.client);
            sender.defaultTopicProducer = sender.createProducer(sender.configuration.defaultPublicationTopic());
            return sender;
        }
    }

    private PulsarMessageSender() {
        super(new JacksonMessageAdapter());
    }

    private PulsarMessagingConfiguration configuration;

    private PulsarClient client;

    private Producer<String> defaultTopicProducer;

    private Producer<String> createProducer(String topic) {
        try {
            return client.newProducer(Schema.STRING)
                    .topic(topic)
                    .create();
        } catch (PulsarClientException e) {
            throw new PousseCafeException("Unable to connect to Pulsar broker", e);
        }
    }

    @Override
    protected synchronized void sendMarshalledMessage(OriginalAndMarshaledMessage marshalledMessage) {
        try {
            producer(marshalledMessage.original()).send((String) marshalledMessage.marshaled());
        } catch (PulsarClientException e) {
            throw new PousseCafeException("Unable to send message to Pulsar broker", e);
        }
    }

    private Producer<String> producer(Message message) {
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
}
