package poussecafe.pulsar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import poussecafe.exception.PousseCafeException;
import poussecafe.messaging.Message;
import poussecafe.runtime.OriginalAndMarshaledMessage;

public class SyncPulsarMessageSender extends PulsarMessageSender {

    @SuppressWarnings("squid:S2176")
    public static class Builder implements PulsarMessageSender.Builder {

        private SyncPulsarMessageSender sender = new SyncPulsarMessageSender();

        @Override
        public Builder configuration(PulsarMessagingConfiguration configuration) {
            sender.configuration = configuration;
            return this;
        }

        @Override
        public Builder client(PulsarClient client) {
            sender.client = client;
            return this;
        }

        @Override
        public SyncPulsarMessageSender build() {
            Objects.requireNonNull(sender.configuration);
            Objects.requireNonNull(sender.client);
            sender.defaultTopicProducer = sender.createProducer(sender.configuration.defaultPublicationTopic());
            return sender;
        }
    }

    private SyncPulsarMessageSender() {

    }

    @Override
    protected synchronized void sendMarshalledMessage(OriginalAndMarshaledMessage marshalledMessage) {
        Message originalMessage = marshalledMessage.original();
        String marshalled = (String) marshalledMessage.marshaled();
        try {
            producer(originalMessage).send(marshalled);
        } catch (PulsarClientException e) {
            throw new PousseCafeException("Unable to send message", e);
        }
    }

    @Override
    public void sendMarshalledMessages(List<OriginalAndMarshaledMessage> value) {
        Map<Producer<String>, List<OriginalAndMarshaledMessage>> messagesPerProducer = messagesPerProducer(value);
        for(Entry<Producer<String>, List<OriginalAndMarshaledMessage>> entry : messagesPerProducer.entrySet()) {
            sendBulk(entry);
        }
    }

    private Map<Producer<String>, List<OriginalAndMarshaledMessage>> messagesPerProducer(List<OriginalAndMarshaledMessage> value) {
        Map<Producer<String>, List<OriginalAndMarshaledMessage>> messagesPerProducer = new HashMap<>();
        for(OriginalAndMarshaledMessage message : value) {
            Producer<String> producer = producer(message.original());
            List<OriginalAndMarshaledMessage> producerMessages = messagesPerProducer.computeIfAbsent(producer, key -> new ArrayList<>());
            producerMessages.add(message);
        }
        return messagesPerProducer;
    }

    private void sendBulk(Entry<Producer<String>, List<OriginalAndMarshaledMessage>> entry) {
        Producer<String> producer = entry.getKey();
        for(OriginalAndMarshaledMessage message : entry.getValue()) {
            producer.sendAsync((String) message.marshaled());
        }
        try {
            producer.flush();
        } catch (PulsarClientException e) {
            throw new PousseCafeException("Unable to send messages to Pulsar broker", e);
        }
    }
}
