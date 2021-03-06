package poussecafe.pulsar;

import java.io.IOException;
import java.util.Objects;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import poussecafe.exception.PousseCafeException;
import poussecafe.jackson.JacksonMessageAdapter;
import poussecafe.messaging.EnvelopeSource;
import poussecafe.messaging.MessageReceiverConfiguration;
import poussecafe.messaging.ReceptionThreadMessageReceiver;

public class PulsarMessageReceiver extends ReceptionThreadMessageReceiver<Message<String>> {

    public static class Builder {

        public Builder configuration(MessageReceiverConfiguration configuration) {
            this.configuration = configuration;
            return this;
        }

        private MessageReceiverConfiguration configuration;

        public Builder consumerFactory(ConsumerFactory consumerFactory) {
            this.consumerFactory = consumerFactory;
            return this;
        }

        private ConsumerFactory consumerFactory;

        public PulsarMessageReceiver build() {
            Objects.requireNonNull(configuration);
            PulsarMessageReceiver receiver = new PulsarMessageReceiver(configuration);
            receiver.consumerFactory = consumerFactory;
            return receiver;
        }
    }

    private PulsarMessageReceiver(MessageReceiverConfiguration configuration) {
        super(configuration);
    }

    private ConsumerFactory consumerFactory;

    @Override
    protected void actuallyStartReceiving() {
        consumer = consumerFactory.buildConsumer();
        super.actuallyStartReceiving();
    }

    private Consumer<String> consumer;

    @Override
    protected Object extractPayload(Message<String> envelope) {
        return envelope.getValue();
    }

    @Override
    protected poussecafe.messaging.Message deserialize(Object payload) {
        return messageAdapter.adaptSerializedMessage(payload);
    }

    @Override
    protected Runnable buildAcker(Message<String> envelope) {
        return ackRunnable(envelope);
    }

    private Runnable ackRunnable(Message<String> message) {
        return () -> ack(message);
    }

    private void ack(Message<String> message) {
        try {
            consumer.acknowledge(message);
        } catch (PulsarClientException e) {
            throw new PousseCafeException("Unable to ack message", e);
        }
    }

    private JacksonMessageAdapter messageAdapter = new JacksonMessageAdapter();

    @Override
    protected EnvelopeSource<Message<String>> envelopeSource() {
        return new EnvelopeSource<>() {
            @Override
            public Message<String> get() {
                try {
                    return consumer.receive();
                } catch (PulsarClientException e) {
                    throw new RuntimePulsarClientException(e);
                }
            }

            @Override
            public void close() throws IOException {
                closeIfConnected();
            }
        };
    }

    private void closeIfConnected() throws PulsarClientException {
        if(consumer.isConnected()) {
            consumer.close();
        }
    }

    @Override
    protected synchronized void actuallyInterruptReception() {
        throw new UnsupportedOperationException();
    }
}
