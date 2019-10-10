package poussecafe.pulsar;

import java.util.Objects;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import poussecafe.messaging.MessageReceiverConfiguration;
import poussecafe.messaging.Messaging;
import poussecafe.messaging.MessagingConnection;

public class PulsarMessaging extends Messaging {

    public PulsarMessaging(PulsarMessagingConfiguration configuration) {
        Objects.requireNonNull(configuration);
        this.configuration = configuration;

        try {
            client = new PulsarClientFactory(configuration)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimePulsarClientException("Unable to create client", e);
        }

        consumerFactory = new ConsumerFactory.Builder()
                .configuration(configuration)
                .client(client)
                .build();
    }

    private PulsarMessagingConfiguration configuration;

    private PulsarClient client;

    private ConsumerFactory consumerFactory;

    public static final String NAME = "pulsar";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public MessagingConnection connect(MessageReceiverConfiguration receiverConfiguration) {
        return new MessagingConnection.Builder()
                .messaging(this)
                .messageReceiver(new PulsarMessageReceiver.Builder()
                        .configuration(receiverConfiguration)
                        .consumerFactory(consumerFactory)
                        .build())
                .messageSender(new PulsarMessageSender.Builder()
                        .configuration(configuration)
                        .client(client)
                        .build())
                .build();
    }
}
