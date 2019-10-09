package poussecafe.pulsar;

import java.util.Objects;
import poussecafe.messaging.MessageReceiverConfiguration;
import poussecafe.messaging.Messaging;
import poussecafe.messaging.MessagingConnection;

public class PulsarMessaging extends Messaging {

    public PulsarMessaging(PulsarMessagingConfiguration configuration) {
        Objects.requireNonNull(configuration);
        this.configuration = configuration;

        consumerFactory = new ConsumerFactory(configuration);
    }

    private PulsarMessagingConfiguration configuration;

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
                .messageSender(new PulsarMessageSender(configuration))
                .build();
    }
}
