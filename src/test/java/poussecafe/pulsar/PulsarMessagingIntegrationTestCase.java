package poussecafe.pulsar;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import poussecafe.messaging.MessageReceiverConfiguration;
import poussecafe.messaging.MessagingConnection;
import poussecafe.processing.MessageBroker;
import poussecafe.processing.ReceivedMessage;

import static java.util.Arrays.asList;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public abstract class PulsarMessagingIntegrationTestCase {

    @Before
    public void clearTopic() {
        MessageBroker messageBroker = mock(MessageBroker.class);
        Mockito.doAnswer(this::ack).when(messageBroker).dispatch(any());
        MessagingConnection connection = openConnectionWith(messageBroker, false);
        Duration maxWait = Duration.ofSeconds(10);
        Duration inactivityDuration = Duration.ofSeconds(3);
        await().atMost(maxWait).until(() -> lastAckInThePast(inactivityDuration));
        connection.close();
    }

    private Void ack(InvocationOnMock invocation) {
        ReceivedMessage receivedMessage = invocation.getArgument(0);
        receivedMessage.ack();
        lastAck.set(LocalDateTime.now());
        return null;
    }

    private AtomicReference<LocalDateTime> lastAck = new AtomicReference<>(LocalDateTime.now());

    private boolean lastAckInThePast(Duration durationInThePast) {
        return lastAck.get().isBefore(LocalDateTime.now().minus(durationInThePast));
    }

    protected MessagingConnection openConnectionWith(MessageBroker messageBroker, boolean sendAsynchronously) {
        PulsarMessagingConfiguration configuration = new PulsarMessagingConfiguration.Builder()
                .brokerUrl("pulsar://localhost:6650")
                .subscriptionName("test")
                .subscriptionTopics(asList("test-topic"))
                .defaultPublicationTopic("test-topic")
                .sendAsynchronously(sendAsynchronously)
                .build();
        MessageReceiverConfiguration receiverConfiguration = new MessageReceiverConfiguration.Builder()
                .messageBroker(messageBroker)
                .build();
        MessagingConnection connection = new PulsarMessaging(configuration).connect(receiverConfiguration);
        connection.open();
        return connection;
    }
}
