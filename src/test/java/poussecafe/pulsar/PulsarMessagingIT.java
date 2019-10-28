package poussecafe.pulsar;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import poussecafe.messaging.MessageReceiverConfiguration;
import poussecafe.messaging.MessageSender;
import poussecafe.messaging.MessagingConnection;
import poussecafe.processing.MessageBroker;
import poussecafe.processing.ReceivedMessage;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class PulsarMessagingIT {

    @Test
    public void sendAndReceive() throws InterruptedException {
        givenMessageBrokerMock();
        givenStartedConnection();
        whenSendingMessages();
        thenMessagesReceivedInOrder();
    }

    private void givenMessageBrokerMock() {
        messageBroker = mock(MessageBroker.class);
        Mockito.doAnswer(this::brokerDispatch).when(messageBroker).dispatch(any());
    }

    private MessageBroker messageBroker;

    private Void brokerDispatch(InvocationOnMock invocation) {
        ReceivedMessage receivedMessage = invocation.getArgument(0);
        receivedMessage.ack();
        receivedMessages.add((TestMessage) receivedMessage.message().original());
        return null;
    }

    private BlockingQueue<TestMessage> receivedMessages = new LinkedBlockingQueue<>();

    private void givenStartedConnection() {
        PulsarMessagingConfiguration configuration = new PulsarMessagingConfiguration.Builder()
                .brokerUrl("pulsar://localhost:6650")
                .subscriptionName("test")
                .subscriptionTopics(asList("test-topic"))
                .defaultPublicationTopic("test-topic")
                .build();
        MessageReceiverConfiguration receiverConfiguration = new MessageReceiverConfiguration.Builder()
                .messageBroker(messageBroker)
                .build();
        connection = new PulsarMessaging(configuration).connect(receiverConfiguration);
        connection.startReceiving();
    }

    private MessagingConnection connection;

    private void whenSendingMessages() {
        MessageSender sender = connection.messageSender();
        for(int i = FIRST_MESSAGE_ID; i <= LAST_MESSAGE_ID; ++i) {
            sender.sendMessage(new TestMessage(i));
        }
    }

    private static final int FIRST_MESSAGE_ID = 1;

    private static final int LAST_MESSAGE_ID = 100;

    private void thenMessagesReceivedInOrder() throws InterruptedException {
        for(int i = FIRST_MESSAGE_ID; i <= LAST_MESSAGE_ID; ++i) {
            TestMessage receivedMessage = receivedMessages.take();
            assertThat(receivedMessage.id, equalTo(i));
        }
    }
}
