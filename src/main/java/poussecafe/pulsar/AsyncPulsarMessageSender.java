package poussecafe.pulsar;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poussecafe.messaging.Message;
import poussecafe.runtime.OriginalAndMarshaledMessage;

public class AsyncPulsarMessageSender extends PulsarMessageSender {

    @SuppressWarnings("squid:S2176")
    public static class Builder implements PulsarMessageSender.Builder  {

        private AsyncPulsarMessageSender sender = new AsyncPulsarMessageSender();

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
        public AsyncPulsarMessageSender build() {
            Objects.requireNonNull(sender.configuration);
            Objects.requireNonNull(sender.client);
            sender.defaultTopicProducer = sender.createProducer(sender.configuration.defaultPublicationTopic());
            return sender;
        }
    }

    private AsyncPulsarMessageSender() {
        ackReceptionThread = new Thread(this::waitAcks);
        ackReceptionThread.setDaemon(true);
        ackReceptionThread.setName("Ack reception thread for consumer " + toString());
        ackReceptionThread.start();
    }

    private void waitAcks() {
        int counter = 0;
        while(true) {
            MessageAndCompletable messageAndCompletable = null;
            try {
                messageAndCompletable = pendingAcks.take();
                if(messageAndCompletable == STOP) {
                    return;
                }
                CompletableFuture<MessageId> pendingAck = messageAndCompletable.completable;
                MessageId messageId = pendingAck.get();
                logger.debug("Got ID {} for message #{}", messageId, counter);
                ++counter;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException e) {
                logger.error("Failed sending message {}", messageAndCompletable.message, e);
            }
        }
    }

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Thread ackReceptionThread;

    @Override
    protected ProducerBuilder<String> createProducerBuilder(String topic) {
        return super.createProducerBuilder(topic)
                .blockIfQueueFull(true);
    }

    @Override
    protected synchronized void sendMarshalledMessage(OriginalAndMarshaledMessage marshalledMessage) {
        Message originalMessage = marshalledMessage.original();
        String marshalled = (String) marshalledMessage.marshaled();
        CompletableFuture<MessageId> pendingAck = producer(originalMessage).sendAsync(marshalled);

        MessageAndCompletable messageAndCompletable = new MessageAndCompletable();
        messageAndCompletable.message = marshalledMessage;
        messageAndCompletable.completable = pendingAck;
        pendingAcks.add(messageAndCompletable);
    }

    private BlockingQueue<MessageAndCompletable> pendingAcks = new LinkedBlockingQueue<>();

    private static class MessageAndCompletable {

        OriginalAndMarshaledMessage message;

        CompletableFuture<MessageId> completable;
    }

    private static final MessageAndCompletable STOP = new MessageAndCompletable();

    @Override
    public void close() {
        pendingAcks.add(STOP);
        try {
            ackReceptionThread.join();
        } catch (InterruptedException e) {
            logger.error("Thread was interrupted while joining ack reception thread");
            Thread.currentThread().interrupt();
        }
        super.close();
    }
}
