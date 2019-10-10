package poussecafe.pulsar;

import java.util.Objects;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import poussecafe.exception.PousseCafeException;

public class ConsumerFactory {

    public static class Builder {

        private ConsumerFactory factory = new ConsumerFactory();

        public Builder configuration(PulsarMessagingConfiguration configuration) {
            factory.configuration = configuration;
            return this;
        }

        public Builder client(PulsarClient client) {
            factory.client = client;
            return this;
        }

        public ConsumerFactory build() {
            Objects.requireNonNull(factory.configuration);
            Objects.requireNonNull(factory.client);
            return factory;
        }
    }

    private ConsumerFactory() {

    }

    private PulsarMessagingConfiguration configuration;

    private PulsarClient client;

    public Consumer<String> buildConsumer() {
        try {
            return client.newConsumer(Schema.STRING)
                    .topics(configuration.topics())
                    .subscriptionType(configuration.subscriptionType())
                    .subscriptionName(configuration.subscriptionName())
                    .subscribe();
        } catch (PulsarClientException e) {
            throw new PousseCafeException("Unable to connect to Pulsar broker", e);
        }
    }
}
