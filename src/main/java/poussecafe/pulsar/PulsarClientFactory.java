package poussecafe.pulsar;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarClientFactory {

    public PulsarClientFactory(PulsarMessagingConfiguration configuration) {
        Objects.requireNonNull(configuration);
        this.configuration = configuration;
    }

    private PulsarMessagingConfiguration configuration;

    public PulsarClient build() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(configuration.brokerUrl())
                .statsInterval(configuration.statsInterval().toSeconds(), TimeUnit.SECONDS)
                .build();
    }
}
