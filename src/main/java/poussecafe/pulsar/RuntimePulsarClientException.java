package poussecafe.pulsar;

@SuppressWarnings("serial")
public class RuntimePulsarClientException extends RuntimeException {

    public RuntimePulsarClientException() {
        super();
    }

    public RuntimePulsarClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public RuntimePulsarClientException(String message) {
        super(message);
    }

    public RuntimePulsarClientException(Throwable cause) {
        super(cause);
    }
}
