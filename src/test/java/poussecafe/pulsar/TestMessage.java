package poussecafe.pulsar;

import poussecafe.messaging.Message;

public class TestMessage implements Message {

    public TestMessage() {

    }

    public TestMessage(int id) {
        this.id = id;
    }

    public int id;
}
