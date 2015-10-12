package fm.liu.timo.util.messenger;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Mailbox {
    private final ConcurrentLinkedQueue<Mail<?>> mailbox = new ConcurrentLinkedQueue<>();

    public Mail<?> poll() {
        return mailbox.poll();
    }

    public boolean offer(final Mail<?> message) {
        return mailbox.offer(message);
    }

    public boolean isEmpty() {
        return mailbox.isEmpty();
    }

    public int size() {
        return mailbox.size();
    }
}
