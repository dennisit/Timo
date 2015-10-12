package fm.liu.timo.util.messenger;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostOffice implements Runnable {
    private static PostOffice                      office     = null;
    private final Set<Messenger>                        users      = new CopyOnWriteArraySet<>();
    private final AtomicBoolean                    isClosed   = new AtomicBoolean();
    private final ExecutorService                  threadPool = Executors.newCachedThreadPool();
    private final ThreadLocal<ArrayDeque<Mail<?>>> mails      =
            new ThreadLocal<ArrayDeque<Mail<?>>>() {
                @Override
                protected ArrayDeque<Mail<?>> initialValue() {
                    return new ArrayDeque<Mail<?>>();
                }
            };

    private PostOffice() {
        threadPool.submit(this);
    }

    public static PostOffice getInstance() {
        open();
        return office;
    }

    private static void open() {
        if (office == null) {
            office = new PostOffice();
        }
    }

    Mailman assign(final Messenger user) {
        return new Mailman(this, new Mailbox(), user);
    }

    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void run() {
        while (!isClosed()) {
            boolean scheduled = false;
            for (Messenger user : users) {
                Mailman postman = user.getPostman();
                if (postman.needScheduling()) {
                    scheduled = true;
                    schedulePostman(postman);
                }
            }
            if (scheduled) {
                Thread.yield();
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    public Set<Messenger> getUsers() {
        return users;
    }

    public void register(Messenger user) {
        users.add(user);
    }

    public void unregister(Messenger user) {
        users.remove(user);
    }

    public boolean send(Mail<?> mail) {
        if (isClosed.get()) {
            return false;
        }
        final ArrayDeque<Mail<?>> localQueue = mails.get();
        if (!localQueue.isEmpty()) {
            localQueue.addLast(mail);
            return true;
        }
        localQueue.addLast(mail);
        while ((mail = localQueue.peekFirst()) != null) {
            for (Messenger user : mail.to) {
                if (user == mail.from) {
                    continue;
                }
                if (users.contains(user)) {
                    user.getPostman().deliver(mail);
                }
            }
            localQueue.pollFirst();
        }
        return true;
    }

    final void schedulePostman(final Mailman postman) {
        threadPool.submit(postman);
    }

    public void close() {
        office = null;
        isClosed.set(true);
        threadPool.shutdown();
        shutdownAndAwaitTermination(threadPool);
        mails.remove();
    }

    private void shutdownAndAwaitTermination(ExecutorService pool) {
        try {
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

}
