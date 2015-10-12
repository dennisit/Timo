package fm.liu.timo.util.messenger;

public class Mailman implements Runnable {
    private volatile boolean working = false;
    final PostOffice         office;
    final Mailbox            mailbox;
    final Messenger               user;

    public Mailman(final PostOffice office, final Mailbox mailbox, final Messenger user) {
        this.office = office;
        this.mailbox = mailbox;
        this.user = user;
    }

    final boolean deliver(final Mail<?> mail) {
        return mailbox.offer(mail);
    }

    @Override
    public void run() {
        if (working) {
            return;
        }
        try {
            working = true;
            if (mailbox.isEmpty()) {
                return;
            }
            long idleBegin = System.currentTimeMillis();
            Mail<?> mail = null;
            while (!office.isClosed()) {
                long now = System.currentTimeMillis();
                while ((mail = mailbox.poll()) != null) {
                    user.receive(mail);
                    now = System.currentTimeMillis();
                    idleBegin = now;
                }
                final long idle = now - idleBegin;
                if (idle > 1000) {
                    break;
                }
                if (idle > 100) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }
        } finally {
            working = false;
        }
    }

    public boolean needScheduling() {
        return !(working || mailbox.isEmpty());
    }

}
