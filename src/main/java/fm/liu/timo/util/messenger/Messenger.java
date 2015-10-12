package fm.liu.timo.util.messenger;

public abstract class Messenger {
    final String     name;
    final Mailman    mailman;
    final PostOffice office;

    public Messenger() {
        this(null);
    }

    public Messenger(final String name) {
        this.name = name;
        this.office = PostOffice.getInstance();
        this.mailman = office.assign(this);
    }

    public void register() {
        office.register(this);
    }

    public void unregister() {
        office.unregister(this);
    }

    public String getName() {
        return name;
    }

    public Mailman getPostman() {
        return mailman;
    }

    public void send(final Mail<?> mail) {
        mail.setFrom(this);
        office.send(mail);
    }

    public abstract void receive(final Mail<?> mail);
}
