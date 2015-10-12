package fm.liu.timo.util.messenger;

import java.util.HashSet;
import java.util.Set;

public class Mail<T> {
    public Messenger            from;
    public final Set<Messenger> to;
    public final T         msg;

    public Mail(T msg) {
        this.to = PostOffice.getInstance().getUsers();
        this.msg = msg;
    }

    public Mail(Messenger to, T msg) {
        this.to = new HashSet<Messenger>();
        this.to.add(to);
        this.msg = msg;
    }

    public Mail(Set<Messenger> to, T msg) {
        this.to = to;
        this.msg = msg;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("Mail from: <").append(from.name).append("> to: <");
        int i = 0;
        for (Messenger user : to) {
            s.append(user.name);
            i++;
            if (i != to.size()) {
                s.append(",");
            }
        }
        s.append(">, msg: <").append(msg).append(">.");
        return s.toString();
    }

    public void setFrom(Messenger from) {
        this.from = from;
    }
}
