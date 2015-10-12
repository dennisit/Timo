package fm.liu.timo.server.session.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import fm.liu.messenger.Mail;
import fm.liu.timo.TimoServer;
import fm.liu.timo.config.model.Table;
import fm.liu.timo.mysql.packet.OkPacket;
import fm.liu.timo.mysql.packet.RowDataPacket;
import fm.liu.timo.net.connection.BackendConnection;
import fm.liu.timo.server.ServerConnection;

/**
 * @author liuhuanting
 */
public class InitAutoIncrementHandler extends SimpleHandler {
    private Table            table;
    private ServerConnection front;
    private String           type;
    private AtomicInteger    count;

    public InitAutoIncrementHandler(Table table, ServerConnection front, String type) {
        this.table = table;
        this.front = front;
        this.type = type;
        this.count = new AtomicInteger(table.getNodes().size());
    }

    @Override
    public void field(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection con) {}

    @Override
    public void row(byte[] row, BackendConnection con) {
        RowDataPacket p = new RowDataPacket(1);
        p.read(row);
        String val = new String(p.fieldValues.get(0)).trim();
        if (val != null && !val.equals("")) {
            long tmp = Long.parseLong(val);
            if (tmp > table.getAutoIncrement().current().longValue()) {
                table.getAutoIncrement().current().set(tmp);
            }
        }
    }

    @Override
    protected void eof(byte[] eof) {
        if (count.decrementAndGet() == 0) {
            if (front != null) {
                front.write(OkPacket.OK);
                front = null;
            }
            sendMail(true);
        }
    }

    @Override
    public void close(String reason) {
        sendMail(false);
    }

    @Override
    protected void ok(byte[] ok) {}

    @Override
    protected void error(byte[] error) {
        if (count.decrementAndGet() == 0) {
            sendMail(false);
        }
    }

    private void sendMail(boolean success) {
        switch (type) {
            case "INIT":
                TimoServer.getSender()
                        .send(new Mail<String>(TimoServer.getInstance().getStarter(), type));
                break;
            case "RELOAD":
                //TODO
                break;
        }
    }
}
