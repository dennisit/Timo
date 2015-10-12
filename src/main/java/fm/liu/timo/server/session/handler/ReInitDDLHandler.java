package fm.liu.timo.server.session.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import fm.liu.timo.TimoConfig;
import fm.liu.timo.TimoServer;
import fm.liu.timo.config.model.Table;
import fm.liu.timo.mysql.packet.ErrorPacket;
import fm.liu.timo.mysql.packet.OkPacket;
import fm.liu.timo.net.connection.BackendConnection;
import fm.liu.timo.route.Outlets;
import fm.liu.timo.server.session.Session;

/**
 * @author liuhuanting
 */
public class ReInitDDLHandler extends SessionResultHandler {

    private Table      table;
    private AtomicLong affectedRows = new AtomicLong();

    public ReInitDDLHandler(Session session, Outlets outs) {
        this.session = session;
        this.count = new AtomicInteger(outs.getResult().size());
        this.table = outs.getTable();
    }

    @Override
    public void ok(byte[] ok, BackendConnection con) {
        session.release(con);
        OkPacket p = new OkPacket();
        p.read(ok);
        affectedRows.addAndGet(p.affectedRows);
        if (decrement()) {
            if (this.failed()) {
                super.onError();
                return;
            }
            TimoConfig config = TimoServer.getInstance().getConfig();
            new InitDDLHandler(table, config.getNodes(), config.getSystem().isAutoIncrement(),
                    session.getFront(), "REINIT").execute();
        }
    }

    @Override
    public void error(byte[] error, BackendConnection con) {
        ErrorPacket err = new ErrorPacket();
        err.read(error);
        this.setFail(err.errno, new String(err.message));
        session.release(con);
        if (decrement()) {
            super.onError();
        }
    }

    @Override
    public void field(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection con) {}

    @Override
    public void row(byte[] row, BackendConnection con) {}

    @Override
    public void eof(byte[] eof, BackendConnection con) {}

}
