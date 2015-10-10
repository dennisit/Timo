package fm.liu.timo.server.session;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import fm.liu.timo.TimoServer;
import fm.liu.timo.backend.Node;
import fm.liu.timo.config.Isolations;
import fm.liu.timo.config.model.Database;
import fm.liu.timo.mysql.handler.xa.XACommitHandler;
import fm.liu.timo.mysql.handler.xa.XAEndHandler;
import fm.liu.timo.mysql.handler.xa.XAPrepareHandler;
import fm.liu.timo.mysql.handler.xa.XARollbackHandler;
import fm.liu.timo.mysql.handler.xa.XAStartHandler;
import fm.liu.timo.mysql.packet.OkPacket;
import fm.liu.timo.net.connection.BackendConnection;
import fm.liu.timo.server.ServerConnection;
import fm.liu.timo.server.session.handler.ResultHandler;

/**
 * @author liuhuanting
 */
public class XATransactionSession extends TransactionSession {
    private String   XID;
    private XAState  state;
    private File     prepareLog;
    private Database database;

    public enum XAState {
        ACTIVE, IDLE, PREPARED, FINISHED
    }

    public XATransactionSession(ServerConnection front) {
        super(front);
        variables.setIsolationLevel(Isolations.SERIALIZABLE);
    }

    public void start(Database database) {
        this.setDatabase(database);
        this.XID = TimoServer.getInstance().nextXID();
        Map<Integer, Node> nodes = TimoServer.getInstance().getConfig().getNodes();
        ResultHandler handler = new XAStartHandler(this, database.getNodes().size());
        String db = database.getName();
        while (TimoServer.getXaCommiting().get(db).get() > 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        TimoServer.getXaStarting().get(db).incrementAndGet();
        database.getNodes()
                .forEach(id -> nodes.get(id).getSource().query("XA START " + XID, handler));
    }

    @Override
    public void commit(boolean restart) {
        if (getConnections().isEmpty()) {
            super.commit(restart);
            return;
        }
        Collection<BackendConnection> cons = availableConnections();
        if (cons.size() == getConnections().size()) {
            ResultHandler handler = new XAEndHandler(this, cons, true, restart);
            cons.forEach(con -> con.query("XA END " + XID, handler));
        } else {
            onError();
        }
    }

    public void xaPrepare(boolean restart) {
        if (getConnections().isEmpty()) {
            super.commit(restart);
            return;
        }
        Collection<BackendConnection> cons = availableConnections();
        if (cons.size() == getConnections().size()) {
            ResultHandler handler = new XAPrepareHandler(this, cons, restart);
            cons.forEach(con -> con.query("XA PREPARE " + XID, handler));
        } else {
            onError();
        }
    }

    public void xaCommit(boolean restart) {
        if (getConnections().isEmpty()) {
            super.commit(restart);
            return;
        }
        String db = database.getName();
        while (TimoServer.getXaStarting().get(db).get() > 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        Collection<BackendConnection> cons = availableConnections();
        if (cons.size() == getConnections().size()) {
            ResultHandler handler = new XACommitHandler(this, cons, restart);
            TimoServer.getXaCommiting().get(db).incrementAndGet();
            cons.forEach(con -> con.query("XA COMMIT " + XID, handler));
        } else {
            onError();
        }
    }

    private void rollback() {
        if (getConnections().isEmpty()) {
            super.commit(false);
            return;
        }
        Collection<BackendConnection> cons = availableConnections();
        if (cons.size() == getConnections().size()) {
            ResultHandler handler = new XAEndHandler(this, cons, false, false);
            cons.forEach(con -> con.query("XA END " + XID, handler));
        } else {
            onError();
        }
    }

    public void xaRollback() {
        if (getConnections().isEmpty()) {
            super.commit(false);
            return;
        }
        Collection<BackendConnection> cons = availableConnections();
        if (cons.size() == getConnections().size()) {
            ResultHandler handler = new XARollbackHandler(this, cons, true);
            cons.forEach(con -> con.query("XA ROLLBACK " + XID, handler));
        } else {
            onError();
        }
    }

    @Override
    public void rollback(boolean response) {
        switch (state) {
            case ACTIVE:
                rollback();
                break;
            default:
                front.write(OkPacket.OK);
                break;
        }
    }

    @Override
    public void clear() {
        front.reset();
        KeySetView<Integer, BackendConnection> keys = connections.keySet();
        for (Integer id : keys) {
            BackendConnection con = connections.remove(id);
            if (con.isClosed()) {
                continue;
            }
            con.setHandler(null);
            con.close("cleared");
        }
        this.state = XAState.FINISHED;
        this.XID = null;
    }

    public void release() {
        front.reset();
        KeySetView<Integer, BackendConnection> keys = connections.keySet();
        for (Integer id : keys) {
            BackendConnection con = connections.remove(id);
            con.release();
        }
        this.state = XAState.FINISHED;
        this.XID = null;
    }

    public XAState getState() {
        return state;
    }

    public void setState(XAState state) {
        this.state = state;
        if (this.prepareLog != null && state == XAState.FINISHED) {
            this.prepareLog.delete();
        }
    }

    public String getXID() {
        return XID;
    }

    public void setPrepareLog(File file) {
        this.prepareLog = file;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }
}
