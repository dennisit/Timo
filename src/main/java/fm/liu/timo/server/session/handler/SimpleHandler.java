package fm.liu.timo.server.session.handler;

import fm.liu.timo.net.connection.BackendConnection;

/**
 * @author liuhuanting
 */
public abstract class SimpleHandler implements ResultHandler {

    @Override
    public void ok(byte[] ok, BackendConnection con) {
        con.release();
        ok(ok);
    }

    @Override
    public void error(byte[] error, BackendConnection con) {
        con.release();
        error(error);
    }

    @Override
    public void eof(byte[] eof, BackendConnection con) {
        con.release();
        eof(eof);
    }

    protected abstract void eof(byte[] eof);

    protected abstract void ok(byte[] ok);

    protected abstract void error(byte[] error);

}
