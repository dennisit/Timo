package fm.liu.timo.server.session.handler;

import java.util.List;
import java.util.Map;
import org.pmw.tinylog.Logger;
import fm.liu.messenger.Mail;
import fm.liu.timo.TimoServer;
import fm.liu.timo.backend.Node;
import fm.liu.timo.config.model.Table;
import fm.liu.timo.config.util.AutoIncrement;
import fm.liu.timo.mysql.packet.OkPacket;
import fm.liu.timo.mysql.packet.RowDataPacket;
import fm.liu.timo.net.connection.BackendConnection;
import fm.liu.timo.parser.ast.expression.primary.Identifier;
import fm.liu.timo.parser.ast.fragment.ddl.ColumnDefinition;
import fm.liu.timo.parser.ast.fragment.ddl.datatype.DataType;
import fm.liu.timo.parser.util.Pair;
import fm.liu.timo.server.ServerConnection;

/**
 * @author liuhuanting
 */
public class InitDDLHandler extends SimpleHandler {
    private Table              table;
    private Map<Integer, Node> nodes;
    private boolean            isAutoIncrement;
    private ServerConnection   front;
    private String             type;

    public InitDDLHandler(Table table, Map<Integer, Node> nodes, boolean isAutoIncrement,
            ServerConnection front, String type) {
        this.table = table;
        this.nodes = nodes;
        this.isAutoIncrement = isAutoIncrement;
        this.front = front;
        this.type = type;
    }

    public void execute() {
        nodes.get(table.getRandomNode()).getSource().notNullGet()
                .query("SHOW CREATE TABLE " + table.getName(), this);
    }

    @Override
    public void field(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection con) {}

    @Override
    public void row(byte[] row, BackendConnection con) {
        RowDataPacket packet = new RowDataPacket(2);
        packet.read(row);
        table.recordDDL(new String(packet.fieldValues.get(1)));
    }

    @Override
    protected void eof(byte[] eof) {
        if (this.isAutoIncrement) {
            table.setAutoIncrement(null);
            for (Pair<Identifier, ColumnDefinition> pair : table.getColumns()) {
                ColumnDefinition c = pair.getValue();
                if (c.isAutoIncrement()) {
                    table.setAutoIncrement(new AutoIncrement(pair.getKey().getIdTextUpUnescape(), 0,
                            getLengthByType(c.getDataType())));
                }
            }
            if (table.getAutoIncrement() != null) {
                String sql = "SELECT MAX(" + table.getAutoIncrement().getColumn() + ") FROM "
                        + table.getName();
                InitAutoIncrementHandler handler = new InitAutoIncrementHandler(table, front, type);
                front = null;
                table.getNodes()
                        .forEach(i -> nodes.get(i).getSource().notNullGet().query(sql, handler));
            } else {
                sendMail(true);
            }
        }
        if (front != null) {
            front.write(OkPacket.OK);
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
        Logger.warn("table '{}' is not created", table.getName());
        sendMail(true);
    }

    private long getLengthByType(DataType type) {
        long max;
        switch (type.getTypeName()) {
            case TINYINT:
                max = 127L;
                break;
            case SMALLINT:
                max = 32767L;
                break;
            case MEDIUMINT:
                max = 8388607L;
                break;
            case INT:
                max = 2147483647L;
                break;
            case BIGINT:
                max = 9223372036854775807L;
                break;
            default:
                throw new IllegalArgumentException("unsupported auto_increment column type");
        }
        if (type.isUnsigned()) {
            max = 2 * max + 1L;
        }
        return max;
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
