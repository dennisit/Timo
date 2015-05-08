/*
 * Copyright 1999-2012 Alibaba Group.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package re.ovo.timo.manager.response;

import java.nio.ByteBuffer;

import re.ovo.timo.TimoServer;
import re.ovo.timo.config.Fields;
import re.ovo.timo.manager.ManagerConnection;
import re.ovo.timo.mysql.PacketUtil;
import re.ovo.timo.net.FrontendConnection;
import re.ovo.timo.net.NIOProcessor;
import re.ovo.timo.net.buffer.BufferQueue;
import re.ovo.timo.net.mysql.EOFPacket;
import re.ovo.timo.net.mysql.FieldPacket;
import re.ovo.timo.net.mysql.ResultSetHeaderPacket;
import re.ovo.timo.net.mysql.RowDataPacket;
import re.ovo.timo.server.ServerConnection;
import re.ovo.timo.util.IntegerUtil;
import re.ovo.timo.util.LongUtil;
import re.ovo.timo.util.StringUtil;
import re.ovo.timo.util.TimeUtil;

/**
 * 查看当前有效连接信息
 * 
 * @author xianmao.hexm 2010-9-27 下午01:16:57
 * @author wenfeng.cenwf 2011-4-25
 */
public final class ShowConnection {

    private static final int FIELD_COUNT = 14;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("PROCESSOR", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOCAL_PORT", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCHEMA", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CHARSET", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_IN", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("NET_OUT", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("ALIVE_TIME(S)", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("WRITE_ATTEMPTS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("RECV_BUFFER", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SEND_QUEUE", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("CHANNELS", Fields.FIELD_TYPE_LONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c);
        }

        // write eof
        buffer = eof.write(buffer, c);

        // write rows
        byte packetId = eof.packetId;
        String charset = c.getCharset();
        for (NIOProcessor p : TimoServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc != null) {
                    RowDataPacket row = getRow(fc, charset);
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, c);
                }
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(FrontendConnection c, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(c.getProcessor().getName().getBytes());
        row.add(LongUtil.toBytes(c.getId()));
        row.add(StringUtil.encode(c.getHost(), charset));
        row.add(IntegerUtil.toBytes(c.getPort()));
        row.add(IntegerUtil.toBytes(c.getLocalPort()));
        row.add(StringUtil.encode(c.getSchema(), charset));
        row.add(StringUtil.encode(c.getCharset(), charset));
        row.add(LongUtil.toBytes(c.getNetInBytes()));
        row.add(LongUtil.toBytes(c.getNetOutBytes()));
        row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
        row.add(IntegerUtil.toBytes(c.getWriteAttempts()));
        ByteBuffer bb = c.getReadBuffer();
        row.add(IntegerUtil.toBytes(bb == null ? 0 : bb.capacity()));
        BufferQueue bq = c.getWriteQueue();
        row.add(IntegerUtil.toBytes(bq == null ? 0 : bq.size()));
        if (c instanceof ServerConnection) {
            ServerConnection sc = (ServerConnection) c;
            row.add(IntegerUtil.toBytes(sc.getSession().getTargetCount()));
        } else {
            row.add(null);
        }
        return row;
    }

}