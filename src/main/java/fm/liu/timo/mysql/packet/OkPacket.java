/*
 * Copyright 2015 Liu Huanting.
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
package fm.liu.timo.mysql.packet;

import java.nio.ByteBuffer;
import fm.liu.timo.mysql.BufferUtil;
import fm.liu.timo.mysql.MySQLMessage;

/**
 * From server to client in response to command, if no error and no result set.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0
 * 1-9 (Length Coded Binary)   affected_rows
 * 1-9 (Length Coded Binary)   insert_id
 * 2                           server_status
 * 2                           warning_count
 * n   (until end of packet)   message fix:(Length Coded String)
 * 
 * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#OK_Packet
 * </pre>
 * 
 */
public class OkPacket extends MySQLServerPacket {
    public static final byte   FIELD_COUNT = 0x00;
    public static final byte[] OK          = new byte[] {7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};

    public byte   fieldCount = FIELD_COUNT;
    public long   affectedRows;
    public long   insertId;
    public int    serverStatus;
    public int    warningCount;
    public byte[] message;

    @Override
    protected void readBody(MySQLMessage mm) {
        fieldCount = mm.read();
        affectedRows = mm.readLength();
        insertId = mm.readLength();
        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();
        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
    }

    @Override
    protected void writeBody(ByteBuffer buffer) {
        buffer.put(fieldCount);
        BufferUtil.writeLength(buffer, affectedRows);
        BufferUtil.writeLength(buffer, insertId);
        BufferUtil.writeUB2(buffer, serverStatus);
        BufferUtil.writeUB2(buffer, warningCount);
        if (message != null) {
            BufferUtil.writeWithLength(buffer, message);
        }
    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += BufferUtil.getLength(affectedRows);
        i += BufferUtil.getLength(insertId);
        i += 4;
        if (message != null) {
            i += BufferUtil.getLength(message);
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }

}
