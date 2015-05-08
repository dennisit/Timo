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
package re.ovo.timo.server.handler;

import re.ovo.timo.server.ServerConnection;
import re.ovo.timo.server.parser.ServerParse;
import re.ovo.timo.server.parser.ServerParseShow;
import re.ovo.timo.server.response.ShowTimoCluster;
import re.ovo.timo.server.response.ShowTimoStatus;
import re.ovo.timo.server.response.ShowDataSources;
import re.ovo.timo.server.response.ShowDatabases;

/**
 * @author xianmao.hexm
 */
public final class ShowHandler {

    public static void handle(String stmt, ServerConnection c, int offset) {
        switch (ServerParseShow.parse(stmt, offset)) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(c);
                break;
            case ServerParseShow.DATASOURCES:
                ShowDataSources.response(c);
                break;
            case ServerParseShow.Timo_STATUS:
                ShowTimoStatus.response(c);
                break;
            case ServerParseShow.Timo_CLUSTER:
                ShowTimoCluster.response(c);
                break;
            default:
                c.execute(stmt, ServerParse.SHOW);
        }
    }

}