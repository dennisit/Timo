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
package fm.liu.timo.config.model;

import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import fm.liu.timo.config.util.AutoIncrement;
import fm.liu.timo.parser.ast.expression.primary.Identifier;
import fm.liu.timo.parser.ast.fragment.ddl.ColumnDefinition;
import fm.liu.timo.parser.ast.stmt.ddl.DDLCreateTableStatement;
import fm.liu.timo.parser.recognizer.SQLParserDelegate;
import fm.liu.timo.parser.util.Pair;

/**
 * @author Liu Huanting 2015年5月9日
 * 逻辑数据表配置信息
 */
public class Table {
    private final int                                databaseID;
    private final String                             name;
    private final TableType                          type;
    private final Rule                               rule;
    private final List<Integer>                      nodes;
    private AutoIncrement                            autoIncrement;
    private volatile DDLCreateTableStatement         ddl;
    private List<Pair<Identifier, ColumnDefinition>> columns;

    public Table(int databaseID, String name, int type, Rule rule, List<Integer> nodes) {
        this.databaseID = databaseID;
        this.name = name;
        switch (type) {
            case 1:
                this.type = TableType.SPLIT;
                break;
            default:
                this.type = TableType.GLOBAL;
        }
        this.rule = rule;
        this.nodes = nodes;
    }

    public int getDatabaseID() {
        return databaseID;
    }

    public String getName() {
        return name;
    }

    public TableType getType() {
        return type;
    }

    public Rule getRule() {
        return rule;
    }

    public List<Integer> getNodes() {
        return nodes;
    }

    public enum TableType {
        GLOBAL, SPLIT
    }

    public int getRandomNode() {
        return nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
    }

    public void recordDDL(String sql) {
        try {
            this.ddl = (DDLCreateTableStatement) SQLParserDelegate.parse(sql);
            this.columns = ddl.getColDefs();
        } catch (SQLSyntaxErrorException e) {
            e.printStackTrace();
        }
    }

    public DDLCreateTableStatement getDDL() {
        return ddl;
    }

    public List<Pair<Identifier, ColumnDefinition>> getColumns() {
        return columns;
    }

    public AutoIncrement getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(AutoIncrement autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

}
