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
package fm.liu.timo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.pmw.tinylog.Logger;
import fm.liu.timo.backend.Node;
import fm.liu.timo.backend.Source;
import fm.liu.timo.config.ErrorCode;
import fm.liu.timo.config.loader.ServerConfigLoader;
import fm.liu.timo.config.loader.SystemConfigLoader;
import fm.liu.timo.config.model.Database;
import fm.liu.timo.config.model.Datanode;
import fm.liu.timo.config.model.Datasource;
import fm.liu.timo.config.model.SystemConfig;
import fm.liu.timo.config.model.User;
import fm.liu.timo.manager.ManagerConnection;
import fm.liu.timo.mysql.packet.OkPacket;
import fm.liu.timo.net.connection.Variables;
import fm.liu.timo.server.session.handler.InitDDLHandler;
import fm.liu.timo.util.messenger.Mail;
import fm.liu.timo.util.messenger.Messenger;

/**
 * @author Liu Huanting 2015年5月10日
 */
public class TimoConfig {
    private volatile SystemConfig          system;
    private volatile Map<String, User>     users;
    private volatile Map<String, Database> databases;
    private volatile Map<Integer, Node>    nodes;
    private ReentrantLock                  lock = new ReentrantLock();
    private Messenger                      reloader;

    public TimoConfig() {
        this.system = new SystemConfigLoader().getSystemConfig();
        ServerConfigLoader conf =
                new ServerConfigLoader(system.getUrl(), system.getUsername(), system.getPassword());
        this.users = conf.getUsers();
        this.databases = conf.getDatabases();
        this.nodes = initDatanodes(conf.getDatanodes(), conf.getDatasources(), conf.getHandovers());
    }

    private Map<Integer, Node> initDatanodes(Map<Integer, Datanode> datanodes,
            Map<Integer, Datasource> datasources, Map<Integer, ArrayList<Integer>> handovers) {
        Variables variables = new Variables();
        variables.setCharset(system.getCharset());
        variables.setIsolationLevel(system.getTxIsolation());
        Map<Integer, Source> sources = new HashMap<>();
        for (Entry<Integer, Datasource> datasource : datasources.entrySet()) {
            sources.put(datasource.getKey(),
                    new Source(datasource.getValue(), variables, system.getHeartbeatPeriod()));
        }

        for (Integer id : handovers.keySet()) {
            ArrayList<Source> backups = new ArrayList<>();
            for (Integer handover : handovers.get(id)) {
                backups.add(sources.get(handover));
            }
            sources.get(id).setBackups(backups);
        }

        Map<Integer, Node> nodes = new HashMap<Integer, Node>();
        for (Datanode datanode : datanodes.values()) {
            ArrayList<Source> sourceList = new ArrayList<Source>();
            for (Integer i : datanode.getDatasources()) {
                sourceList.add(sources.get(i));
            }
            Node node = new Node(datanode.getID(), datanode.getStrategy(), sourceList);
            nodes.put(datanode.getID(), node);
        }
        return nodes;
    }

    public void reload(ManagerConnection c) {
        AtomicBoolean success = new AtomicBoolean(true);
        SystemConfig _system = new SystemConfigLoader().getSystemConfig();
        ServerConfigLoader _conf =
                new ServerConfigLoader(system.getUrl(), system.getUsername(), system.getPassword());
        Map<String, User> _users = _conf.getUsers();
        Map<String, Database> _databases = _conf.getDatabases();
        Map<Integer, Datasource> _datasources = _conf.getDatasources();
        Map<Integer, Node> _nodes =
                initDatanodes(_conf.getDatanodes(), _datasources, _conf.getHandovers());
        _nodes.values().forEach(n -> {
            if (!n.init()) {
                success.set(false);
            }
        });
        final AtomicInteger count = new AtomicInteger();
        this.databases.values().forEach(db -> count.addAndGet(db.getTables().size()));
        reloader = new Messenger() {
            @Override
            public void receive(Mail<?> mail) {
                success.set(success.get() & ((Boolean) mail.msg).booleanValue());
                if (count.decrementAndGet() == 0) {
                    executeReload(success.get(), _system, _users, _databases, _nodes, c);
                }
            }
        };
        reloader.register();
        _databases.values()
                .forEach(db -> db.getTables().values().forEach(
                        t -> new InitDDLHandler(t, _nodes, system.isAutoIncrement(), null, "RELOAD")
                                .execute()));
    }

    protected void executeReload(boolean success, SystemConfig _system, Map<String, User> _users,
            Map<String, Database> _databases, Map<Integer, Node> _nodes, ManagerConnection c) {
        if (success) {
            lock.lock();
            try {
                this.system = _system;
                this.users = _users;
                this.databases = _databases;
                this.nodes = _nodes;
                if (this.system.isEnableXA()) {
                    databases.keySet().forEach(db -> {
                        TimoServer.getXaStarting().put(db, new AtomicLong());
                        TimoServer.getXaCommiting().put(db, new AtomicLong());
                    });
                }
            } finally {
                lock.unlock();
            }
            c.write(OkPacket.OK);
            Logger.info("reload config success by manager");
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "reload config failed");
            Logger.info("reload config failed by manager");
            _nodes.values().forEach(n -> n.getSources().forEach(s -> s.clear("reload failed")));
        }
    }

    public SystemConfig getSystem() {
        return system;
    }

    public Map<String, User> getUsers() {
        return users;
    }

    public Map<String, Database> getDatabases() {
        return databases;
    }

    public Map<Integer, Node> getNodes() {
        return nodes;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public Messenger getReloader() {
        return reloader;
    }

}
