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
package fm.liu.timo.route;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import fm.liu.timo.config.model.Table;

/**
 * @author Liu Huanting 2015年5月10日
 */
public class Outlets {
    private Set<Outlet>          outlets;
    private Table                table;
    private int                  info;
    private Set<String>          groupBy;
    private Map<String, Integer> orderBy;
    private int                  limitSize   = -1;
    private int                  limitOffset = 0;
    private boolean              usingMaster = false;

    public Outlets() {
        this.outlets = new HashSet<Outlet>();
    }

    public void add(Outlet out) {
        outlets.add(out);
    }

    public void set(Set<Outlet> outlets) {
        this.outlets = outlets;
    }

    public Set<Outlet> getResult() {
        return outlets;
    }

    public int size() {
        return outlets.size();
    }

    public int getInfo() {
        return info;
    }

    public void setInfo(int info) {
        this.info = info;
    }

    public Set<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(Set<String> groupBy) {
        this.groupBy = groupBy;
    }

    public Map<String, Integer> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(Map<String, Integer> orderBy) {
        this.orderBy = orderBy;
    }

    public void setLimit(int limitSize, int limitOffset) {
        this.limitSize = limitSize;
        this.limitOffset = limitOffset;
    }

    public int getLimitSize() {
        return limitSize;
    }

    public int getLimitOffset() {
        return limitOffset;
    }

    public void setUsingMaster(boolean usingMaster) {
        this.usingMaster = usingMaster;
    }

    public boolean usingMaster() {
        return usingMaster;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }
}
