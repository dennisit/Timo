package fm.liu.timo.config.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author liuhuanting
 */
public class AutoIncrement {
    private String              column;
    private long                init;
    private long                max;
    private volatile AtomicLong current;

    public AutoIncrement(String column, long init, long max) {
        this.column = column;
        this.init = init;
        this.max = max;
        this.current = new AtomicLong(init);
    }

    public String getColumn() {
        return column;
    }

    public long getInit() {
        return init;
    }

    public long getMax() {
        return max;
    }

    public AtomicLong current() {
        return current;
    }
}
