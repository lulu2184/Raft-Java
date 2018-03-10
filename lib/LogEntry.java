package lib;

import java.io.Serializable;

public class LogEntry implements Serializable {

    private static final long serialVersionUID = 234158L;
    public int term;
    public int index;
    public int value;

    public LogEntry(int term, int index, int value) {
        this.term = term;
        this.index = index;
        this.value = value;
    }
}
