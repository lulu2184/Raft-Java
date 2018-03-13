package lib;

import java.io.Serializable;
import java.util.Comparator;

public class LogEntry implements Serializable{

    private static final long serialVersionUID = 234158L;
    public int term;
    public int index;
    public int value;

    public LogEntry(int term, int index, int value) {
        this.term = term;
        this.index = index;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry entry = (LogEntry) o;
        int termc = Integer.compare(this.term, entry.term);
        int indexc = Integer.compare(this.index, entry.index);
        int valuec = Integer.compare(this.value, entry.value);
        return (termc == 0 && indexc == 0 && valuec == 0);
    }

    @Override
    public int hashCode() {
        int hash = 0;
        hash += Integer.hashCode(term);
        hash += Integer.hashCode(index);
        hash += Integer.hashCode(value);
        return hash;
    }

    public String toString() {
        return "[term=" + term + ",value=" + value + ",index=" + index + "]";
    }
}
