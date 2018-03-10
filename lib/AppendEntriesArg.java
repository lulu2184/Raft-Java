package lib;

import java.io.Serializable;
import java.util.List;

public class AppendEntriesArg implements Serializable {
    private static final long serialVersionUID = 96872L;

    public int term;
    public int leaderId;
    public int prevLogIndex;
    public int prevLogTerm;
    public List<LogEntry> entries;
    public int leaderCommit;

    public AppendEntriesArg(int term, int leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries,
                            int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
}
