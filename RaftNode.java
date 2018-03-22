import lib.*;

import java.io.*;
import java.rmi.RemoteException;
import java.util.*;

public class RaftNode implements MessageHandling {
    private static int heartBeatFreq = 100;
    private static int MAX_ELECTION_TIMEOUT = 600;
    private static int MIN_ELECTION_TIMEOUT = 300;
    private static boolean debug = false;
    private static boolean append_debug = false;
    private static boolean count_debug = false;
    private static Random random = new Random();

    private int id;
    private static TransportLib lib;
    private int num_peers;

    private NodeRole currentRole;
    private int currentTerm;
    private Integer votedFor;
    private int electionTimeout;
    private int commitIndex;
    private List<LogEntry> log;
    private Timer electionTimer;
    private Timer heartBeatTimer;

    private int[] nextIndex;
    private int[] matchIndex;

    private boolean hasHeartBeat;

    private List<SendAppendThread> sendThreadList;

    private enum NodeRole {
        Follower, Candidate, Leader
    }

    public RaftNode(int port, int id, int num_peers) {
        this.id = id;
        this.num_peers = num_peers;
        this.log = new ArrayList<>();
        this.currentRole = NodeRole.Follower;
        this.commitIndex = 0;
        this.votedFor = -1;
        this.currentTerm = 0;
        this.hasHeartBeat = false;

        lib = new TransportLib(port, id, this);

        this.electionTimeout =
                random.nextInt((MAX_ELECTION_TIMEOUT - MIN_ELECTION_TIMEOUT)) + MIN_ELECTION_TIMEOUT;
        if (debug)
            System.out.printf("Server %d's election timeout is %d ms.\n", id, electionTimeout);

        /* Set up timer to check heartbeat periodically. */
        this.electionTimer = new Timer();
        this.electionTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (isLeader() || hasHeartBeat) {
                    resetHeartBeat();
                } else {
                    startNewElection();
                }
            }
        }, 0, electionTimeout);
    }

    /*
     *call back.
     */
    @Override
    public synchronized StartReply start(int command) {
        synchronized (log) {
            if (isLeader()) {
                int index = log.size();
                LogEntry entry = new LogEntry(currentTerm, index + 1, command);
                log.add(entry);
                if (append_debug)
                    System.out.printf("Server %d add %s from client, %s at index %d\n",
                            id, entry.toString(), currentRole.toString(), index + 1);

                broadcastAppendEntriesInParallel();
                return new StartReply(index + 1, currentTerm, isLeader());
            } else {
                return new StartReply(getLastEntry().index, currentTerm, isLeader());
            }
        }
    }

    @Override
    public GetStateReply getState() {
        return new GetStateReply(currentTerm, isLeader());
    }

    /**
     * @param message the message this node receives.
     * @return The message this node should reply for the incoming message.
     */
    @Override
    public Message deliverMessage(Message message) {
        if (message.getType().equals(MessageType.RequestVoteArgs)) {
            return responseRequestVoteArgs(message);
        }

        if (message.getType().equals(MessageType.AppendEntriesArgs)) {
            return responseAppendEntriesArgs(message);
        }

        return null;
    }

    private Message responseRequestVoteArgs(Message message) {
        RequestVoteArgs request = (RequestVoteArgs) convertByteArrayToObject(message.getBody());
        if (count_debug) {
            System.out.printf("Server %d receives RequestVote from %d. at term %d\n", id, request.candidateId, currentTerm);
        }
        boolean granted = false;
        if (request.term >= currentTerm) {
            updateTerm(request.term);
            if (debug)
                System.out.printf("Server %d receives RequestVote from %d. at term %d\n", id, request.candidateId, currentTerm);
            synchronized (votedFor) {
                if (votedFor < 0) {
                    LogEntry lastEntry = getLastEntry();
                    if ((request.lastLogTerm == lastEntry.term && request.lastLogIndex >= lastEntry.index)
                            || request.lastLogTerm > lastEntry.term) {
                        granted = true;
                        votedFor = request.candidateId;
                    }
                }
            }
        }
        if (debug && granted)
            System.out.printf("Server %d gives vote to server %d. at term %d\n", id, request.candidateId, currentTerm);
        return new Message(MessageType.RequestVoteReply, id, request.candidateId,
                convertObjectToByteArray(new RequestVoteReply(currentTerm, granted)));
    }

    private Message responseAppendEntriesArgs(Message message) {
        synchronized (log) {
            AppendEntriesArgs appendEntriesArgs = (AppendEntriesArgs) convertByteArrayToObject(message.getBody());
            boolean success;

            /* debug message */
            if (count_debug) {
                if (appendEntriesArgs.entries.size() == 0) {
                    System.out.printf("Server %d receives heartbeat from %d.\n", id, appendEntriesArgs.leaderId);
                } else {
                    System.out.printf("Server %d at term %d receives append entry from %d at term %d for index %d. %s\n",
                            id, currentTerm, appendEntriesArgs.leaderId, appendEntriesArgs.term,
                            appendEntriesArgs.prevLogIndex + 1, currentRole.toString());
                }
            }

            if (appendEntriesArgs.term < currentTerm) {
                success = false;
            } else if (conflictPrevLog(appendEntriesArgs)) {
                success = false;
                receiveHeartBeat();
                updateTerm(appendEntriesArgs.term);
            } else {
                success = true;
                receiveHeartBeat();
                if (isCandidate())
                    changeStateToFollower();
                updateTerm(appendEntriesArgs.term);

                if (!appendEntriesArgs.entries.isEmpty()) {
                    if (append_debug) {
                        System.out.printf("Success on server %d.\n", id);
                        System.out.printf("Server %d at term %d receives append entry from %d at term %d for index %d. %s\n",
                                id, currentTerm, appendEntriesArgs.leaderId, appendEntriesArgs.term, appendEntriesArgs.prevLogIndex + 1,
                                currentRole.toString());
                    }

                    /* If existing entry conflicts with the new one, delete all following entries. */
                    LogEntry lastEntry = getLastEntry();
                    while (lastEntry != null && lastEntry.index > appendEntriesArgs.prevLogIndex) {
                        if (append_debug)
                            System.out.printf("Delete log on index %d at server %d. (prevLogIndex = %d)\n",
                                    lastEntry.index, id, appendEntriesArgs.prevLogIndex);
                        log.remove(log.size() - 1);
                        lastEntry = getLastEntry();
                    }

                    /* Update log */
                    log.addAll(appendEntriesArgs.entries);
                }

                /* Commit logs according to leader's commitIndex. */
                if (appendEntriesArgs.leaderCommit > commitIndex) {
                    int oldCommitIndex = commitIndex;
                    commitIndex = Integer.min(appendEntriesArgs.leaderCommit, log.size());
                    for (int i = oldCommitIndex; i < commitIndex; i++) {
                        try {
                            if (append_debug)
                                System.out.printf("Server %d commits log at %d (value = %d)\n", id, log.get(i).index, log.get(i).value);
                            lib.applyChannel(new ApplyMsg(id, log.get(i).index, log.get(i).value, false, null));
                        } catch (RemoteException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            return new Message(MessageType.AppendEntriesReply, id, message.getSrc(),
                    convertObjectToByteArray(new AppendEntriesReply(currentTerm, success)));
        }
    }

    private boolean conflictPrevLog(AppendEntriesArgs request) {
        return (log.size() < request.prevLogIndex || (log.size() > 0 && log.size() >= request.prevLogIndex
                && request.prevLogIndex > 0
                && log.get(request.prevLogIndex - 1).term != request.prevLogTerm));
    }

    private void updateTerm(int term) {
        synchronized (votedFor) {
            if (term > currentTerm) {
                changeStateToFollower();
                currentTerm = term;
                votedFor = -1;
            }
        }
    }

    private void resetHeartBeat() {
        hasHeartBeat = false;
    }

    private void receiveHeartBeat() {
        hasHeartBeat = true;
    }

    private LogEntry getLastEntry() {
        if (log ==null || log.size() == 0) {
            return new LogEntry(0, 0, 0);
        } else {
            return log.get(log.size() - 1);
        }
    }

    private boolean isCandidate() {
        return currentRole.equals(NodeRole.Candidate);
    }

    private boolean isLeader() {
        return currentRole.equals(NodeRole.Leader);
    }

    private void becomeNewLeader() {
        currentRole = NodeRole.Leader;
        if (debug || count_debug)
            System.out.printf("Server %d becomes a leader at term %d.", id, currentTerm);

        /* Set up heart beat timer. */
        heartBeatTimer = new Timer();
        heartBeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                broadcastHeartBeat();
            }
        }, 0, heartBeatFreq);

        /* Re-initialize send thread list. */
        sendThreadList = new ArrayList<>();

        /* Reinitialize nextIndex and matchIndex */
        nextIndex = new int[num_peers];
        matchIndex = new int[num_peers];
        for (int i = 0; i < num_peers; i++) {
            nextIndex[i] = getLastEntry().index + 1;
            matchIndex[i] = 0;
        }
    }

    private void changeStateToFollower() {
        if (isLeader() && this.heartBeatTimer != null) {
            this.heartBeatTimer.cancel();
            this.heartBeatTimer = null;
        }
        currentRole = NodeRole.Follower;
        if (sendThreadList != null) {
            for (SendAppendThread thread : sendThreadList) {
                thread.stopThread();
            }
        }
        this.sendThreadList = new ArrayList<>();
    }

    /**
     * Send heartbeat to all the followers.
     */
    private void broadcastHeartBeat() {
        LogEntry lastLogEntry = getLastEntry();
        AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(currentTerm, id, lastLogEntry.index,
                lastLogEntry.term, Arrays.asList(), commitIndex);
        byte[] messageBody = convertObjectToByteArray(appendEntriesArgs);
        for (int i = 0; i < num_peers; i++) {
            if (i == id) continue;
            try {
                Message reply = lib.sendMessage(new Message(MessageType.AppendEntriesArgs, id, i, messageBody));
                if (reply != null) {
                    AppendEntriesReply appendReply = (AppendEntriesReply) convertByteArrayToObject(reply.getBody());
                    if (!appendReply.success)
                        updateTerm(appendReply.term);
                }
            } catch (RemoteException e) {
                System.err.printf("Server %d fails to send AppendEntries to server %d.\n", id, i);
            }
        }
    }

    /**
     * Send AppendEntry message to all the followers in parallel.
     */
    private void broadcastAppendEntriesInParallel() {
        int currentIndex = getLastEntry().index;
        SuccessListener successListener = new SuccessListener(currentIndex);
        List<LogEntry> logCopy = new ArrayList<>(log);

        /* Stop sending thread for preceding logs. */
        for (SendAppendThread thread : sendThreadList) {
            thread.stopThread();
        }
        sendThreadList = new ArrayList<>();

        /* Start a new sending thread for each follower. */
        for (int i = 0; i < num_peers; i++) {
            if (i == id) continue;
            SendAppendThread sendThread = new SendAppendThread(i, currentIndex, successListener, logCopy);
            (new Thread(sendThread)).start();
            sendThreadList.add(sendThread);
        }
        long waitStartTime = System.currentTimeMillis();

        /* Blocking until the log is committed or timeout. */
        while (!successListener.isCommitted() && (System.currentTimeMillis() - waitStartTime) < MAX_ELECTION_TIMEOUT);
        if (append_debug)
            System.out.printf("Server %d(Leader) committed.\n", id);
        if (!successListener.isCommitted()) {
            for (SendAppendThread thread : sendThreadList) {
                thread.stopThread();
            }
            sendThreadList = new ArrayList<>();
        }
    }

    private void startNewElection() {
        currentTerm++;
        if (debug)
            System.out.printf("Server %d starts a new election at term %d (now: %s).\n", id, currentTerm, currentRole);
        synchronized (votedFor) {
            votedFor = id;
            currentRole = NodeRole.Candidate;
        }

        // send out requestVote msg
        LogEntry lastLogEntry = getLastEntry();
        RequestVoteArgs requestVoteArgs = new RequestVoteArgs(currentTerm, id, lastLogEntry.index, lastLogEntry.term);
        byte[] payload = convertObjectToByteArray(requestVoteArgs);

        // broadcast requestVote request
        int votesCounter = 1;
        for (int i = 0; i < num_peers; i++) {
            if (i == id) continue;
            if (!isCandidate()) break;
            try {
                Message response = lib.sendMessage(new Message(MessageType.RequestVoteArgs, id, i, payload));
                if (response != null) {
                    RequestVoteReply reply = (RequestVoteReply) convertByteArrayToObject(response.getBody());
                    if (reply.term > currentTerm) {
                        updateTerm(reply.term);
                        break;
                    }
                    if (reply.voteGranted) votesCounter++;
                }
            } catch (RemoteException e){
                System.err.printf("Server %d fails to send RequestVote to server %d at term %d.\n", id, i, currentTerm);
            } catch (ClassCastException e) {
                e.printStackTrace();
            }
        }

        if (debug)
            System.out.printf("Sever %d receives %d votes at term %d.\n", id, votesCounter, currentTerm);
        if (isCandidate() && votesCounter + votesCounter > num_peers) {
            becomeNewLeader();
        }
    }

    /**
     * A helper function to convert an Object to a byte array for serializing an object.
     */
    private static byte[] convertObjectToByteArray(Object object) {
        ByteArrayOutputStream  byteStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(byteStream);
            outputStream.writeObject(object);
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteStream.toByteArray();
    }

    /**
     * A helper function to convert a byte array to an Object for de-serializing an object.
     */
    private static Object convertByteArrayToObject(byte[] bytes) {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        Object object = null;
        try {
            ObjectInputStream inputStream = new ObjectInputStream(byteStream);
            object = inputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }

    //main function
    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        //new usernode
        try {
            RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        } catch (Exception | Error e) {
            e.printStackTrace();
        }
    }

    /**
     * Thread for sending AppendEntry messages to one follower.
     * If the follower is down, or leader cannot get response, the leader would try endlessly.
     * If the append message is conflict with the follower's log, decrease nextIndex by 1 and continue to try.
     */
    private class SendAppendThread implements Runnable {
        private int followerId;
        private int currentIndex;
        private boolean isStopped;
        private SuccessListener successListener;
        private int counter = 0;
        private List<LogEntry> logCopy;

        private SendAppendThread(int followerId, int currentIndex, SuccessListener successListener,
                                 List<LogEntry> logCopy) {
            this.followerId = followerId;
            this.currentIndex = currentIndex;
            this.isStopped = false;
            this.successListener = successListener;
            this.logCopy = logCopy;
        }

        @Override
        public void run() {
            nextIndex[this.followerId] = Integer.min(nextIndex[this.followerId], currentIndex);
            while (!this.isStopped) {
                List<LogEntry> entries = new ArrayList<>();
                entries.addAll(logCopy.subList(Integer.max(0, nextIndex[this.followerId] - 1), currentIndex));
                int prevLogTerm = (nextIndex[this.followerId] >= 2) ? logCopy.get(nextIndex[this.followerId] - 2).term : 0;
                AppendEntriesArgs appendEntriesArgs = new AppendEntriesArgs(currentTerm, id,
                        nextIndex[this.followerId] - 1, prevLogTerm, entries, commitIndex);
                Message message = new Message(
                        MessageType.AppendEntriesArgs, id, followerId, convertObjectToByteArray(appendEntriesArgs));
                try {
                    Message response = lib.sendMessage(message);
                    if (response != null) {
                        AppendEntriesReply reply = (AppendEntriesReply) convertByteArrayToObject(response.getBody());
                        if (reply.success) {
                            if (append_debug)
                                System.out.printf("Server %d receives success from server %d at term %d (%d times).\n",
                                        id, this.followerId, reply.term, ++counter);
                            successListener.onSuccess();
                            nextIndex[this.followerId]++;
                            return;
                        } else {
                            if (reply.term > currentTerm) {
                                updateTerm(reply.term);
                            } else {
                                nextIndex[this.followerId]--;
                            }
                        }
                    }
                } catch (RemoteException e) {
                    /* Ignore, and continue trying. */
                    e.printStackTrace();
                }
            }
        }

        private void stopThread() {
            this.isStopped = true;
        }
    }

    /**
     * Listener for AppendEntry success response.
     * Count the number of success replies, commit the corresponding entry if reach an agreement.
     */
    private class SuccessListener {
        private int index;
        private int successCounter = 1;
        private boolean isCommitted;
        private int prevCommit;

        private SuccessListener(int index) {
            this.index = index;
            this.isCommitted = false;
            this.prevCommit = commitIndex;
        }

        private void onSuccess() {
            synchronized (this) {
                if (isCommitted) return;
                successCounter++;
                if (successCounter > num_peers / 2) {
                    commitIndex = index;
                    for (int i = prevCommit; i < commitIndex; ++i) {
                        try {
                            lib.applyChannel(new ApplyMsg(id, log.get(i).index,
                                    log.get(i).value, false, null));
                            if (append_debug)
                                System.out.printf("Server %d commits log at %d (value = %d)[Leader]\n", id, log.get(i).index, log.get(i).value);
                        } catch (RemoteException e) {
                            e.printStackTrace();
                        }
                    }
                    isCommitted = true;
                }
            }
        }

        private Boolean isCommitted() {
            synchronized (this) {
                return isCommitted;
            }
        }
    }
}
