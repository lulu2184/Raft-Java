import lib.*;

import javax.xml.soap.Node;
import java.io.*;
import java.rmi.RemoteException;
import java.util.*;

public class RaftNode implements MessageHandling {
    private static int heartBeatFreq = 10;
    private static boolean debug = false;

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

    private boolean hasHeartBeat;

    private enum NodeRole {
        Follower, Candidate, Leader
    }

    public RaftNode(int port, int id, int num_peers) {
        this.id = id;
        this.num_peers = num_peers;
        this.log = new ArrayList<>();
        this.currentRole = NodeRole.Follower;
        this.commitIndex = 0;
        this.votedFor = null;
        this.currentTerm = 0;
        this.hasHeartBeat = false;

        lib = new TransportLib(port, id, this);

        this.electionTimeout = (new Random()).nextInt(150) + 150;

        this.electionTimer = new Timer();
        this.electionTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (debug)
                    System.out.printf("Server %d check heartbeat (%s)\n", id, currentRole.toString());
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
        //return new StartReply(getLastEntry().index, currentTerm, isLeader());
        if (currentRole.equals(NodeRole.Leader)) {
            int index = log.size();
            LogEntry entry = new LogEntry(currentTerm, index, command);
            log.add(entry);
            System.out.printf("Server %d add %s from client, %s\n", id, entry.toString(), currentRole.toString());
            System.out.printf("%d come from leader %d at term %d\n", command, id, currentTerm);
            broadcastAppendEntriesExceptHeartBeats();
            broadcastAppendEntriesExceptHeartBeats();

            try {
                lib.applyChannel(new ApplyMsg(id, Math.max(commitIndex, 0), log.get(Math.max(commitIndex-1,0)).value, false, null));
                System.out.printf("Server %d commits log at %d\n", id, Math.max(commitIndex-1, 0));
            } catch (RemoteException e) {
                e.printStackTrace();
            }

            return new StartReply(Math.max(commitIndex,0), currentTerm, isLeader());
        }
        return new StartReply(getLastEntry().index, currentTerm, isLeader());
    }

    public void broadcastAppendEntriesExceptHeartBeats() {
        List<LogEntry> appendEntries = new ArrayList<>();
        for (int i = 0; i < commitIndex; ++i) {
            appendEntries.add(log.get(i));
            System.out.println("current commitIndex: " + commitIndex);
            System.out.println("append entry: " + log.get(i));
        }
        if (appendEntries.size() == 0) {
            appendEntries.add(new LogEntry(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE));
        }
        broadcastAppendEntries(appendEntries);
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
            RequestVoteArgs request = (RequestVoteArgs) convertByteArrayToObject(message.getBody());
            boolean granted = false;
            if (debug)
                System.out.printf("Server %d receives RequestVote from %d. at term %d\n", id, request.candidateId, currentTerm);
            if (request.term >= currentTerm) {
                updateTerm(request.term);
                if (votedFor == null || votedFor.equals(request.candidateId)) {
                    // TODO: check whether the candidate is more up-to-date
                    if (request.term == currentTerm && request.lastLogIndex >= log.size()-1) {
                        granted = true;
                    } else {
                        if (request.term > currentTerm)
                            granted = true;
                    }
                }
            }
            return new Message(MessageType.RequestVoteReply, id, request.candidateId,
                    convertObjectToByteArray(new RequestVoteReply(currentTerm, granted)));
        }

        if (message.getType().equals(MessageType.AppendEntriesArgs)) {
            AppendEntriesArg request = (AppendEntriesArg) convertByteArrayToObject(message.getBody());
            if (debug && request.entries.size() == 0)
                System.out.printf("Server %d receives heartbeat from %d.\n", id, request.leaderId);
            boolean success = false;
            if (request.entries.size() == 0 && request.prevLogTerm != -1) {
                // handle heartbeat
                if (request.term >= currentTerm) {
                    this.hasHeartBeat = true;
                    if (isLeader() && this.heartBeatTimer != null) {
                        this.heartBeatTimer.cancel();
                        this.heartBeatTimer = null;
                    }
                    currentRole = NodeRole.Follower;

                    updateTerm(request.term);
                    success = true;
                }
            } else {
                // handle append entry
                System.out.printf("Server %d receives append entry from %d.\n", id, request.leaderId);
                success = true;
                if (request.term < currentTerm)
                    return new Message(MessageType.AppendEntriesReply, id, request.leaderId,
                            convertObjectToByteArray(new AppendEntriesReply(currentTerm, false)));

                // conflict log
                if (log.size()-1 >= request.prevLogIndex
                        && log.size() > 0
                        && request.prevLogIndex != -1
                        && (log.get(request.prevLogIndex).term != request.prevLogTerm)) {
                    // delete all the following entries
                    // TODO: next index
                    int length = log.size();
                    for (int i = length-1; i >= request.prevLogIndex; --i) {
                        log.remove(i);
                    }
                    System.out.printf("Conflict logs at index %d\n", request.prevLogIndex);
                    // return false
//                    return new Message(MessageType.AppendEntriesReply, id, request.leaderId,
//                            convertObjectToByteArray(new AppendEntriesReply(currentTerm, success)));
                }

                // append new entries
                if (request.entries.get(0).index != Integer.MAX_VALUE) {
                    for (LogEntry entry: request.entries) {
                        if (!log.contains(entry)) {
                            System.out.printf("Server %d appends log entry %d %d from leader %d\n", id, entry.term, entry.index, request.leaderId);
                            log.add(entry);
                        } else {
                            System.out.printf("Server %d has this entry at index %d\n", id, log.indexOf(entry));
                        }
                    }
                    // refresh commit index
                    if (request.leaderCommit > commitIndex) {
                        commitIndex = Integer.min(request.leaderCommit, log.size());
                    }
                    try {
                        for (LogEntry entry: log) {
                            if (entry.index < commitIndex) {
                                lib.applyChannel(new ApplyMsg(id, entry.index+1, entry.value, false, null));
                                System.out.printf("Server %d commits log at %d\n", id, entry.index);
                            }
                        }

                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Empty entries");
                }

                if (success)
                    System.out.printf("Server %d approves leader %d.\n", id, request.leaderId);
                else
                    System.out.printf("Server %d does not approve leader %d.\n", id, request.leaderId);


//                if (request.entries.size() > 0) {
//                    // do not have the index
//                    if (log.size()-1 < request.prevLogIndex) {
//                        success = false;
//                    } else {
//
//                    }
//
//                    if (success)
//                        System.out.printf("Server %d approves leader %d.\n", id, request.leaderId);
//                    else
//                        System.out.printf("Server %d does not approve leader %d.\n", id, request.leaderId);
//                    // refresh commit index
//                    if (request.leaderCommit > commitIndex) {
//                        commitIndex = Integer.min(request.leaderCommit, log.size());
//                    }
//                }
            }

            return new Message(MessageType.AppendEntriesReply, id, request.leaderId,
                    convertObjectToByteArray(new AppendEntriesReply(currentTerm, success)));
        }

        return null;
    }

    private void updateTerm(int term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
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

    private LogEntry getPrevLogEntry() {
        if (log == null || log.size() <= 1) {
            return new LogEntry(0, -1, -1);
        } else {
            return log.get(log.size() - 2);
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
        if (debug)
            System.out.printf("Server %d becomes a leader at term %d.", id, currentTerm);

        heartBeatTimer = new Timer();
        heartBeatTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                broadcastAppendEntries(Arrays.asList());
            }
        }, 0, heartBeatFreq);
    }

    private void broadcastAppendEntries(List<LogEntry> entries) {
        LogEntry prevLogEntry = getPrevLogEntry();
        AppendEntriesArg appendEntriesArg = new AppendEntriesArg(currentTerm, id, prevLogEntry.index,
                prevLogEntry.term, entries, commitIndex);
        byte[] messageBody = convertObjectToByteArray(appendEntriesArg);
        int votes = 0;
        for (int i = 0; i < num_peers; i++) {
            if (i == id) continue;
            try {
                // handle reply message
                Message reply = lib.sendMessage(new Message(MessageType.AppendEntriesArgs, id, i, messageBody));
                if (entries.size() > 0) {
                    if (reply.getType().equals(MessageType.AppendEntriesReply)) {
                        AppendEntriesReply aer = (AppendEntriesReply) convertByteArrayToObject(reply.getBody());
                        if (aer.term == currentTerm && aer.success) {
                            votes ++;
                        }
                    }
                }
            } catch (RemoteException e) {
                System.err.printf("Server %d fails to send AppendEntries to server %d.\n", id, i);
            }
        }
        if (votes > num_peers/2) {
            commitIndex = log.size();
            System.out.printf("Leader %d is approved by majority at term %d\n", id, currentTerm);
            if(log.size() < commitIndex) {
                System.out.println("term: " + currentTerm + " " + log.size() + " " + commitIndex + " " + currentRole.toString());
            }
        }

    }

    private void startNewElection() {
        currentTerm++;
        if (debug)
            System.out.printf("Server %d starts a new election at term %d (now: %s).\n", id, currentTerm, currentRole);
        votedFor = id;
        currentRole = NodeRole.Candidate;

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
}
