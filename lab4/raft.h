#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <vector>
#include <random>
#include <set>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

using namespace std;

template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do {                       \
    } while (0);

//     #define RAFT_LOG(fmt, args...)                                                                                   \
//     do {                                                                                                         \
//         auto now =                                                                                               \
//             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
//                 std::chrono::system_clock::now().time_since_epoch())                                             \
//                 .count();                                                                                        \
//         printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
//     } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    int votedFor;
    int commitIndex;
    int lastApplied;
    std::chrono::system_clock::time_point lastResponseTime;

    std::vector<int> nextIndex;
    std::vector<int> matchIndex;
    std::set<int> followers;

    std::vector<log_entry<command>> log;

    //snapshot part4
    int lastIncludedIndex;
    vector<char> snapshotData;

    /* ----Persistent state on all server----  */

    /* ---- Volatile state on all server----  */

    /* ---- Volatile state on leader----  */

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    void start_new_election();
    void start_new_term(int term);
    int get_log_term(int _index);
    void write_to_disk();

};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    followers.clear();
    lastResponseTime = chrono::system_clock::now();
    commitIndex = 0;
    lastApplied = 0;

    //persistence part3
    current_term = storage->current_term;
    log.clear();
    log.assign(storage->log.begin(), storage->log.end());
    votedFor = storage->votedFor;

}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    printf("RAFT : new_command\n");
    mtx.lock();
    if (role == leader) {
        log_entry<command> _log;
        _log.cmd = cmd;
        _log.term = current_term;

        log.push_back(_log);
        write_to_disk();

        // 1 to n
        index = log.size();
        term = current_term;

        mtx.unlock();
        return true;
    }
    mtx.unlock();
    return false;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();

    if (args.term >= current_term){
        // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if (args.term > current_term) {
//            RAFT_LOG("become follower");
            role = follower;
            followers.clear();
            start_new_term(args.term);
        }

        // If votedFor is null or candidateId,
        //only vote once
        if (votedFor == -1 || votedFor == args.candidateId) {
            int _index = log.size();
            auto log_term = get_log_term(_index);

            // and candidate’s log is at least as up-to-date as receiver’s log, grant vote
            if (log_term < args.lastLogTerm || (log_term == args.lastLogTerm && _index <= args.lastLogIndex)) {
                //receive
//                RAFT_LOG("vote for %d", args.candidateId);
                reply.voteGranted = true;

                votedFor = args.candidateId;
                write_to_disk();

                goto release;
            }
        }
    }

    //reject
    reply.voteGranted = false;

    release:
    reply.term = current_term;
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();

    if (reply.term > current_term){
//        RAFT_LOG("become follower");
        role = follower;
        followers.clear();
        //begin new term
        start_new_term(reply.term);
        goto release;
    }

    if (role == candidate){
        if (reply.voteGranted) {
//            RAFT_LOG("get voted from %d", target);
            followers.insert(target);
        }

        if ((int) followers.size() > num_nodes() / 2){
            RAFT_LOG("become leader");
            role = leader;
            followers.clear();
            // initialize the volatile state
            nextIndex.resize(num_nodes(), log.size() + 1);
            matchIndex.resize(num_nodes(), 0);
        }
    }

    release:
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    mtx.lock();

    //if useless, reject
    if (arg.term < current_term){
        reply.success = false;
        goto release;
    }

    //ping to self
    if (arg.leaderId == my_id){//! why role == leader fail ???
        reply.success = true;
        goto release;
    }

    //out of date
    if (arg.term > current_term){
//        RAFT_LOG("become follower");
        role = follower;
        followers.clear();
        start_new_term(arg.term);
    }

    //ping
    if (arg.entries.empty()){
        RAFT_LOG("receive ping");
        if (role == candidate){
            // leader has been elected
            role = follower;
            followers.clear();
        }
        lastResponseTime = chrono::system_clock::now();

        //sync term and log with leader, commit
        if (get_log_term(arg.prevLogIndex) == arg.prevLogTerm && arg.prevLogIndex == log.size()) {
            if (arg.leaderCommit > commitIndex){
                commitIndex = min(arg.leaderCommit, (int)log.size());
//                RAFT_LOG("commit index %d", commitIndex);
            }
        }
        goto release;
    }

    //Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm
    //not match
    if (get_log_term(arg.prevLogIndex) != arg.prevLogTerm || log.size() < arg.prevLogIndex){
        reply.success = false;
        goto release;
    }

    //match
    //If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    //force to overwrite
    log.erase(log.begin() + arg.prevLogIndex, log.end());//! if safe ? in 5.4 explain how
    //append
    log.insert(log.end(), arg.entries.begin(), arg.entries.end());
    write_to_disk();

    if (arg.leaderCommit > commitIndex){
        commitIndex = min(arg.leaderCommit, (int)log.size());
    }

    reply.success = true;

//    RAFT_LOG("append ok")

    release:
    reply.term = current_term;
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here

    mtx.lock();
    vector<int> tmp;

    if (role != leader){
        goto release;
    }

    if (arg.entries.empty()){
        goto release;
    }

    if (!reply.success){
        if (reply.term > current_term){
//            RAFT_LOG("become follower");
            role = follower;
            followers.clear();
            start_new_term(reply.term);
        }
        else {
            //If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
            //roll back
            if (nextIndex[node] > 1) {//! important: index start from 1
                nextIndex[node]--;
            }
            assert(nextIndex[node] >= 1);
        }
        goto release;
    }

    //If successful: update nextIndex and matchIndex for follower
    matchIndex[node] = arg.prevLogIndex + arg.entries.size();
    nextIndex[node] = matchIndex[node] + 1;

    //a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm, set commitIndex = N
    //! may be some bugs here
    tmp.assign(matchIndex.begin(), matchIndex.end());
    sort(tmp.begin(), tmp.end());
    commitIndex = tmp[tmp.size() / 2];

//    RAFT_LOG("handle repy ok");

    release:
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
//    mtx.lock();
//
//    if (args.term < current_term || role == leader){
//        reply.term = current_term;
//        goto release;
//    }
//
//    if (args.lastIncludedIndex <= lastIncludedIndex){
//        goto release;
//    }
//
//    if (args.term > current_term){
//        role = follower;
//        start_new_term(args.term);
//    }
//
//    snapshotData = args.data;
//
//    if (args.lastIncludedIndex < (int)log.size() && args.term == get_log_term(log.size())){
//        //delete duplicate
//        log.erase(log.begin(), log.begin() + args.lastIncludedIndex);
//
//        if (lastApplied < args.lastIncludedIndex || commitIndex < args.lastIncludedIndex){
//            commitIndex = args.lastIncludedTerm;
//            lastApplied = args.lastIncludedIndex + 1;
//            state-apply_snapshot(args.data);
//        }
//
//        lastIncludedIndex = args.lastIncludedIndex;
//        write_to_disk();
//    } else{
//        log.clear();
//        commitIndex = args.lastIncludedIndex;
//        lastApplied = args.lastIncludedIndex + 1;
//
//    }
//
//    release:
//    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
//        RAFT_LOG("vote RPC fail");
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
//        RAFT_LOG("entry RPC fail");
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.


    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        mtx.lock();
        if (role == follower || role == candidate){
            std::random_device rd;
            std::default_random_engine eng(rd());

            std::uniform_real_distribution<> u(300, 500);//! try many times
            int timeout = u(eng);

//            RAFT_LOG("timeout: %d", timeout);


            if (chrono::system_clock::now() - lastResponseTime > chrono::milliseconds(timeout)){
                //start election
                RAFT_LOG("time out");
                role = candidate;
                followers.clear();
                start_new_term(current_term + 1);
                start_new_election();
            }

        }
        mtx.unlock();
        this_thread::sleep_for(chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.


    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        mtx.lock();
        if (role == leader) {
            for (int i = 0; i < num_nodes(); i++){
                // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
                if (log.size() >= nextIndex[i]){
                    //update a lag behind client
                    append_entries_args<command> args;
                    args.term = current_term;
                    args.leaderId = my_id;
                    args.prevLogIndex = nextIndex[i] - 1;
                    args.prevLogTerm = get_log_term(args.prevLogIndex);
                    args.entries.assign(log.begin() + args.prevLogIndex, log.end());
                    args.leaderCommit = commitIndex;

                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                }
            }
        }
        mtx.unlock();
        this_thread::sleep_for(chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.


    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        mtx.lock();
        // If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
        if (commitIndex > lastApplied){
            while (lastApplied < commitIndex) {
                RAFT_LOG("apply");
                printf("RAFT : apply\n");
                state->apply_log(log[lastApplied ++].cmd);
            }
        }
        mtx.unlock();
        this_thread::sleep_for(chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:

        mtx.lock();
        if (role == leader) {
            for (int i = 0; i < num_nodes(); i++) {
                append_entries_args<command> args;
                args.term = current_term;
                args.leaderId = my_id;
                args.prevLogIndex = nextIndex[i] - 1;
                args.prevLogTerm = get_log_term(args.prevLogIndex);
                args.leaderCommit = commitIndex;
                args.entries = std::vector<log_entry<command>>(0);// empty entries -> ping

                RAFT_LOG("ping to %d", i);
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
            }
        }
        mtx.unlock();

        this_thread::sleep_for(chrono::milliseconds(150));//! too small -> rpc_count over; too big -> always timeout
    }


    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::start_new_election() {
//    RAFT_LOG("start new election");

    request_vote_args args;
    args.term = current_term;
    args.candidateId = my_id;
    args.lastLogIndex = log.size();
    args.lastLogTerm = get_log_term(args.lastLogIndex);

    for (int i = 0; i < num_nodes(); i++){
//        RAFT_LOG("tell election message to %d", i);
        thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start_new_term(int term) {
//    RAFT_LOG("start new term: %d", term);

    current_term = term;
    votedFor = -1;//! remember to reinitialize! else no leader
    write_to_disk();

    followers.clear();

    //Reset election timer
    lastResponseTime = chrono::system_clock::now();//! forget at the begin....qwq
}

template <typename state_machine, typename command>
int raft<state_machine, command>::get_log_term(int index) {
    return index == 0 ? 0 : log[index - 1].term;//! notice seg fault
}

template <typename state_machine, typename command>
void raft<state_machine, command>::write_to_disk() {
    storage->write_to_disk(current_term, votedFor, log);
}

#endif // raft_h