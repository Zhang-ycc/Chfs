#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

using namespace std;

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    int current_term;
    int votedFor;
    vector<log_entry<command>> log;

    void write_to_disk(int &term, int &_votedFor, vector<log_entry<command>> &logs);

private:
    std::mutex mtx;
    string path;
    // Lab3: Your code here
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    path = dir + "/state.bin";
    std::ifstream file(path, std::ios::in | std::ios::binary);
    if (file.is_open()){
        file.read((char *)&current_term, sizeof(int ));
        file.read((char *)&votedFor, sizeof(int ));
//        printf("restore!!! term: %d\n", current_term);

        unsigned int _size;
        file.read((char *)&_size, sizeof(unsigned int ));

        for (int i = 0; i < _size; i++){
            log_entry<command> _log;
            file.read((char *)&_log.term, sizeof(int ));

            int size;
            file.read((char *)&size, sizeof(int ));

            char* buf = new char[size];
            file.read(buf, size);

            _log.cmd.deserialize(buf, size);
            log.push_back(_log);
        }
    } else {
        current_term = 0;
        votedFor = -1;
//        log_entry<command> _log;
//        _log.term = 0;
//        log.push_back(_log);
    }
    file.close();
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

template <typename command>
void raft_storage<command>::write_to_disk(int &term, int &_votedFor, vector<log_entry<command>> &logs) {
    // Lab3: Your code here
    mtx.lock();

    current_term = term;
    votedFor = _votedFor;
    log.clear();
    log.assign(logs.begin(), logs.end());

    std::ofstream file(path, std::ios::out | std::ios::binary);//! always overwrite

//    printf("write to disk!!!!\n");

    file.write((char *)&current_term, sizeof(int ));
    file.write((char *)&votedFor, sizeof(int ));

    unsigned int _size = log.size();
//    printf("log size : %d\n", _size);
    file.write((char *)&_size, sizeof(unsigned int ));

    for (int i = 0; i < _size; i++){
        log_entry<command> _log = log[i];//!!! notice do not type error !!!
        file.write((char *)&_log.term, sizeof(int ));

        int size = _log.cmd.size();
        file.write((char *)&size, sizeof(int ));

        char* buf = new char[size];
        _log.cmd.serialize(buf, size);
        file.write(buf, size);
    }
    file.close();

    mtx.unlock();
}

#endif // raft_storage_h