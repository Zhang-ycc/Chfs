#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/sendfile.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "rpc.h"
#include "extent_protocol.h"

#define MAX_LOG_SZ 131072

using namespace std;

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_GET,
        CMD_GETATTR,
        CMD_REMOVE,
    };

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    unsigned long long ec_id;
    string param;

    // constructor override
    chfs_command(txid_t n, cmd_type t, extent_protocol::extentid_t i, string &p) {
        id = n;
        type = t;
        ec_id = i;
        param = p;
    }

    chfs_command() { }


    uint64_t size() const {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t) + sizeof(ec_id) + param.size();
        return s;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const command& log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();

    std::vector<command> get_log_entries();
    uint64_t log_size = 0;

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
};

template<typename command>
persister<command>::persister(const std::string& dir){
    printf("start\n");
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A
}

template<typename command>
void persister<command>::append_log(const command& log) {
    // Your code here for lab2A

    std::ofstream file;
    file.open(file_path_logfile, std::ios::app|std::ios::binary);
//    if (file.is_open()){
//        printf("ok!\n");
//    }

    //store size of string
    uint64_t size = log.param.size();
    file.write((char *)&size, sizeof(uint64_t));
    //store tid
    file.write((char *)&log.id, sizeof(unsigned long long));
    //store command type
    file.write((char *)&log.type, sizeof(int));
    //store eid
    file.write((char *)&log.ec_id, sizeof(unsigned long long));
    //store string (if exist)
    if (size > 0){
//            for (uint64_t i = 0; i < size; i++)
//                file.write(&log.param[i], sizeof(char ));
        file.write(&log.param[0],size);
    }
    file.close();

//    std::ofstream file2;
//    file2.open("test/log.txt", std::ios::app);
//    file2 << size << " " << log.id << " "<< log.type << " " <<log.ec_id <<" "<< log.param.c_str();
//    //file2 << size << " " << log.param;
//    file2 << endl;
//
//    file2.close();

    log_size += log.size();

}

template<typename command>
void persister<command>::checkpoint() {
    if (log_size <= MAX_LOG_SZ){
        return;
    }

//    std::ofstream file2;
//    file2.open("test/log.txt", std::ios::app);
//    //file2 << size << " " << log.id << " "<< log.type << " " <<log.ec_id <<" "<< log.param.c_str();
//    file2 << "xxxxxxxxx\n";
//    file2.close();

    std::ifstream log_file(file_path_logfile, ios::in|ios::binary);
    std::ofstream check_file(file_path_checkpoint, ios::app | ios::binary);

    while (!log_file.eof()){
        uint64_t size = 0;
        unsigned long long id;
        chfs_command::cmd_type type;
        unsigned long long ec_id;
        string param;

        log_file.read((char *)&size, sizeof(uint64_t));
        log_file.read((char *)&id, sizeof(unsigned long long));
        log_file.read((char *)&type, sizeof(int));
        log_file.read((char *)&ec_id, sizeof(unsigned long long));

        if (size > 0) {
            param.resize(size);
            log_file.read(&param[0], size);
        }

//        std::ofstream file2;
//        file2.open("test/log2.txt", std::ios::app);
//        file2 << size << " " <<  id << " " << type  << " " << ec_id << " " << param.c_str() << endl;
//        //file2 << size << " " << param.size() << endl;
//        file2.close();

        check_file.write((char *)&size, sizeof(uint64_t));
        check_file.write((char *)&id, sizeof(unsigned long long));
        check_file.write((char *)&type, sizeof(int));
        check_file.write((char *)&ec_id, sizeof(unsigned long long));
        if (size > 0){
            check_file.write(&param[0],size);
//            for (uint64_t i = 0; i < size; i++)
//                check_file.write(&param[i], sizeof(char ));
        }
    }

    log_file.close();
    check_file.close();

    FILE *file = NULL;
    file = fopen(file_path_logfile.c_str(), "w+");
    fclose(file);
    log_size = 0;
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A

    std::ifstream file(file_path_logfile, std::ios::in|std::ios::binary);
    if (!file)
        return;

    //read to the end
    while (!file.eof()){
        uint64_t size = 0;
        unsigned long long id;
        chfs_command::cmd_type type;
        unsigned long long ec_id;
        string param;

        file.read((char *)&size, sizeof(uint64_t));

        //param.reserve(size);

        file.read((char *)&id, sizeof(unsigned long long));
        file.read((char *)&type, sizeof(int));
        file.read((char *)&ec_id, sizeof(unsigned long long));



        command res(id, type, ec_id, param);

        if (size > 0) {
//            for (uint64_t i = 0; i < size; i++) {
//                char c;
//                file.read(&c, sizeof(char ));
//                res.param += c;
//            }
            res.param.resize(size);
            file.read(&res.param[0], size);
        }
//
//        std::ofstream file2;
//        file2.open("test/logfile.txt", std::ios::app);
//        file2 << size << " " <<  id << " " << type  << " " << ec_id << " " << res.param.c_str() << endl;
//       // file2 << size << " " << param.size() << endl;
//        file2.close();

        log_entries.push_back(res);
        log_size += res.size();

    }
    file.close();
};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A

    std::ifstream file(file_path_checkpoint, std::ios::in|std::ios::binary);
    if (!file)
        return;

    //read to the end
    while (!file.eof()){
        uint64_t size = 0;
        unsigned long long id;
        chfs_command::cmd_type type;
        unsigned long long ec_id;
        string param;

        file.read((char *)&size, sizeof(uint64_t));

        file.read((char *)&id, sizeof(unsigned long long));
        file.read((char *)&type, sizeof(int));
        file.read((char *)&ec_id, sizeof(unsigned long long));

        command res(id, type, ec_id, param);


        if (size > 0) {
            res.param.resize(size);
            file.read(&res.param[0], size);
            //printf(res.param.c_str());
        }
        log_entries.push_back(res);

//        std::ofstream file2;
//        file2.open("test/checkpoint.txt", std::ios::app);
//        file2 << size << " " << id << " "<< type << " " <<ec_id <<" "<< res.param.c_str();
//        file2 << endl;
//        file2.close();

    }
    file.close();
};

template<typename command>
std::vector<command> persister<command>::get_log_entries(){
    return log_entries;
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h