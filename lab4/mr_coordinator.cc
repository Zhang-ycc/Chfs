#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string>
#include <vector>
#include <mutex>

#include "mr_protocol.h"
#include "rpc.h"

using namespace std;

struct Task {
	int taskType;     // should be either Mapper or Reducer
	bool isAssigned;  // has been assigned to a worker
	bool isCompleted; // has been finised by a worker
	int index;        // index to the file
    chrono::system_clock::time_point startTime;
};

class Coordinator {
public:
	Coordinator(const vector<string> &files, int nReduce);
	mr_protocol::status askTask(int, mr_protocol::AskTaskResponse &reply);
	mr_protocol::status submitTask(int taskType, int index, bool &success);
	bool isFinishedMap();
	bool isFinishedReduce();
	bool Done();
    bool assignTask(Task &task);

private:
	vector<string> files;
	vector<Task> mapTasks;
	vector<Task> reduceTasks;

	mutex mtx;

	long completedMapCount;
	long completedReduceCount;
	bool isFinished;


	string getFile(int index);
};


// Your code here -- RPC handlers for the worker to call.

mr_protocol::status Coordinator::askTask(int, mr_protocol::AskTaskResponse &reply) {
	// Lab4 : Your code goes here.
    Task task;
    if (assignTask(task)){
        reply.taskType = (mr_tasktype)task.taskType;
        reply.index = task.index;
        reply.filename = getFile(task.index);
        reply.file_num = files.size();
    } else {
        reply.taskType = NONE;
    }
	return mr_protocol::OK;
}

mr_protocol::status Coordinator::submitTask(int taskType, int index, bool &success) {
	// Lab4 : Your code goes here.
    mtx.lock();
    switch (taskType) {
        case MAP:{
            mapTasks[index].isCompleted = true;
            completedMapCount ++;
            break;
        }
        case REDUCE:{
            reduceTasks[index].isCompleted = true;
            completedReduceCount ++;
            break;
        }
        case NONE:{
            break;
        }
    }

    mtx.unlock();

    if (isFinishedMap() && isFinishedReduce()){
        isFinished = true;
    }

    success = true;
	return mr_protocol::OK;
}

string Coordinator::getFile(int index) {
	this->mtx.lock();
	string file = this->files[index];
	this->mtx.unlock();
	return file;
}

bool Coordinator::isFinishedMap() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedMapCount >= long(this->mapTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

bool Coordinator::isFinishedReduce() {
	bool isFinished = false;
	this->mtx.lock();
	if (this->completedReduceCount >= long(this->reduceTasks.size())) {
		isFinished = true;
	}
	this->mtx.unlock();
	return isFinished;
}

//
// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
//
bool Coordinator::Done() {
	bool r = false;
	this->mtx.lock();
	r = this->isFinished;
	this->mtx.unlock();
	return r;
}

bool Coordinator::assignTask(Task &task) {
    task.taskType = NONE;
    mtx.lock();
    if (completedMapCount < (int)mapTasks.size()){
        for (auto &_task : mapTasks){
            if (_task.isAssigned && !_task.isCompleted){
                //timeout
                if (chrono::system_clock::now() - _task.startTime > chrono::seconds(30)){
                    _task.isAssigned = false;
                }
            }
            if (!_task.isAssigned && !_task.isCompleted){
                task = _task;
                _task.isAssigned = true;
                _task.startTime = chrono::system_clock::now();
                mtx.unlock();
                return true;
            }
        }
    } else if (completedReduceCount < (int)reduceTasks.size()){
        for (auto &_task : reduceTasks){
            if (_task.isAssigned && !_task.isCompleted){
                //timeout
                if (chrono::system_clock::now() - _task.startTime > chrono::seconds(30)){
                    _task.isAssigned = false;
                }
            }
            if (!_task.isAssigned && !_task.isCompleted){
                task = _task;
                _task.isAssigned = true;
                _task.startTime = chrono::system_clock::now();
                mtx.unlock();
                return true;
            }
        }
    }
    mtx.unlock();
    return false;
}

//
// create a Coordinator.
// nReduce is the number of reduce tasks to use.
//
Coordinator::Coordinator(const vector<string> &files, int nReduce)
{
	this->files = files;
	this->isFinished = false;
	this->completedMapCount = 0;
	this->completedReduceCount = 0;

	int filesize = files.size();
	for (int i = 0; i < filesize; i++) {
		this->mapTasks.push_back(Task{mr_tasktype::MAP, false, false, i});
	}
	for (int i = 0; i < nReduce; i++) {
		this->reduceTasks.push_back(Task{mr_tasktype::REDUCE, false, false, i});
	}
}

int main(int argc, char *argv[])
{
	int count = 0;

	if(argc < 3){
		fprintf(stderr, "Usage: %s <port-listen> <inputfiles>...\n", argv[0]);
		exit(1);
	}
	char *port_listen = argv[1];

	setvbuf(stdout, NULL, _IONBF, 0);

	char *count_env = getenv("RPC_COUNT");
	if(count_env != NULL){
		count = atoi(count_env);
	}

	vector<string> files;
	char **p = &argv[2];
	while (*p) {
		files.push_back(string(*p));
		++p;
	}

	rpcs server(atoi(port_listen), count);

	Coordinator c(files, REDUCER_COUNT);

	//
	// Lab4: Your code here.
	// Hints: Register "askTask" and "submitTask" as RPC handlers here
	//

    server.reg(mr_protocol::asktask, &c, &Coordinator::askTask);
    server.reg(mr_protocol::submittask, &c, &Coordinator::submitTask);

	while(!c.Done()) {
		sleep(1);
	}

	return 0;
}


