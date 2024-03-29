#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <algorithm>

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

bool isLetter(char ch){
    return ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'));
}

int mapHash(const string &str) {
    uint32_t hashVal = 0;
    for (char ch : str) {
        hashVal = hashVal * 1331 + (int)ch;
    }
    return hashVal % REDUCER_COUNT;
}

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.
    vector<KeyVal> ret;
    int start = 0, end = 0;
    while (start < (int)content.size()){
        while (!isLetter(content[start]) && start < (int)content.size()){
            start ++;
        }

        end = start + 1;
        while (isLetter(content[end]) && end < (int)content.size()) {
            end++;
        }

        if (start < (int)content.size()) {
            KeyVal ans;
            ans.key = content.substr(start, end - start);
            ans.val = "1";
            ret.push_back(ans);
        }
        start = end + 1;
    }
    return ret;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
    int sum = 0;
    for (auto &_value: values){
        sum += atoi(_value.data());
    }
    return to_string(sum);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const string &filenames);
	void doReduce(int index, int file_num);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

//one file once time
void Worker::doMap(int index, const string &filename)
{
    // Lab4: Your code goes here.
    string filepath = basedir + filename;
    ifstream file(filepath);

    ostringstream s;
    s << file.rdbuf();
    string content = s.str();

    file.close();

    vector <KeyVal> KVA = Map(filename, content);
    vector <string> contents(REDUCER_COUNT);

    for (auto const &keyVal : KVA){
        contents[mapHash(keyVal.key)] += keyVal.key + ' ' + keyVal.val + '\n';
    }

    string intermediate_basedir = basedir + "mr-" + to_string(index) + '-';

    for (int i = 0; i < REDUCER_COUNT; i++){
        auto const content = contents[i];
        if (!content.empty()){
            string intermediatePath = intermediate_basedir + to_string(i);
            ofstream outfile(intermediatePath);
            outfile << content;
            outfile.close();
        }
    }
}

void Worker::doReduce(int index, int file_num)
{
	// Lab4: Your code goes here.
    vector<KeyVal> intermediate;

    for (int i = 0; i < file_num; i ++){
        string filepath = basedir + "mr-" + to_string(i) + '-' + to_string(index);
        //fprintf(stderr, "%s\n", filepath.data());
        ifstream file(filepath);
        if (file.is_open()){
            string key, val;
            while(file >> key >> val){
                KeyVal keyVal;
                keyVal.key = key;
                keyVal.val = val;
                intermediate.push_back(keyVal);
            }
            file.close();
        }
    }

    sort(intermediate.begin(), intermediate.end(),
         [](KeyVal const & a, KeyVal const & b) {
             return a.key < b.key;
         });

    string content;

    for (unsigned int i = 0; i < intermediate.size();) {
        unsigned int j = i + 1;
        for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;)
            j++;

        vector < string > values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = Reduce(intermediate[i].key, values);
        content += intermediate[i].key + ' ' + output + '\n';

        i = j;
    }

    string filepath = basedir + "mr-out-" + to_string(index);
    ofstream outfile(filepath);
    outfile << content << endl;
    outfile.close();
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab4: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//
        mr_protocol::AskTaskResponse res;
        int ret = cl->call(mr_protocol::asktask, id, res);
        if (ret == mr_protocol::OK){
            switch (res.taskType) {
                case MAP:{
                    doMap(res.index, res.filename);
                    doSubmit(MAP, res.index);
                    break;
                }
                case REDUCE:{
                    doReduce(res.index, res.file_num);
                    doSubmit(REDUCE, res.index);
                    break;
                }
                case NONE:{
                    sleep(1);
                    break;
                }
            }
        }
	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

