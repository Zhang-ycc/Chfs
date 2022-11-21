// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  log = false;
  _persister->restore_checkpoint();
  _persister->restore_logdata();
  printf("restore ok!\n");
  vector<chfs_command> entries = _persister->get_log_entries();
  for (auto log : entries){

      extent_protocol::extentid_t id = log.ec_id;
      string str = log.param;

      switch (log.type) {
          case chfs_command::CMD_CREATE: {
              uint32_t type = atoi(str.c_str());
              create(type, id);
              break;
          }
          case chfs_command::CMD_PUT: {
              int a;
              put(id, str, a);
              break;
          }
          case chfs_command::CMD_REMOVE:{
              int a;
              remove(id, a);
              break;
          }
          default:{
              break;
          }
      }
  }
  log = true;
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
    writeLog(chfs_command::CMD_CREATE, 0, to_string(type));

  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    writeLog(chfs_command::CMD_PUT, id,buf);

  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
    //writeLog(chfs_command::CMD_GET, id, "");

  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0) {
      buf = "";
  }
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

//    std::ofstream file2;
//    file2.open("test/log2.txt", std::ios::app);
//    file2 << "1 " << buf.size() << " " << buf.data() << endl;
//    file2.close();

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{

    //writeLog(chfs_command::CMD_GETATTR, id, "");

  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
    writeLog(chfs_command::CMD_REMOVE, id, "");

  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  im->remove_file(id);
 
  return extent_protocol::OK;
}

void extent_server::writeLog(chfs_command::cmd_type type, extent_protocol::extentid_t ec_id, string s){
    if (log) {
        chfs_command log(tid, type, ec_id, s);
        transaction.push_back(log);
        //_persister->append_log(log);
    }
}

void extent_server::begin(chfs_command::txid_t id){
    if (log) {
        tid = id;
        string s = "";
        chfs_command log(id, chfs_command::CMD_BEGIN, 0, s);
        transaction.clear();
        transaction.push_back(log);
    }
}


void extent_server::commit(chfs_command::txid_t id){
    if (log) {
        string s = "";
        chfs_command log(id, chfs_command::CMD_COMMIT, 0, s);
        transaction.push_back(log);
        for (auto log: transaction) {
            _persister->append_log(log);
        }
        transaction.clear();
        _persister->checkpoint();
    }
}


