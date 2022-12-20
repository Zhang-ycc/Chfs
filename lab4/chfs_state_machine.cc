#include "chfs_state_machine.h"

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    cmd_tp = chfs_command_raft::command_type::CMD_NONE;
    type = 0;
    id = 0;
    buf = "";
    res = std::make_shared<result>();
    res->start = chrono::system_clock::now();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
        cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
    res->start = chrono::system_clock::now();
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here

}

int chfs_command_raft::size() const{
    // Lab3: Your code here
    return sizeof(command_type) + sizeof(uint32_t) + sizeof(extent_protocol::extentid_t) +  sizeof(uint64_t)
           + buf.size();
}

void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    int pos = 0;
    uint64_t buf_size = buf.size();

    memcpy(buf_out + pos, (char *) &cmd_tp, sizeof(command_type));
    pos += sizeof(command_type);

    memcpy(buf_out + pos, (char *) &type, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    memcpy(buf_out + pos, (char *) &id, sizeof(extent_protocol::extentid_t));
    pos += sizeof(extent_protocol::extentid_t);

    memcpy(buf_out + pos, (char *) &buf_size, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    if (buf_size > 0) {
        memcpy(buf_out + pos, &buf[0], buf_size);
    }

    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here

    int pos = 0;
    uint64_t buf_size = 0;

    memcpy((char *) &cmd_tp, buf_in + pos, sizeof(command_type));
    pos += sizeof(command_type);

    memcpy((char *) &type, buf_in + pos, sizeof(uint32_t));
    pos += sizeof(uint32_t);

    memcpy((char *) &id, buf_in + pos, sizeof(extent_protocol::extentid_t));
    pos += sizeof(extent_protocol::extentid_t);

    memcpy((char *) &buf_size, buf_in + pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    if (buf_size > 0) {
        char *_buf = new char[buf_size];
        memcpy(_buf, buf_in + pos, buf_size);
        buf = _buf;
    } else{
        buf = "";
    }

    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m << cmd.buf << cmd.id << cmd.type << cmd.cmd_tp;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int type;
    u >> cmd.buf >> cmd.id >> cmd.type >> type;
    cmd.cmd_tp = chfs_command_raft::command_type(type);
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here

    std::unique_lock<std::mutex> lock(chfs_cmd.res->mtx);

    chfs_cmd.res->done = true;

    switch (chfs_cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT: {
            es.create(chfs_cmd.type, chfs_cmd.res->id);
            break;
        }
        case chfs_command_raft::CMD_GET: {
            es.get(chfs_cmd.id, chfs_cmd.res->buf);
            break;
        }
        case chfs_command_raft::CMD_PUT: {
            int a;
            es.put(chfs_cmd.id, chfs_cmd.buf, a);
            break;
        }
        case chfs_command_raft::CMD_GETA: {
            es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
            break;
        }
        case chfs_command_raft::CMD_RMV: {
            int a;
            es.remove(chfs_cmd.id, a);
            break;
        }
        case chfs_command_raft::CMD_NONE: {
            break;
        }
    }

    chfs_cmd.res->cv.notify_all();

    return;
}