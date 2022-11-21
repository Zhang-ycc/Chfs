// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

/*
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create',
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log
 * to achive all-or-nothing for these transactions.
 */

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    //return (! isfile(inum) && !isSymlink(inum));
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    }
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}


bool
chfs_client::isSymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }

    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

    release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

    release:
    return r;
}

int
chfs_client::getSymlink(inum inum, symlinkinfo &sin)
{
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    sin.atime = a.atime;
    sin.mtime = a.mtime;
    sin.ctime = a.ctime;
    sin.size = a.size;

    release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    ec->beginLog(tid);

    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    std::string buf;

    if ((r = ec->get(ino, buf)) != OK) {
        return r;
    }

    size_t n = buf.size();
    if (n < size)
        buf += std::string(size - n, '\0');
    else
        buf = buf.substr(0, size);




    if ((r = ec->put(ino, buf)) != OK) {
        return r;
    }

    ec->commitLog(tid++);

    return r;
}

#define ALIGN(size) (((size) + 0x3) & ~0x3)

int chfs_client::make(inum parent, const char *name, extent_protocol::types type, inum &ino_out) {
    int r = OK;

    //printf("hhhhhhhhhhhh: %s\n",name);

    //lookup if file/dir exist
    bool found = false;
    if ((r = lookup(parent, name, found, ino_out)) != OK){
        return r;
    }
    if (found) {
        return EXIST;
    }

    //create file/dir
    if ((r = ec->create(type, ino_out) != OK)) {
        return r;
    }

    //get parent dir
    std::string buf;
    if ((r = ec->get(parent, buf)) != OK) {
        return r;
    }


    //add to parent dir

    buf.append(name);
    buf.push_back('^');
    buf.append(filename(ino_out));
    buf.push_back('^');


    if ((r = ec->put(parent, buf)) != OK) {
        ec->remove(ino_out);
        return r;
    }

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    ec->beginLog(tid);

    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    r = make(parent, name, extent_protocol::T_FILE, ino_out);

    ec->commitLog(tid++);

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    ec->beginLog(tid);

    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    r = make(parent, name, extent_protocol::T_DIR, ino_out);

    ec->commitLog(tid++);

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    //get parent dir
    std::list<dirent> dir;
    if ((r = readdir(parent, dir)) != OK){
        return r;
    }

    //search
    found = false;
    for (auto item : dir){
        //printf(item.name.c_str());
        //printf("\n");
        if (strcmp(item.name.c_str(), name) == 0){
            //printf("rrrrrrrrrr  %s\n",item.name.c_str());
            found = true;
            ino_out = item.inum;
            break;
        }
    }

    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;

    //get dir
    if ((r = ec->get(dir, buf)) != OK) {
        return r;
    }

    int start = 0;
    int end = buf.length();
    while(start < end)
    {
        struct dirent new_dirent;
        size_t n1 = buf.find('^', start);
        new_dirent.name = buf.substr(start, n1-start);
        size_t n2 = buf.find('^', n1 + 1);
        new_dirent.inum = n2i(buf.substr(n1 + 1, n2 - n1 - 1));
        list.push_back(new_dirent);
        start = n2 + 1;
    }

    return r;

}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    std::string buf;
    if ((r = ec->get(ino, buf)) != OK){
        return r;
    }

    data = buf.substr(off, size);

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
                   size_t &bytes_written)
{
    ec->beginLog(tid);

    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    //get origin content
    std::string buf;

    if ((r = ec->get(ino, buf)) != OK){
        return r;
    }

    int n = buf.size();

    if (off < n){
        if (off + (int)size < n) {
            buf = buf.substr(0, off) + std::string(data, size) +
                  buf.substr(off + size, n - off - size);
        }
        else {
            buf = buf.substr(0, off) + std::string(data, size);
        }
    }
    else{
        buf += std::string(off - n, '\0') + std::string(data, size);

    }

    bytes_written = size;

    if ((r = ec->put(ino, buf)) != OK){
        return r;
    }

    ec->commitLog(tid++);

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    ec->beginLog(tid);

    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    std::string buf;
    if ((r = ec->get(parent, buf)) != OK) {
        return r;
    }

    inum ino;

    size_t pos = 0;
    const char *ptr = buf.c_str();
    bool found = false;

    while (pos < buf.size()) {
        inum tmp;
        uint16_t rec_len, name_len;
        memcpy(&tmp, ptr, 4);
        memcpy(&rec_len, ptr + 4, 2);
        memcpy(&name_len, ptr + 6, 2);

        if (strcmp(std::string(ptr + 8, name_len).c_str(), name) == 0) {
            found = true;
            ino = tmp;
            //delete from parent dir
            buf = buf.substr(0, pos) +
                  buf.substr(pos + rec_len, buf.size() - pos - rec_len);
            break;
        }
        ptr += rec_len;
        pos += rec_len;
    }

    if (!found) {
        return NOENT;
    }

    //remove file
    if ((r = ec->remove(ino)) != OK) {
        return r;
    }

    //update parent dir
    if ((r = ec->put(parent, buf)) != OK) {
        return r;
    }

    ec->commitLog(tid++);

    return r;
}

int
chfs_client::symlink(const char *link, inum parent,
                     const char *name, inum &ino_out)
{
    int r = OK;

    if ((r = make(parent, name, extent_protocol::T_SYMLINK, ino_out)) != OK){
        return r;
    }

    size_t bytes_written;
    if ((r = write(ino_out, strlen(link), 0, link, bytes_written)) != OK){
        return r;
    }

    return r;
}

int
chfs_client::readlink(inum ino, std::string &link)
{
    int r = OK;

    if ((r = ec->get(ino, link)) != OK){
        return r;
    }

    return r;
}




