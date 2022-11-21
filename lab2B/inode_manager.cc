#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
    if (id < 0 || id >= BLOCK_NUM || !buf)
        return;
    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
    if (id < 0 || id >= BLOCK_NUM || !buf)
        return;
    memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated. //important!!!!
   */

  for (blockid_t i = (IBLOCK(INODE_NUM - 1, BLOCK_NUM) + 1); i < BLOCK_NUM; i++){
      if (using_blocks[i] == 0){
          using_blocks[i] = 1;
          return i;
      }
  }

  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  if (id < (IBLOCK(INODE_NUM - 1, BLOCK_NUM) + 1) || id >= BLOCK_NUM)
      return;

  using_blocks[id] = 0;

  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  for (uint32_t i = 1; i <= INODE_NUM; i++){
      if (!get_inode(i)){
          struct inode ino;
          ino.size = 0;
          ino.type = type;
          ino.atime = (uint32_t)(time(NULL));
          ino.ctime = (uint32_t)(time(NULL));
          ino.mtime = (uint32_t)(time(NULL));
          put_inode(i, &ino);
          return i;
      }
  }
  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */

  inode *ino = get_inode(inum);

  if (!ino)
      return;

  ino->type = 0;
  put_inode(inum, ino);

  free(ino);

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode*
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  /*
   * your code goes here.
   */

  if (inum < 0 || inum >= INODE_NUM)
      return NULL;

  char buf[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  struct inode *ino_disk = (struct inode*)buf + inum % IPB;

  if (ino_disk->type == 0)
      return NULL;

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode *ino = get_inode(inum);
  if (!ino)
      return;

  uint32_t block_num, _size = 0;

  if (ino->size % BLOCK_SIZE == 0){
      block_num = ino->size/BLOCK_SIZE;
  }
  else {
      block_num = ino->size/BLOCK_SIZE + 1;
  }

  char *buf = (char *)malloc(BLOCK_SIZE * block_num);

  for (uint32_t i = 0; i < NDIRECT && i < block_num; i++){
      bm->read_block(ino->blocks[i], buf + _size);
      _size += BLOCK_SIZE;
  }

  //if indirect block exits
  if (block_num > NDIRECT){
      //get indirect block
      blockid_t _block[NINDIRECT];
      bm->read_block(ino->blocks[NDIRECT], (char*)_block);

      for (uint32_t i = 0; i < block_num - NDIRECT; i++) {
          bm->read_block(_block[i], buf + _size);
          _size += BLOCK_SIZE;
      }

  }

  //copy
  *size = ino->size;
  *buf_out = buf;

  //update access time
  ino->atime = (uint32_t)(time(NULL));
  put_inode(inum, ino);

  free(ino);

  return;
}


/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf
   * is larger or smaller than the size of original inode
   */

    inode *ino = get_inode(inum);
    if (!ino)
        return;

    uint32_t new_num;
    if (size % BLOCK_SIZE == 0){
        new_num = size/BLOCK_SIZE;
    }
    else {
        new_num = size/BLOCK_SIZE + 1;
    }
    if (new_num > MAXFILE)
        return;

    uint32_t origin_num;
    if (ino->size % BLOCK_SIZE == 0){
        origin_num = ino->size/BLOCK_SIZE;
    }
    else {
        origin_num = ino->size/BLOCK_SIZE + 1;
    }

    if (new_num > origin_num){
        if (new_num <= NDIRECT){
            for (uint32_t i = origin_num; i< new_num; i++){
                ino->blocks[i] = bm->alloc_block();
            }
        }
        else {
            blockid_t _block[NINDIRECT];
            bm->read_block(ino->blocks[NDIRECT], (char*)_block);

            if (origin_num <= NDIRECT){
                for (uint32_t i = origin_num; i<= NDIRECT; i++){
                    ino->blocks[i] = bm->alloc_block();
                }
                for (uint32_t i = NDIRECT; i < new_num; i++){
                    _block[i - NDIRECT] = bm->alloc_block();
                }
            }
            else {
                for (uint32_t i = origin_num; i < new_num; i++){
                    _block[i - NDIRECT] = bm->alloc_block();
                }
            }

            bm->write_block(ino->blocks[NDIRECT], (char*)_block);
        }
    }
    else {
        if (new_num <= NDIRECT){
            if (origin_num <= NDIRECT){
                for (uint32_t i = new_num; i < origin_num; i++){
                    bm->free_block(ino->blocks[i]);
                }
            }
            else {
                blockid_t _block[NINDIRECT];
                bm->read_block(ino->blocks[NDIRECT], (char*)_block);

                for (uint32_t i = new_num; i <= NDIRECT; i++){
                    bm->free_block(ino->blocks[i]);
                }
                for (uint32_t i = NDIRECT; i < origin_num; i++){
                    bm->free_block(_block[i-NDIRECT]);
                }
            }
        }
        else {
            blockid_t _block[NINDIRECT];
            bm->read_block(ino->blocks[NDIRECT], (char*)_block);

            for (uint32_t i = new_num; i < origin_num; i++){
                bm->free_block(_block[i-NDIRECT]);
            }

            bm->write_block(ino->blocks[NDIRECT], (char*)_block);
        }
    }

    int _size = 0;
    for (uint32_t i = 0; i < new_num && i < NDIRECT; i++){
        bm->write_block(ino->blocks[i], buf + _size);
        _size += BLOCK_SIZE;
    }

    if (new_num > NDIRECT){
        blockid_t _block[NINDIRECT];
        bm->write_block(ino->blocks[NDIRECT], (char*)_block);

        for (uint32_t i = 0; i < new_num - NDIRECT; i++) {
            bm->write_block(_block[i], buf + _size);
            _size += BLOCK_SIZE;
        }
    }

    ino->size = size;

    //update time
    ino->atime = (uint32_t)(time(NULL));
    ino->ctime = (uint32_t)(time(NULL));
    ino->mtime = (uint32_t)(time(NULL));
    put_inode(inum, ino);

    free(ino);

  return;

}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode *ino = get_inode(inum);
  if (ino){
      a.type = ino->type;
      a.mtime = ino->mtime;
      a.ctime = ino->ctime;
      a.atime = ino->atime;
      a.size = ino->size;
  }
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
    inode *ino = get_inode(inum);
    if (!ino)
        return;

    uint32_t block_num;

    if (ino->size % BLOCK_SIZE == 0){
        block_num = ino->size/BLOCK_SIZE;
    }
    else {
        block_num = ino->size/BLOCK_SIZE + 1;
    }

    if (block_num <= NDIRECT){
        for (uint32_t i = 0; i < block_num; i++){
            bm->free_block(ino->blocks[i]);
        }
    }
    else {
        for (uint32_t i = 0; i < NDIRECT; i++){
            bm->free_block(ino->blocks[i]);
        }

        //free indirect blocks
        blockid_t _block[NINDIRECT];
        bm->read_block(ino->blocks[NDIRECT], (char*)_block);

        for (uint32_t i = NDIRECT; i < block_num; i++){
            bm->free_block(_block[i-NDIRECT]);
        }

        //free indirect block
        bm->free_block(ino->blocks[NDIRECT]);
    }

    //free block contains node
    free_inode(inum);
    bm->free_block(IBLOCK(inum,bm->sb.nblocks));
    free(ino);

  return;
}

