#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

BLKSIZE = 512 #Global variable for block size

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    """Implements a hierarchical file system by using FUSE virtual filesystem.
       The file structure and data are stored in local memory in variable.
       Data is lost when the filesystem is unmounted"""

    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2, files={})
        # The key 'files' holds a dict of filenames(and their attributes
        #  and 'files' if it is a directory) under each level

    def traverse(self, path, tdata = False):
        """Traverses the dict of dict(self.files) to get pointer
            to the location of the current file.
            Retuns the node from self.data if tdata else from self.files"""
        p = self.data if tdata else self.files['/']
        if tdata:
            for i in path.split('/') :
                p = p[i] if len(i) > 0 else p
        else:
            for i in path.split('/') :
                p = p['files'][i] if len(i) > 0 else p
	#print (p)
        return p

    def traverseparent(self, path, tdata = False):
        """Traverses the dict of dict(self.files) to get pointer
            to the parent directory of the current file.
            Also returns the child name as string"""
        p = self.data if tdata else self.files['/']
	#print(path.rfind('/')+1)
	#print(path.rfind('/'))
        target = path[path.rfind('/')+1:]
        path = path[:path.rfind('/')]
        if tdata:
            for i in path.split('/') :
                p = p[i] if len(i) > 0 else p
        else:
            for i in path.split('/') :
                p = p['files'][i] if len(i) > 0 else p
        return p, target

    def chmod(self, path, mode):
        p = self.traverse(path)
        p['st_mode'] &= 0o770000
        p['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        p = self.traverse(path)
        p['st_uid'] = uid
        p['st_gid'] = gid

    def create(self, path, mode):
        p, tar = self.traverseparent(path)
        p['files'][tar] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time())
        self.fd += 1
        return self.fd

    def getattr(self, path, fh = None):
        try:
            p = self.traverse(path)
        except KeyError:
            raise FuseOSError(ENOENT)
        return {attr:p[attr] for attr in p.keys() if attr != 'files'}

    def getxattr(self, path, name, position=0):
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        p, tar = self.traverseparent(path)
        p['files'][tar] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),files={})
        p['st_nlink'] += 1
        d, d1 = self.traverseparent(path, True)
        d[d1] = defaultdict(bytes)
	print (d)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        d = self.traverse(path, True)
	dat=''.join(d)						# Join the content in the list d and then output the portion of data requested
	dat1=dat[offset:offset+size]				#by system
        return dat1

    def readdir(self, path, fh):
        p = self.traverse(path)['files']
        return ['.', '..'] + [x for x in p ]

    def readlink(self, path):
        return self.traverse(path, True)

    def removexattr(self, path, name):
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        po, po1 = self.traverseparent(old)
        pn, pn1 = self.traverseparent(new)
        if po['files'][po1]['st_mode'] & 0o770000 == S_IFDIR:
            po['st_nlink'] -= 1
            pn['st_nlink'] += 1
        pn['files'][pn1] = po['files'].pop(po1)
        do, do1 = self.traverseparent(old, True)
        dn, dn1 = self.traverseparent(new, True)
        dn[dn1] = do.pop(do1)

    def rmdir(self, path):
        p, tar = self.traverseparent(path)
        if len(p['files'][tar]['files']) > 0:
            raise FuseOSError(ENOTEMPTY)
        p['files'].pop(tar)
        p['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        p = self.traverse(path)
        attrs = p.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        p, tar = self.traverseparent(target)
        p['files'][tar] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
        d, d1 = self.traverseparent(target, True)
        d[d1] = source

    def truncate(self, path, length, fh = None):
        d,d1 = self.traverseparent(path, True)				#Join the list of the data blocks, modify it to truncate the size and 
	d[d1]=''.join(d[d1])						# break the modified data into block sizes again to store it as blocks.
	d[d1]=d[d1][:length]
	t=len(d[d1]) 
	a=list()
	for pos in range(0,t,BLKSIZE):
        	a.append(d[d1][pos:pos+BLKSIZE])      
	d[d1] = a
        p = self.traverse(path)
        p['st_size'] = length

    def unlink(self, path):
        p, tar = self.traverseparent(path)
        p['files'].pop(tar)

    def utimens(self, path, times = None):
        now = time()
        atime, mtime = times if times else (now, now)
        p = self.traverse(path)
        p['st_atime'] = atime
        p['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        p = self.traverse(path)
        d, d1 = self.traverseparent(path, True)				#Join the list of elements in string, Perform all the changes in the 
	a= list()							# data string. once the changes are made, break all the data into 
	d[d1]=''.join(d[d1])						# blocks of BLKSIZE and transfer those changes in self.data.
	d[d1]= d[d1][:offset] + data + d[d1][offset+len(data):]
	t=len(d[d1]) 
	for pos in range(0,t,BLKSIZE):
        	a.append(d[d1][pos:pos+BLKSIZE])
	d[d1] = a
	print(d[d1])
	print(len(d[d1]))
	p['st_size']=t
	return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)	
    #logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True,debug=True)
