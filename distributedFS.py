#!/usr/bin/env python

import logging
import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest
from datetime import datetime, timedelta
from xmlrpclib import Binary
from math import ceil
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
        self.fd = 0
        now = time()
	self.rpc_met = xmlrpclib.ServerProxy("http://localhost:"+str(m_port)+'/')	# create an instance of metaserver
	self.dserver = len(d_port)		# number of data servers
	self.rpc_dat= list()			# the list of instances of data server
	for i in range(0,len(d_port)):
		self.rpc_dat.append(xmlrpclib.ServerProxy("http://localhost:"+str(d_port[i])+'/'))	# loop to make instances of data server
		
 	a = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2, files=list())
	strmeta = pickle.dumps(a)
	self.rpc_met.put(Binary('/'),Binary(strmeta))	#initializing meta server with attributes of root directory

    def hashit(self,path):
	"""Hash function, outputs a server index by performing summation of ascii values of the path of a file and gives the index as the modulo of the ascii sum with number of data servers """
	d_ind = 0
	for i in str(path):
		d_ind = d_ind + ord(i)
	return d_ind%self.dserver


    def traverseparent(self, path, tdata = False):
        """Traverses the dict of dict(self.files) to get pointer
            to the parent directory of the current file.
            Also returns the child name and parent path as string"""
        target = path[path.rfind('/')+1:]
        path = path[:path.rfind('/')]
	print(path)
	print(target)
	p = 0
        return p, target,path

    def chmod(self, path, mode):
	"""Get put method to receive metadata from server, make changes to data and putting it back on server """
        ldmeta= self.rpc_met.get(Binary(path))
	dt = pickle.loads(ldmeta.data) 		
        dt['st_mode'] &= 0o770000
        dt['st_mode'] |= mode
	strmeta = pickle.dumps(dt)
	self.rpc_met.put(Binary(path),Binary(strmeta))
        return 0

    def chown(self, path, uid, gid):
	"""Get put method to receive metadata from server, make changes to data and putting it back on server """
        ldmeta= self.rpc_met.get(Binary(path))
	dt = pickle.loads(ldmeta.data) 		
        dt['st_uid'] = uid
        dt['st_gid'] = gid
	strmeta = pickle.dumps(dt)
	self.rpc_met.put(Binary(path),Binary(strmeta))

    def create(self, path, mode):
	"""Get put method to receive metadata from server, make changes to data and putting it back on server """
        p, tar, pat = self.traverseparent(path)
	if len(pat)==0:				# check if operations are performed on root
		pat ='/'
        ldmeta= self.rpc_met.get(Binary(pat))
	dt = pickle.loads(ldmeta.data) 	
	dt['files'].append(tar)			# append child name in files list of parent directory
	strmeta =pickle.dumps(dt)	
	self.rpc_met.put(Binary(pat),Binary(strmeta))
 	a = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                     st_size=0, st_ctime=time(), st_mtime=time(),
                     st_atime=time())	
	strmeta = pickle.dumps(a)
	self.rpc_met.put(Binary(path),Binary(strmeta))	# create a new key value pair for new file at meta server
        self.fd += 1
        return self.fd

    def getattr(self, path, fh = None):
	self.rpc_met.print_content()
	ldmeta= self.rpc_met.get(Binary(path))	# get the metadata of file from meta server and check if the key exists at server	
	if ldmeta ==False:			# if key doesnot exist raise error
            raise FuseOSError(ENOENT)
	else:       
	    dt = pickle.loads(ldmeta.data) 	# else return attributes	
	    return {attr:dt[attr] for attr in dt.keys() if attr != 'files'}

    def getxattr(self, path, name, position=0):
	ldmeta= self.rpc_met.get(Binary(path)) # get the metadata of file from meta server and check if the key exists at server
        if ldmeta!=False:			# if key exists get attrs in attrs variable
	    dt = pickle.loads(ldmeta.data) 
            attrs = dt.get('attrs', {})
	else:					# if key doesnot exist make attrs empty which would raise KeyError
	    attrs = {}	
	try:        
	    return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
	ldmeta= self.rpc_met.get(Binary(path)) # get the metadata of file from meta server
	dt = pickle.loads(ldmeta.data) 
        attrs = dt.get('attrs', {})	# get the attrs from dt and return them
        return attrs.keys()

    def mkdir(self, path, mode):
        p, tar, pat = self.traverseparent(path) # get the paths for the child as well as parent directory
	self.rpc_met.print_content()
	if len(pat)==0:		# if parent path comes up as empty then perform operation on root directory
		pat ='/'
        ldmeta= self.rpc_met.get(Binary(pat))	# add target in files list of parent and increment st_link
	dt = pickle.loads(ldmeta.data) 	
	dt['files'].append(tar)
	dt['st_nlink'] += 1
	strmeta =pickle.dumps(dt)	
	self.rpc_met.put(Binary(pat),Binary(strmeta))  # dump back the changes made in parent
	a = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(),files=list())
	strmeta = pickle.dumps(a)
	self.rpc_met.put(Binary(path),Binary(strmeta))	# add the new key for the new file in metaserver


    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
	x = self.hashit(path+'0')		 # get the server no on which to upload the first byte of data
	lddata= self.rpc_dat[x].get(Binary(path+'0'))	# try to get the first block if it exists
	ldmeta = self.rpc_met.get(Binary(path))	# load the metadata for the file to read
	c = pickle.loads(ldmeta.data) 
	size1 = c['st_size']
	noblock = int(ceil(float(size1)/BLKSIZE))	# calculate number of blocks in the file
	if lddata != False:			# if the file already exists
		dt =pickle.loads(lddata.data)
		for i in range(1,noblock):
			lddata=self.rpc_dat[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(lddata.data)	# get all the blocks of the file and join them
			dt=dt+b	
	else:
		dt={}		# if the file doesnot exist return blank dictionary

	dat=''.join(dt)						
	dat1=dat[offset:offset+size]	#split and return the data asked for			
        return dat1

    def readdir(self, path, fh):
	ldmeta= self.rpc_met.get(Binary(path)) # get the metadata of directory
	dt = pickle.loads(ldmeta.data) 	
        return ['.', '..'] + [x for x in dt['files'] ]	# see in the files list of metadata if there are any files in the directory

    def readlink(self, path):
	x = self.hashit(path+'0')		 # calculate hash function to get the first block to get the server no.
	ldmeta = self.rpc_met.get(Binary(path))	 #load metadata from metaserver
	c = pickle.loads(ldmeta.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/BLKSIZE))
	lddata= self.rpc_dat[x].get(Binary(path+'0'))	# get the first block of the data of file
	if lddata != False:			# if the file exists
		dt =pickle.loads(lddata.data)
		for i in range(1,noblock):	# get all the rest of the data
			lddata=self.rpc_dat[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(lddata.data)
			dt=dt+b
        return dt		#return the data

    def removexattr(self, path, name):
	ldmeta= self.rpc_met.get(Binary(path))  # get metadata from server
        if ldmeta!=False:
	    dt = pickle.loads(ldmeta.data) 
            attrs = dt.get('attrs', {})		# get attrs if they exists or return blank dictionary
	else:
	    attrs ={}	
	try:       
	     del attrs[name]
        except KeyError:
            pass       # Should return ENOATTR


    def rename(self, old, new):
	po, po1, pato= self.traverseparent(old) #get the parent and child path of old filename
        pn, pn1, patn= self.traverseparent(new) #get the parent and child path of new filename
	ldmeta = self.rpc_met.get(Binary(old))	# get the metadta of child of old filename
	dto = pickle.loads(ldmeta.data)
	size1 = dto['st_size']
	noblock = int(ceil(float(size1)/BLKSIZE))	# check its size and calculate number of blocks in file
	print('######noblock#####',noblock)
	if len(pato)==0:			# if the len of parent path is 0 then work on root directory
		pato='/'
	if len(patn)==0:
		patn='/'
	ldmeta1 =self.rpc_met.get(Binary(pato))	#access the parent in old filename
	dt1= pickle.loads(ldmeta1.data)
	dt1['files'].remove(str(po1))		# remove child from parent files list
        if dto['st_mode'] & 0o770000 == S_IFDIR:	# if old path leads to a directory then decrement st_nlink in parent else
		dt1['st_nlink'] -= 1			# if old path is for a file dont make any changes
	strmeta = pickle.dumps(dt1)
	self.rpc_met.put(Binary(pato),Binary(strmeta))	# dump back the changes to metaserver

	ldmeta2 =self.rpc_met.get(Binary(patn))	# access the parent in new filename
	dt2= pickle.loads(ldmeta2.data)
	dt2['files'].append(pn1)		# append child to parent files list
        if dto['st_mode'] & 0o770000 == S_IFDIR:	# if old path leads to a directory then decrement st_nlink
		dt2['st_nlink'] += 1
	strmeta1 = pickle.dumps(dt2)		# dump back changes
	strmeta3 = pickle.dumps(dto)
	self.rpc_met.put(Binary(patn),Binary(strmeta1))
	self.rpc_met.delete_k(Binary(old))	# delete the whole key for old path
	self.rpc_met.put(Binary(new),Binary(strmeta3))	# add new kev value for new path to metaserver

	x = self.hashit(old+'0')		 # get server no. for the first block of old file data
	x1 = self.hashit(new+'0')		 # get server no. for the first block of old file data
	lddata0= self.rpc_dat[x].get(Binary(old+'0'))	# if the file exists then transfer the data to new path key and delete old data
	if lddata0 != False:	
		for i in range(0,noblock):
			lddata0=self.rpc_dat[(x+i)%self.dserver].get(Binary(old+str(i)))			
			b=pickle.loads(lddata0.data)
			strdata=pickle.dumps(b)
			self.rpc_dat[(x1+i)%self.dserver].put(Binary(new+str(i)),Binary(strdata))			
			self.rpc_dat[(x+i)%self.dserver].delete_k(Binary(old+str(i)))

    def rmdir(self, path):
        p, tar,pat = self.traverseparent(path) #check if operation is on root?
	if len(path)==0:
		path='/'	
	ldmeta =self.rpc_met.get(Binary(path))# load the metadata for child
	dt = pickle.loads(ldmeta.data)
        if len(dt['files']) > 0:	# if there are files in directory, raise error
            raise FuseOSError(ENOTEMPTY)
	self.rpc_met.delete_k(Binary(path)) # else delete the key from metaserver
	if len(pat)==0:		#check if parent is root
		pat='/'
	ldmeta =self.rpc_met.get(Binary(pat)) # get the parent directory metadata
	dt = pickle.loads(ldmeta.data)
	dt['files'].remove(str(tar))	# remove the child from files list
        dt['st_nlink'] -= 1		# decrement st_nlink in parent
	strmeta = pickle.dumps(dt)
	self.rpc_met.put(Binary(pat),Binary(strmeta)) # dump changes in parent

    def setxattr(self, path, name, value, options, position=0):
	ldmeta= self.rpc_met.get(Binary(path)) # get metadata and if file exists get attr value
        if ldmeta!=False:
	    dt = pickle.loads(ldmeta.data) 
            attrs = dt.setdefault('attrs', {})
            attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        p, tar,pat = self.traverseparent(target)
	a = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
	strmeta = pickle.dumps(a)
	self.rpc_met.put(Binary(target),Binary(strmeta)) # make new key value pairs in metaserver

	if len(pat)==0:
		pat='/'	
	ldmeta =self.rpc_met.get(Binary(pat))
	dt =pickle.loads(ldmeta.data)
	dt['files'].append(tar)		#append the target in files list
	strmeta = pickle.dumps(dt)
	self.rpc_met.put(Binary(pat),Binary(strmeta))

	x = self.hashit(target+'0')		 
	ldmeta = self.rpc_met.get(Binary(target)) 	#get all the blocks of the file
	c = pickle.loads(ldmeta.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/BLKSIZE))
	lddata= self.rpc_dat[x].get(Binary(target+'0'))	
	if lddata != False:	
		dt =pickle.loads(lddata.data)
		for i in range(1,noblock):
			lddata=self.rpc_dat[(x+i)%self.dserver].get(Binary(target+str(i)))			
			b=pickle.loads(lddata.data)
			dt=dt+b	
	else:
		dt={}
	dt=''.join(dt)						# change the data in the file
	dt = source
	t=len(dt) 
	a=list()
	for pos in range(0,t,BLKSIZE):
        	a.append(dt[pos:pos+BLKSIZE])      #break the file into blocks and update at server
	dt = a
	for i in range(0,len(dt)):
		strdata=pickle.dumps(dt[i])
		lddata=self.rpc_dat[(x+i)%self.dserver].put(Binary(target+str(i)),Binary(strdata))			

    def truncate(self, path, length, fh = None):
	x = self.hashit(path+'0')		 
	ldmeta = self.rpc_met.get(Binary(path))	#get the blocks of the file change the data and update it back
	c = pickle.loads(ldmeta.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/BLKSIZE))
	lddata= self.rpc_dat[x].get(Binary(path+'0'))	
	if lddata != False:	
		dt =pickle.loads(lddata.data)
		for i in range(1,noblock):
			lddata=self.rpc_dat[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(lddata.data)
			dt=dt+b	
	else:
		dt={}
	dt=''.join(dt)						
	dt=dt[:length]
	t=len(dt) 
	a=list()
	for pos in range(0,t,BLKSIZE):
        	a.append(dt[pos:pos+BLKSIZE])      
	dt = a
	for i in range(0,len(dt)):
		strdata=pickle.dumps(dt[i])
		lddata=self.rpc_dat[(x+i)%self.dserver].put(Binary(path+str(i)),Binary(strdata))			
	c['st_size']= length
	strmeta = pickle.dumps(c)
	self.rpc_met.put(Binary(path),Binary(strmeta))
	

    def unlink(self, path):
        p, tar,pat = self.traverseparent(path)
	self.rpc_met.delete_k(Binary(path))		# delete the keys from the server
	if len(pat)==0:
		pat='/'
	ldmeta =self.rpc_met.get(Binary(pat))
	dt = pickle.loads(ldmeta.data)        
	dt['files'].remove(str(tar))		# remove the child from parent dir
	strmeta = pickle.dumps(dt)
	self.rpc_met.put(Binary(pat),Binary(strmeta))

    def utimens(self, path, times = None):
        now = time()
        atime, mtime = times if times else (now, now)	# get the meta data make changes and upload back
        ldmeta= self.rpc_met.get(Binary(path))
	dt = pickle.loads(ldmeta.data) 	
	dt['st_atime'] = atime
	dt['st_mtime'] = mtime
	strmeta = pickle.dumps(dt)
	self.rpc_met.put(Binary(path),Binary(strmeta))

    def write(self, path, data, offset, fh):
	x = self.hashit(path+'0')		 
	ldmeta = self.rpc_met.get(Binary(path))	# calculate size and no of blocks from metadata
	c = pickle.loads(ldmeta.data) 
	size = c['st_size']
	noblock = int(ceil(float(size)/BLKSIZE))
	lddata= self.rpc_dat[x].get(Binary(path+'0'))	
	if lddata != False:			# get all the file blocks make changes and upload it back
		dt =pickle.loads(lddata.data)
		for i in range(1,noblock):
			lddata=self.rpc_dat[(x+i)%self.dserver].get(Binary(path+str(i)))			
			b=pickle.loads(lddata.data)
			dt=dt+b
	else:
		dt={}
	a= list()								
	dt=''.join(dt)						
	dt= dt[:offset] + data + dt[offset+len(data):]
	t=len(dt) 
	for pos in range(0,t,BLKSIZE):
        	a.append(dt[pos:pos+BLKSIZE])
	dt = a
	for i in range(0,len(dt)):
		strdata=pickle.dumps(dt[i])
		lddata=self.rpc_dat[(x+i)%self.dserver].put(Binary(path+str(i)),Binary(strdata))			
	c['st_size']=t
	strmeta = pickle.dumps(c)
	self.rpc_met.put(Binary(path),Binary(strmeta))
	return len(data)


if __name__ == '__main__':
    if len(argv) <= 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)
    m_port = argv[2]
    d_port = list()
    for x in range(3,len(argv)):
	d_port.append(argv[x])		
    logging.basicConfig(level=logging.DEBUG)	
    #logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True,debug=True)
