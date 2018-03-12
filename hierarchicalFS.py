#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
##Helper functions
    def add_path(self,path1):					#splits the system provided path into a list with paths as list members
        npath= list(path1.split('/'))
	npath.remove('')
        print(npath)
        return npath

## System call functions
    def chmod(self, path, mode):

	npath = self.add_path(path)				#split the path
	if npath[0] == '':					#Checks if root directory is accessed
		self.files['/']['st_mode'] &= 0o770000
		self.files['/']['st_mode'] |= mode
	else:
		tmp_dict = self.files['/']			#Temporary dictionary used for traversing nested dictionary
		for x in npath:
			tmp_dict = tmp_dict[str(x)]
		tmp_dict['st_mode'] &= 0o770000			#change the mode of the file
		tmp_dict['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):

	npath = self.add_path(path)				#split the path
	if npath[0] == '':					#changes if root directory is accessed
		self.files['/']['st_uid'] = uid
		self.files['/']['st_gid'] = gid
	else:
		tmp_dict = self.files['/']			#traversing the nested files dictionary
		for x in npath:
			tmp_dict = tmp_dict[str(x)]
		tmp_dict['st_uid'] = uid
		tmp_dict['st_gid'] = gid
        return 0

    def create(self, path, mode):
	
	npath = self.add_path(path)				#Split the path
	tmp_dict=self.files['/']	
	if npath[0] == '':			     		#just pass the file descriptor as it is when accessing root
		return self.fd	
	else:	
		for x in npath:					#traverse the nested directories and add the metadata of a new file in files 
			if x not in tmp_dict:			#dictionary
				tmp_dict[str(x)] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

			tmp_dict = tmp_dict[str(x)]
		print(self.files)
		self.fd += 1
        	return self.fd
 
 
    def getattr(self, path, fh=None):
	npath = self.add_path(path)												#Split the path
	other_val=['st_ctime','st_mtime','st_atime','st_mode','st_nlink','st_size']	# a temp dictionary to screen all attributes of a 
	tmp_attr={}									#given directory

	
	if npath[0] == '':	
		for x in self.files[path]:						#if accessing root transfer all the attributes of root
			if x in other_val:						# to tmp_attr and return it
				tmp_attr[x]=self.files[path][x]
		return tmp_attr
	else:
		tmp_dict=self.files['/']						#if accessing some other directory, traverse to the 
		for x in npath:								#directory and copy attributes in tmp_attr and return
			if x in tmp_dict:	 	
				tmp_dict = tmp_dict[str(x)]	
			else :
				raise FuseOSError(ENOENT)		
		for x in tmp_dict:
			if x in other_val:
				tmp_attr[x]= tmp_dict[x] 
		return tmp_attr	


    def getxattr(self, path, name, position=0):

	npath= self.add_path(path)						#Split the path	
	if npath[0]=='':
		attrs = self.files['/'].get('attrs',{})				#if accessing root get all the attrs associated with root
	else:
		tmp_dict= self.files['/']					#if accessing other directory traverse to the directory and 
		for x in npath:							# get all the attrs associated with it
			tmp_dict=tmp_dict[str(x)]
		attrs = tmp_dict.get('attrs',{})	
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):

	npath= self.add_path(path)						#Split the path	
	if npath[0]=='':
		attrs = self.files['/'].get('attrs',{})				#same as above function only difference, keys are returned 
	else:									#instead of values
		tmp_dict= self.files['/']		
		for x in npath:
			tmp_dict=tmp_dict[str(x)]
		attrs = tmp_dict.get('attrs',{})	

        return attrs.keys()

    def mkdir(self, path, mode):
	t_int = 1 
	t_fl = 1.0
	t_long =12345L
	npath = self.add_path(path)						#split the path
	tmp_dict=self.files['/']	
	if npath[0] == '':							#if accessing root simply print the files metadata dictionary
		print (self.files)			
	else:
		if len(npath) < 2:						#if path length less than 2, parent directory is first element 
			self.files['/']['st_nlink']+=1					#in npath
		else:
			dir_path=npath[-2]					#if path more than 2, parent directory is second last element
		for x in npath:							#in npath
			if x not in tmp_dict:		
				tmp_dict[str(x)] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                	st_size=0, st_ctime=time(), st_mtime=time(),	#if directory doesnot exist already in self.files
                                	st_atime=time())				#add its metadata to the files dictionary

			if len(npath) >= 2:
				if x == dir_path:
					tmp_dict[str(dir_path)]['st_nlink']+=1		#add the link attribute of parent directory
			tmp_dict = tmp_dict[str(x)]	
		print(self.files)
				
    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
	t_int = 1
	t_fl = 1.0
	t_long = 12345L
	npath = self.add_path(path)						#Split path
	if npath[0] == '':						
		tmp_dict=self.files['/']					#in root output name of all the dictionary keys
		keys=tmp_dict.keys()						#other than attributes
		fin_out= ['.','..']
		other_val=['st_ctime','st_mtime','st_atime','st_mode','st_nlink','st_size']
		for val in keys:
			if val not in other_val:
				fin_out.append(val)
	
		return fin_out		
	else:									#in other directories, first reach to the last directory in
		tmp_dict=self.files['/']					#path and get all the other keys except the attributes
		for x in npath:
			tmp_dict = tmp_dict[str(x)]	
		keys = tmp_dict.keys()
		fin_out= ['.','..']
		other_val=['st_ctime','st_mtime','st_atime','st_mode','st_nlink','st_size']
		for val in keys:
			if val not in other_val:
				fin_out.append(val)
	
			
		return fin_out	


    def readlink(self, path):
        return self.data[path]

    def removexattr(self, path, name):
        
	npath= self.add_path(path)						# Split path
	if npath[0]=='':
		attrs = self.files['/'].get('attrs',{})				#get the attributes of directories in attrs 
	else:
		tmp_dict= self.files['/']		
		for x in npath:
			tmp_dict=tmp_dict[str(x)]
		attrs = tmp_dict.get('attrs',{})	

        try:
            del attrs[name]							#delete the attribute to be removed
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        
	npath=self.add_path(old)						#Split the old path
	npath1=self.add_path(new)						#Split the new path
	if npath[0] == '':
		self.files[new] = self.files.pop('/')				#Remove the old name binding and transfer the attributes to 
		
	else:									# new key
		tmp_dict = self.files['/']
		if len(npath) == 1:
			dir_path = npath[0]
		else:
			dir_path = npath[-1]
		for x in npath:
			if x == dir_path:
				if tmp_dict[dir_path]['st_mode'] == 33204:	#check whether the name is of a file or a directory
					self.data[new]=self.data.pop(old)	#in case of file transfer data
					tmp_dict[str(npath1[-1])] = tmp_dict.pop(str(x))#change file metadata
					print('hi')
				else:				
					tmp_dict[str(npath1[-1])] = tmp_dict.pop(str(x))# in case of directory just change directory metadata
			else:		
				tmp_dict = tmp_dict[str(x)] 
				

    def rmdir(self, path):

	npath = self.add_path(path)					#Split the path
	if len(npath) < 2:
		if npath[0] != '':	        			#Pop the directory key that is to be removed
			self.files['/'].pop(npath[0])
        		self.files['/']['st_nlink'] -= 1		#Decrease the link count in parent directory
		else:
			self.files.pop(path)
        		self.files['/']['st_nlink'] -= 1
	elif len(npath) >= 2:	
		tmp_dict=self.files['/']				#Travel to the parent directory and remove the child directory
		dir_path = npath[-2]	# parent directory					
		print (dir_path)
		print (npath[-1])	
		for x in npath:
			if x != dir_path and x!= npath[-1]:
				tmp_dict = tmp_dict[str(x)]	
	
			elif x == dir_path:				#Decrease the link count in parent directory
				tmp_dict[str(x)]['st_nlink'] -= 1
				tmp_dict[str(x)].pop(str(npath[-1]))	#pop the childeren directory

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options        					#set the attribute to a given value
	npath= self.add_path(path)
	if npath[0]=='':
		 attrs = self.files['/'].setdefault('attrs', {})
		 attrs[name] = value	
	else:
		tmp_dict= self.files['/']		
		for x in npath:
			tmp_dict=tmp_dict[str(x)]
		attrs = tmp_dict.setdefault('attrs',{})	
		attrs[name] = value
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR


    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
       # self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
        #                          st_size=len(source))

	npath = self.add_path(target)
	tmp_dict = self.files['/']
	if npath[0] == '':
		self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
	else:
		for x in npath:
			tmp_dict = tmp_dict(str(x))
		tmp_dict = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))

        self.data[target] = source

    def truncate(self, path, length, fh=None):
      
	self.data[path] = self.data[path][:length]	
	npath= self.add_path(path)
	tmp_dict = self.files['/']
	if npath[0] == '':
		self.files['/']['st_size'] = length
	else:
		for x in npath[0:len(npath)-2]:
			tmp_dict = tmp_dict[str(x)]
		tmp_dict['st_size'] = length

    def unlink(self, path):						#traverse the path and unlink the path name from metadata
       
	npath= self.add_path(path)
	tmp_dict = self.files['/']
	for x in npath:	
		if x == npath[-1]:
			tmp_dict.pop(npath[-1])
		else:
			tmp_dict = tmp_dict[str(x)]
		

    def utimens(self, path, times=None):				#traverse the path and modify the time attributes in metadata
        now = time()
        atime, mtime = times if times else (now, now)

	npath = self.add_path(path)
	if npath[0]=='':
		self.files['/']['st_atime'] = atime
		self.files['/']['st_mtime'] = mtime
	else:
		tmp_dict= self.files['/']
		for x in npath:
			tmp_dict=tmp_dict[str(x)]
		print(tmp_dict)	
		tmp_dict['st_atime'] = atime
		tmp_dict['st_mtime'] = mtime

    def write(self, path, data, offset, fh):				#Traverse the path to change the metadata and add the data to the data
									#dictionary with path name as key.
      	npath = self.add_path(path)
	if npath[0] == '':
		self.data[path] = self.data[path][:offset] + data
		self.files['/']['st_size'] = len(self.data[path])

		return len(data)
	else:
		tmp_dict=self.files['/']
		for x in npath:
			tmp_dict = tmp_dict[str(x)]
		self.data[path] = self.data[path][:offset] + data
		tmp_dict['st_size'] = len(self.data[path])
		return len(data)  				

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True, debug=True)
