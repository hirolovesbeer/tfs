#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import os.path
import sys
import glob
import subprocess
import yaml

import hdfs3
from hdfs3 import HDFileSystem

import __builtin__

class TransparentFileSystem(object):
    def __init__(self, base=False):
        if base is False:
            self.hdfs_flag = False
            # self.base = os
            self.base = __builtin__
            return 
        else:
            self.hdfs_flag = True
            self.base = base

    def __getattribute__(self, name):
        # warning: to access members like "self.xxx" cause infinity recursive call
        print '__getattribute__'
        print object.__getattribute__(self, 'base')
        if (object.__getattribute__(self, 'hdfs_flag')) is True:
            print 'hdfs3 method call: ' + name
            return getattr(object.__getattribute__(self, 'base'), name)
        else:
            print '__getattribute__ ' + name
            return object.__getattribute__(self, name)

    def __getattr__(self, name):
        # if method missing, call builtin functions
        print '__getattr__'
        return getattr(object.__getattribute__(self, 'base'), name)
 
    def cat(self, path):
        f = open(path)
        read_data = file.readlines(f) 
        f.close()
        return read_data

    def chmod(self, path, mod):
        return os.chmod(path)

    def chwon(self, path, owner, group):
        return os.chown(path, owner, group)

    def df(self, path):
        df = subprocess.Popen(["/bin/df", path], stdout=subprocess.PIPE)
        return df.communicate()[0]
        
    def du(self, path):
        du = subprocess.Popen(["/usr/bin/du", path], stdout=subprocess.PIPE)
        return du.communicate()[0]

    def exists(self, path):
        return os.path.exists(path)
        
    def glob(self, path):
        return glob.glob(path)

    def stat(self, path):
        return os.stat(path)

    def ls(self, path):
        return os.listdir(path)

if __name__ == "__main__":
    # load the hdfs node info
    f = open('hdfs.yml', 'r')
    data = yaml.load(f)
    f.close()

    hdfs_nn = data['hdfs_nn']
    hdfs = HDFileSystem(host=hdfs_nn, port=data['hdfs_port'])

    tfs = TransparentFileSystem(hdfs)
    print hdfs.exists('/tmp')
    # print hdfs.hoge('/tmp')
    print tfs.exists('/tmp')
    # print tfs.hoge('/tmp')

    # tfs_local = TransparentFileSystem()
    # print tfs_local.glob('/var/tmp')

    print 'test'
    print tfs.glob('/tmp')
    # tfs.hoge()
    tfs_local = TransparentFileSystem()
    # print tfs_local.glob('/home/vagrant/work/data/*')
    # tfs_local.hoge()

    # print tfs.hoge()
    print tfs_local.hex(255)

    print tfs.info('/tmp/csv/json_dir_httparchive_Aug_1_2014_pages_urls_kw_print_domain_ip_byte_type.dat')
    print tfs.get_block_locations('/tmp/csv/json_dir_httparchive_Aug_1_2014_pages_urls_kw_print_domain_ip_byte_type.dat')
    # print tfs.cat('/tmp/csv/json_dir_httparchive_Aug_1_2014_pages_urls_kw_print_domain_ip_byte_type.dat')
    dir = '/home/vagrant/work/data'
    file =  '/home/vagrant/work/data/json_dir_httparchive_Aug_1_2014_pages_urls_kw_print_domain_ip_byte_type.dat'
    print tfs_local.df(file)
    print tfs_local.du(file)
    print tfs_local.exists(file)
    print tfs_local.stat(file)
    # print tfs_local.ls(dir)

    sys.exit(0)
