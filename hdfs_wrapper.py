#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import os.path
import sys
import glob

import hdfs3
from hdfs3 import HDFileSystem

class TransparentFileSystem(object):
    def __init__(self, base=False):
        if base is False:
            self.hdfs_flag = False
            self.base = os
            return 
        else:
            self.hdfs_flag = True
            self.base = base

    def __getattribute__(self, name):
        # メンバ(selfに.でアクセス)にアクセスするたびに __getattribute__が呼び出されるので無限再帰...
        print '__getattribute__'
        print object.__getattribute__(self, 'base')
        if (object.__getattribute__(self, 'hdfs_flag')) is True:
            print 'hdfs3 method call: ' + name
            return getattr(object.__getattribute__(self, 'base'), name)
        else:
            print '__getattribute__ ' + name
            return object.__getattribute__(self, name)
        
#    def __getattr__(self, name):
#        print '__getattr__'
#        def _method_missing( *args ):
#            return args
#
#        if (hasattr(self.base, name)) is False:
#            raise AttributeError(self.base.__class__.__name__ + ' object has no attribute ' + name)
#
#        return getattr(self.base, name, _method_missing)

    def glob(self, target):
        return glob.glob(target)

if __name__ == "__main__":
    hdfs_nn = '172.16.126.141'
    hdfs = HDFileSystem(host=hdfs_nn, port=8020)

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
    print tfs_local.glob('/home/vagrant/work/data/*')
    # tfs_local.hoge()

    sys.exit(0)
