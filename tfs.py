#!/usr/bin/env python

import sys

import glob
import hdfs3
from hdfs3 import HDFileSystem

hdfs_nn = 'name node ip addr'
hdfs = HDFileSystem(host=hdfs_nn, port=8020)

class TransparentFileSystem:
    def __init__(self):
        self.hdfs_flag = False
        return 

    def set_hdfs_flag(self, flag=True):
        self.hdfs_flag = flag

    def exists(self, target):
        if hdfs.exists(target) is True:
            print target + ' This dir is HDFS.'
            self.hdfs_flag = True
        else:
            print target + ' This dir is not HDFS. Local FS.'

            # if os.path.exists('')

    def glob(self, target):
        if self.hdfs_flag is True:
            return hdfs.glob(target)
        else:
            return glob.glob(target)

if __name__ == "__main__":
    tfs_hdfs = TransparentFileSystem()
    tfs_hdfs.exists('/tmp')
    print tfs_hdfs.hdfs_flag
    print tfs_hdfs.glob('/tmp')

    tfs_local = TransparentFileSystem()
    tfs_local.exists('dir to local')

    tfs_local.set_hdfs_flag(False)
    print tfs_local.hdfs_flag
    print tfs_local.glob('dir to local')

    sys.exit(0)
