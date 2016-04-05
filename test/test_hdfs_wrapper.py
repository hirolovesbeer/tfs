import unittest

from tfs import hdfs_wrapper

class TestTransparentFileSystem(unittest.TestCase):
	def setUp(self):
		self.tfs = hdfs_wrapper.TransparentFileSystem()
	
	def test_init(self):
		self.assertIsInstance(self.tfs, hdfs_wrapper.TransparentFileSystem)

	def test_exists(self):
		self.assertTrue(self.tfs.exists('/tmp'))

	def test_cat(self):
		return

	def test_chmod(self):
		return

if __name__ == '__main__':
	unittest.main()
