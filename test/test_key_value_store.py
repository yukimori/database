import unittest
import sys
import os.path
sys.path.append(
	os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from server.key_value_store import KeyValueStore


class TestKeyValueStore(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super(TestKeyValueStore, self).__init__(*args, **kwargs)
		self.cache = KeyValueStore()

	def setUp(self, *args, **kwargs):
		# set up a different cache for testing the value_cache functionality
		# shortDescription retrieves the first line of the docstring of the test
		test_name = self.shortDescription()
		# 3 of the tests test the value_cache, so initializing the cache accordingly
		if 'value_cache' in test_name:
			key_values = {x: y for x, y in zip(
				xrange(1, 4), [['f'], ['d', 'e'], ['a', 'b', 'c']])}
		else:
			key_values = {x: n for x, n in zip(
				['a', 'b', 'c', 'd', 'e'], xrange(0, 5))}
		for key, value in key_values.iteritems():
			self.cache._add(key, value)

	def tearDown(self, *args, **kwargs):
		pass

	def test_add(self):
		"""test that adding a key to the cache works"""
		key, value = 'f', 100
		# assert that the key 'f' is not in the cache
		self.assertIsNone(self.cache._retrieve(key))
		# add key 'f' to cache
		self.cache._add(key, value)
		# assert key exists in cache
		self.assertEqual(self.cache._retrieve(key), value)

	def test_remove(self):
		"""test that removing a key from the cache works"""
		for index, key in enumerate(['a', 'b', 'c', 'd', 'e']):
			# remove the key from the cache
			self.cache._remove(key)
			# assert that the key has been removed
			self.assertIsNone(self.cache._retrieve(key))

	def test_retrieve(self):
		"""test retrieving a value from the cache works"""
		for index, key in enumerate(['a', 'b', 'c', 'd', 'e']):
			self.assertEqual(self.cache._retrieve(key), index)

	def test_add_to_value_cache(self):
		"""test adding an entry to the value_cache works"""
		self.assertEqual(len(self.cache._retrieve(1)), 1)
		# add another entry to the value 1
		self.cache._add_to_value_cache(1, 'z')
		self.assertEqual(len(self.cache._retrieve(1)), 2)
		# add an entry that does not exist as yet
		self.assertIsNone(self.cache._retrieve(4))
		self.cache._add_to_value_cache(4, 'y')
		self.assertEqual(len(self.cache._retrieve(4)), 1)

	def test_remove_from_value_cache(self):
		"""test that removing a value from the value_cache works"""
		self.assertEqual(len(self.cache._retrieve(1)), 1)
		self.cache._remove_from_value_cache(1, 'f')
		self.assertIsNone(self.cache._retrieve(1))
		self.assertEqual(len(self.cache._retrieve(2)), 2)
		self.cache._remove_from_value_cache(2, 'd')
		self.assertEqual(len(self.cache._retrieve(2)), 1)

	def test_numequalto(self):
		"""test that the value_cache returns the right count"""
		for x in xrange(1, 4):
			self.assertEqual(self.cache._get_numequalto(x), x)
		self.cache._add_to_value_cache(1, 'z')
		self.assertEqual(self.cache._get_numequalto(1), 2)


if __name__ == '__main__':
	unittest.main()
