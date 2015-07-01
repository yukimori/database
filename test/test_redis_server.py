import unittest
import sys
import os.path
sys.path.append(
	os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from server.redis_server import RedisServer


class TestRedisServer(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super(TestRedisServer, self).__init__(*args, **kwargs)
		self.redis_server = RedisServer()
		self.key_values = {'a': 10, 'b': 10, 'c': 30}

	def setUp(self, *args, **kwargs):
		super(TestRedisServer, self).setUp(*args, **kwargs)

	def tearDown(self, *args, **kwargs):
		super(TestRedisServer, self).tearDown(*args, **kwargs)

	def _clear_cache(self):
		self.redis_server = RedisServer()

	def test_set(self):
		"""test that the set method works as expected"""
		# test set without transactions
		for key, value in self.key_values.iteritems():
			self.redis_server('set', key, value)
			self.assertEqual(self.redis_server('get', key), value)
		# test set with transaction
		self._clear_cache()
		self.redis_server('begin')
		for key, value in self.key_values.iteritems():
			self.redis_server('set', key, value)
			self.assertEqual(self.redis_server('get', key), value)
		self.assertEqual(self.redis_server.value_cache._retrieve(10), {'a', 'b'})
		for key, value in self.key_values.iteritems():
			self.assertEqual(
				self.redis_server._write_ahead_log._get_last_transaction()._retrieve(key),
				{'prev': None, 'cur': value})

	def test_unset(self):
		"""test that unset works as expected"""
		for key, value in self.key_values.iteritems():
			self.redis_server('set', key, value)
			self.assertEqual(self.redis_server('get', key), value)
			self.redis_server('unset', key)
			self.assertEqual("NULL", self.redis_server('get', key))

	def test_get(self):
		"""test that get works"""
		self.assertEqual("NULL", self.redis_server('get', 'a'))
		for key, value in self.key_values.iteritems():
			self.redis_server('set', key, value)
		for key, value in self.key_values.iteritems():
			self.assertEqual(self.redis_server('get', key), value)
		# test that get works whe used in transctions
		self._clear_cache()
		self.redis_server('begin')
		for key, value in self.key_values.iteritems():
			self.redis_server('set', key, value)
			self.assertEqual(self.redis_server('get', key), value)
		self.redis_server('begin')
		for key, value in self.key_values.iteritems():
			self.redis_server('set', key, value + 1)
			self.assertEqual(self.redis_server('get', key), value + 1)

	def test_begin(self):
		"""test that the begin command works"""
		write_ahead_log = self.redis_server._write_ahead_log
		self.assertFalse(write_ahead_log._has_open_transaction_blocks())
		self.redis_server('begin')
		self.assertTrue(write_ahead_log._has_open_transaction_blocks)

	def test_numequalto(self):
		"""test that numequalto works as expected"""
		for key, value in self.key_values.iteritems():
			self.redis_server('begin')
			self.redis_server('set', key, value)
		self.assertEqual(self.redis_server('numequalto', 10), 2)
		self.assertEqual(self.redis_server('numequalto', 30), 1)
		# unset c in a new transaction
		self.redis_server('begin')
		self.redis_server('unset', 'c')
		self.assertEqual(self.redis_server('numequalto', 30), 0)
		self.redis_server('rollback')
		# verify c is set again after rollback
		self.assertEqual(self.redis_server('numequalto', 30), 1)

	def test_commit(self):
		"""test that the commit command works"""
		self.assertEqual(self.redis_server('commit'), "NO TRANSACTION")
		for key, value in self.key_values.iteritems():
			self.redis_server('begin')
			self.redis_server('set', key, value)
			self.assertEqual(self.redis_server('get', key), value)
		write_ahead_log = self.redis_server._write_ahead_log
		self.assertEqual(len(write_ahead_log.transaction_blocks), 3)
		self.redis_server('commit')
		self.assertEqual(len(write_ahead_log.transaction_blocks), 0)
		self.assertEqual(self.redis_server('commit'), "NO TRANSACTION")

	def test_rollback_set(self):
		"""test that rollbacks work as expected for the set command"""
		# create 3 transactions, each with 3 variables a, b and c, and
		# different values for a, b and c in each new transaction
		for x in xrange(0, 3):
			self.redis_server('begin')
			for key, value in self.key_values.iteritems():
				# increment the value of the key for each new transaction
				self.redis_server('set', key, value + x)
				self.assertEqual(self.redis_server('get', key), value + x)
		for x in [0, 3]:
			# get the transaction which will be rolled back
			rollback_trans = self.redis_server._write_ahead_log._get_last_transaction()
			# rollback the last transaction
			self.redis_server('rollback')
			for key, value in self.key_values.iteritems():
				rollback_val = rollback_trans._retrieve(key)
				# verify that after rollback, the value associated with the key
				# is the value associated with the 'prev' field in transaction
				# that was rolled back
				self.assertEqual(self.redis_server('get', key), rollback_val['prev'])

	def test_rollback_unset(self):
		"""test the unset operation during rollback"""
		self.redis_server('begin')
		self.redis_server('set', 'x', 100)
		self.redis_server('begin')
		self.redis_server('unset', 'x')
		self.assertEqual("NULL", self.redis_server('get', 'x'))
		self.redis_server('rollback')
		self.assertEqual(self.redis_server('get', 'x'), 100)
