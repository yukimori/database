import unittest
import sys
import os.path
sys.path.append(
	os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
from server.write_ahead_log import WriteAheadLog
from server.key_value_store import KeyValueStore


class TestWriteAheadLog(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super(TestWriteAheadLog, self).__init__(*args, **kwargs)
		self.write_ahead_log = WriteAheadLog()

	def test_add_new_transaction_block(self):
		self.assertListEqual(self.write_ahead_log.transaction_blocks, [])
		self.write_ahead_log._add_new_transaction_block()
		self.assertNotEqual(self.write_ahead_log.transaction_blocks, [])

	def test_has_open_transaction_block(self):
		self.assertFalse(self.write_ahead_log._has_open_transaction_blocks())
		self.write_ahead_log._add_new_transaction_block()
		self.assertTrue(self.write_ahead_log._has_open_transaction_blocks())

	def test_get_last_transaction(self):
		self.assertIsNone(self.write_ahead_log._get_last_transaction())
		self.write_ahead_log._add_new_transaction_block()
		last_transaction_block = self.write_ahead_log._get_last_transaction()
		self.assertIsInstance(last_transaction_block, KeyValueStore)
		self.write_ahead_log._add_new_transaction(prev=None, key='z', value=1)
		self.write_ahead_log._add_new_transaction(prev=None, key='a', value=10)
		self.write_ahead_log._add_new_transaction(prev=None, key='n', value=60)
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertEqual(last_trans._retrieve('z'), {'prev': None, 'cur': 1})
		self.assertEqual(last_trans._retrieve('a'), {'prev': None, 'cur': 10})
		self.assertEqual(last_trans._retrieve('n'), {'prev': None, 'cur': 60})
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=1, key='z', value=10)
		self.write_ahead_log._add_new_transaction(prev=None, key='x', value=99)
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertEqual(last_trans._retrieve('z'), {'prev': 1, 'cur': 10})
		self.assertEqual(last_trans._retrieve('x'), {'prev': None, 'cur': 99})

	def test_key_in_transaction(self):
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertFalse(self.write_ahead_log._key_in_transaction(last_trans, 'z'))
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=None, key='z', value=1)
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertTrue(self.write_ahead_log._key_in_transaction(last_trans, 'z'))
		self.assertFalse(self.write_ahead_log._key_in_transaction(last_trans, 'a'))

	def test_add_new_transaction(self):
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=None, key='a', value=10)
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertEqual(last_trans._retrieve('a'), {'prev': None, 'cur': 10})
		self.write_ahead_log._add_new_transaction_block()
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertNotIn('a', last_trans.keys)
		first_transaction = self.write_ahead_log._get_last_transaction(2)
		self.assertIn('a', first_transaction.keys)
		self.write_ahead_log._add_new_transaction(prev=10, key='a', value=55)
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertEqual(last_trans._retrieve('a'), {'prev': 10, 'cur': 55})
		self.write_ahead_log._add_new_transaction(prev=55, key='a', value=None)
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertEqual(last_trans._retrieve('a'), {'prev': 10, 'cur': None})

	def test_commit(self):
		self.assertIsNone(self.write_ahead_log._commit())
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=None, key='a', value=10)
		self.assertTrue(self.write_ahead_log._commit())
		self.assertEqual(self.write_ahead_log.transaction_blocks, [])
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=None, key='a', value=10)
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=10, key='a', value=55)
		self.assertTrue(self.write_ahead_log._commit())
		self.assertEqual(self.write_ahead_log.transaction_blocks, [])

	def test_rollback(self):
		self.assertIsNone(self.write_ahead_log._rollback())
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=None, key='a', value=10)
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=10, key='a', value=90)
		self.write_ahead_log._add_new_transaction_block()
		self.write_ahead_log._add_new_transaction(prev=90, key='a', value=55)
		rolled_back_trans = self.write_ahead_log._rollback()
		self.assertEqual(rolled_back_trans._retrieve('a'), {'prev': 90, 'cur': 55})
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertEqual(last_trans._retrieve('a'), {'prev': 10, 'cur': 90})
		self.write_ahead_log._rollback()
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertEqual(last_trans._retrieve('a'), {'prev': None, 'cur': 10})
		self.write_ahead_log._rollback()
		last_trans = self.write_ahead_log._get_last_transaction()
		self.assertIsNone(last_trans)

if __name__ == '__main__':
	unittest.main()
