import sys
from write_ahead_log import WriteAheadLog
from key_value_store import KeyValueStore


class RedisServer(object):
	def __init__(self):
		# keep a write ahead log of open transactions
		self._write_ahead_log = WriteAheadLog()
		# use the generic KeyValueStore as the in memory cache
		self.cache = KeyValueStore()
		# an inverted index type of data structure would
		# be required to perform the NUMEQUALTO operation.
		# for O(logn) NUMEQUALTO, a data structure
		# that supports either O(logn) or O(1) lookups
		# is required. If the transactions are
		# going to deal with a small number of keys,
		# repeatedly, then having a size limited LRU value cache
		# would serve the purpose. Alternatively, one could
		# use something like a Balanced Binary Search Tree, but
		# it seems a bit of an overkill here.
		self.value_cache = KeyValueStore()

	def __call__(self, command, *args):
		"""call the appropriate method based on the command"""
		return getattr(self, '_' + command.lower())(*args)

	def _set(self, name, value):
		self.cache._add(name, value)
		self.value_cache._add_to_value_cache(value, name)
		# if there's a transaction open, add this operation to it
		if self._write_ahead_log._has_open_transaction_blocks():
			self._write_ahead_log._add_new_transaction(name, value)

	def _get(self, name):
		print self.cache._retrieve(name)

	def _unset(self, name):
		"""removes the key 'name' from the cache if it exists;
		adds a new transaction if the write_ahead_log is not empty."""
		value = self.cache._remove(name)
		self.value_cache._remove_from_value_cache(value, name)
		# if there's a transaction block open
		# add the unset operation to it
		if self._write_ahead_log._has_open_transaction_blocks():
			self._write_ahead_log._add_new_transaction(name, None)

	def _numequalto(self, value):
		print self.value_cache._get_numequalto(value)

	def _begin(self):
		"""add a new empty transaction block to the write ahead log"""
		self._write_ahead_log._add_new_transaction_block()

	def _commit(self):
		if not self._write_ahead_log._commit():
			print "NO TRANSACTION"

	def _rollback(self):
		last_transaction = self._write_ahead_log._rollback()
		if not last_transaction:
			print "NO TRANSACTION"
			return
		for name, value in last_transaction.iteritems():
			self.cache._add(name, value['prev'])

	def _end(self):
		# flush the write ahead log to cache
		self._write_ahead_log._commit()
		# flush cache to persistent storage
		sys.exit()
