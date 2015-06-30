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
		# if there's a transaction open, add this operation to it
		if self._write_ahead_log._has_open_transaction_blocks():
			# along with the name and value, we also pass
			# the current cache value for the given key. Since this
			# is done before writing the new value to the cache,
			# this retrieves the value stored in the last
			# transaction. If the same key appears twice in any
			# transaction block, the write ahead log takes
			# care of only storing the previous value associated with
			# key *once*.
			self._write_ahead_log._add_new_transaction(
				prev=self.cache._retrieve(name), key=name, value=value)
		# After writing to the write ahead log, add the key, value
		# pair to the cache and value cache.
		self.value_cache._remove_from_value_cache(self.cache._retrieve(name), name)
		self.value_cache._add_to_value_cache(value, name)
		self.cache._add(name, value)

	def _get(self, name):
		value = self.cache._retrieve(name)
		return value if value else "NULL"

	def _unset(self, name):
		"""removes the key 'name' from the cache if it exists;
		adds a new transaction if the write_ahead_log is not empty."""
		# if there's a transaction block open
		# add the unset operation to it by setting the value to None
		if self._write_ahead_log._has_open_transaction_blocks():
			self._write_ahead_log._add_new_transaction(
				prev=self.cache._retrieve(name), key=name, value=None)
		value = self.cache._remove(name)
		self.value_cache._remove_from_value_cache(value, name)

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
		# if there's no open transaction block, then return
		if last_transaction is None:
			print "NO TRANSACTION"
			return
		# if there is an open transaction block,
		# iterate through all the keys in the block
		for key in last_transaction.keys:
			# set the key's value in the cache to
			# the previous value stored in the transaction block
			self.cache._add(key, last_transaction[key]['prev'])
			# remove the current value from the value cache
			self.value_cache._remove_from_value_cache(last_transaction[key]['cur'], key)
			# add the previous value back to the value cache
			self.value_cache._add_to_value_cache(last_transaction[key]['prev'], key)

	def _end(self):
		# flush the write ahead log to cache
		self._write_ahead_log._commit()
		# flush cache to persistent storage
		sys.exit()
