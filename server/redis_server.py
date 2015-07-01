import sys
from write_ahead_log import WriteAheadLog
from key_value_store import KeyValueStore


class RedisServer(object):
	def __init__(self):
		self._write_ahead_log = WriteAheadLog()
		self.cache = KeyValueStore()
		self.value_cache = KeyValueStore()

	def __call__(self, command, *args):
		"""call the appropriate method based on the command passed in"""
		return getattr(self, '_' + command.lower())(*args)

	def _set(self, name, value):
		"""Set the variable 'name' to the value 'value'."""
		# if there's a transaction open, add this operation to it
		if self._write_ahead_log._has_open_transaction_blocks():
			# along with the key and value, we also pass
			# the 'current value' (retrieed from the cache) for the given key.
			# Since this is done before writing the new value to the cache,
			# this retrieves the previous value for the key, if it exists.
			# If the same key appears twice in any
			# transaction block, the write ahead log takes
			# care of only storing the *first* previous value, the one associated
			# with a parent transaction of the current transaction.
			self._write_ahead_log._add_new_transaction(
				prev=self.cache._retrieve(name), key=name, value=value)
		# After writing to the write ahead log, add the key, value
		# pair to the value cache.
		# First remove the old value of key from the value cache
		self.value_cache._remove_from_value_cache(self.cache._retrieve(name), name)
		# Add the new value of key to the value cache
		self.value_cache._add_to_value_cache(value, name)
		# Add the key, value pair to the main cache
		self.cache._add(name, value)

	def _unset(self, name):
		"""Unset the variable name, making it just like that
		the variable was never set."""
		# if there's a transaction block open
		# add the unset operation to it by setting the new value to None
		if self._write_ahead_log._has_open_transaction_blocks():
			self._write_ahead_log._add_new_transaction(
				prev=self.cache._retrieve(name), key=name, value=None)
		# Remove key from the main cache
		value = self.cache._remove(name)
		# Update value cache
		self.value_cache._remove_from_value_cache(value, name)

	def _get(self, name):
		"""Print out the value of the variable name,
		or NULL if that variable is not set."""
		value = self.cache._retrieve(name)
		return value if value else "NULL"

	def _numequalto(self, value):
		"""Print out the number of variables that are currently set to value.
		If no variables equal that value, print 0."""
		return self.value_cache._get_numequalto(value)

	def _begin(self):
		"""add a new empty transaction block to the write ahead log"""
		self._write_ahead_log._add_new_transaction_block()

	def _commit(self):
		"""Close all open transaction blocks, permanently applying
		the changes made in them. Print nothing if successful,
		or print NO TRANSACTION if no transaction is in progress."""
		if not self._write_ahead_log._commit():
			return "NO TRANSACTION"

	def _rollback(self):
		"""Undo all of the commands issued in the most recent transaction
		block, and close the block. Print nothing if successful,
		or print NO TRANSACTION if no transaction is in progress."""
		last_transaction = self._write_ahead_log._rollback()
		# if there's no open transaction block, nothing to rollback
		if last_transaction is None:
			return "NO TRANSACTION"
		# if there is an open transaction block,
		# iterate through all the keys in the block
		for key in last_transaction.keys:
			# set the key's value in the main cache to
			# the previous value stored in the transaction block
			previous_value = last_transaction[key]['prev']
			# if there is no previous value, then the key was set for
			# the first time in the transaction requiring rollback.
			# Remove the key from the cache.
			if not previous_value:
				self.cache._remove(key)
			else:
				self.cache._add(key, previous_value)
				# add the previous value back to the value cache
				self.value_cache._add_to_value_cache(previous_value, key)
			# remove the current value associated with the key from the value cache
			self.value_cache._remove_from_value_cache(last_transaction[key]['cur'], key)

	def _end(self):
		"""Exit the program."""
		# flush the write ahead log
		self._write_ahead_log._commit()
		sys.exit()
