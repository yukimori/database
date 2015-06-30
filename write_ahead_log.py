from key_value_store import KeyValueStore
from functools import wraps


class WriteAheadLog(object):
	def __init__(self):
		self.transaction_blocks = []

	def _add_new_transaction_block(self):
		"""begin a new transaction block by adding a new entry
		to the list of transaction blocks"""
		self.transaction_blocks.append(KeyValueStore())

	def _transaction_block_not_empty(fn):
		@wraps(fn)
		def wrapper(instance, *args, **kwargs):
			if not instance.transaction_blocks:
				return None
			return fn(instance, *args, **kwargs)
		return wrapper

	@_transaction_block_not_empty
	def _get_last_transaction_block(self):
		return self.transaction_blocks[-1]

	@_transaction_block_not_empty
	def _has_open_transaction_blocks(self):
		return True

	@_transaction_block_not_empty
	def _commit(self):
		"""commit all open transaction blocks to memory"""
		self.transaction_blocks = []
		return True

	@_transaction_block_not_empty
	def _rollback(self):
		"""rollback last transaction block"""
		return self.transaction_blocks.pop()

	@_transaction_block_not_empty
	def _get_last_transaction(self, last_transaction_index=1):
		return self.transaction_blocks[-last_transaction_index]

	def _key_in_transaction(self, transaction, key):
		return key in transaction.keys()

	def _get_previous_value(self, key):
		# look for the value of key in any of the parent transactions
		parent_transaction = 2
		while (parent_transaction <= len(self.transaction_blocks)):
			previous_transaction = self._get_last_transaction(parent_transaction)
			if self._key_in_transaction(previous_transaction, key):
				return previous_transaction[key]['cur']
			parent_transaction -= 1
		return None

	def _add_new_transaction(self, key, value):
		current_transaction = self._get_last_transaction(1)
		# if the key isn't in the current transaction,
		# get it's previous value
		if not self._key_in_transaction(current_transaction, key):
			prev = self._get_previous_value(key)
			current_transaction[key] = dict(cur=value, prev=prev)
		else:
			current_transaction[key]['cur'] = value
		self.transaction_blocks[-1] = current_transaction
