from functools import wraps
from key_value_store import KeyValueStore


def _transaction_block_not_empty(fn):
		@wraps(fn)
		def wrapper(instance, *args, **kwargs):
			if not instance.transaction_blocks:
				return None
			return fn(instance, *args, **kwargs)
		return wrapper


class WriteAheadLog(object):
	def __init__(self):
		self.transaction_blocks = []

	def __repr__(self):
		return self.transaction_blocks.__repr__()

	def _add_new_transaction_block(self):
		"""begin a new transaction block by adding a new entry
		to the list of transaction blocks"""
		self.transaction_blocks.append(KeyValueStore())

	@_transaction_block_not_empty
	def _has_open_transaction_blocks(self):
		return True

	@_transaction_block_not_empty
	def _commit(self):
		"""commit all open transaction blocks to memory"""
		# clear the write ahead log
		self.transaction_blocks = []
		return True

	@_transaction_block_not_empty
	def _rollback(self):
		"""rollback last transaction block"""
		return self.transaction_blocks.pop()

	@_transaction_block_not_empty
	def _get_last_transaction(self, last_transaction_index=1):
		return self.transaction_blocks[-last_transaction_index]

	@_transaction_block_not_empty
	def _key_in_transaction(self, transaction, key):
		return key in transaction.keys

	def _add_new_transaction(self, **kwargs):
		"""adds a new transaction to the last open block"""
		current_transaction = self._get_last_transaction()
		key, value, prev = kwargs.get('key'), kwargs.get('value'), kwargs.get('prev')
		# if they key has been seen before in the current transaction block
		if self._key_in_transaction(current_transaction, key):
			# ignore the prev sent in to this function, and use
			# the old prev associated with the key
			prev = current_transaction._retrieve(key)['prev']
		current_transaction._add(key, dict(cur=value, prev=prev))
		# add the updated current transaction to the list of transactions
		self.transaction_blocks[-1] = current_transaction
