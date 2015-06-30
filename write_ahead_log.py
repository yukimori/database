from key_value_store import KeyValueStore
from functools import wraps


class WriteAheadLog(object):
	def __init__(self):
		self.transaction_blocks = []

	def __repr__(self):
		return str(self.transaction_blocks)

	def _transaction_block_not_empty(fn):
		@wraps(fn)
		def wrapper(instance, *args, **kwargs):
			if not instance.transaction_blocks:
				return None
			return fn(instance, *args, **kwargs)
		return wrapper

	def _add_new_transaction_block(self):
		"""begin a new transaction block by adding a new entry
		to the list of transaction blocks"""
		self.transaction_blocks.append(KeyValueStore())

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
		return key in transaction.keys

	# def _get_previous_value(self, key):
	# 	# look for the value of key in any of the parent transactions
	# 	parent_transaction = 2
	# 	while (parent_transaction <= len(self.transaction_blocks)):
	# 		previous_transaction = self._get_last_transaction(parent_transaction)
	# 		if self._key_in_transaction(previous_transaction, key):
	# 			return previous_transaction[key]['cur']
	# 		parent_transaction -= 1
	# 	return None

	def _add_new_transaction(self, **kwargs):
		current_transaction = self._get_last_transaction()
		key, value, prev = kwargs.get('key'), kwargs.get('value'), kwargs.get('prev')
		# if the key isn't in the current transaction,
		# set it's previous value
		# if not self._key_in_transaction(current_transaction, key):
		# 	# prev = self._get_previous_value(key)
		# 	current_transaction._add(key, dict(cur=value, prev=prev))
		# else:
		# 	# if the key has already been seen once in the current
		# 	# open transaction block, only change it's value
		# 	prev = current_transaction._retrieve(key)['prev']
		if self._key_in_transaction(current_transaction, key):
			prev = current_transaction._retrieve(key)['prev']
		current_transaction._add(key, dict(cur=value, prev=prev))
		# add the updated current transaction to the list of transactions
		self.transaction_blocks[-1] = current_transaction
