class KeyValueStore(object):
	"""a very generic key value store, which is used as
	1. the main in memory key value store
	2. the inverted key value store to implement _get_numequalto
	"""
	def __init__(self):
		self._in_memory_cache = dict()

	def __getitem__(self, key):
		return self._in_memory_cache[key]

	def __repr__(self):
		return self._in_memory_cache.__repr__()

	@property
	def keys(self):
		return self._in_memory_cache.keys()

	def _add(self, key, value):
		self._in_memory_cache[key] = value

	def _remove(self, key):
		if self._in_memory_cache.get(key) is not None:
			return self._in_memory_cache.pop(key)

	def _retrieve(self, key):
		return self._in_memory_cache.get(key)

	def _add_to_value_cache(self, key, value):
		# if key is already present
		if self._in_memory_cache.get(key):
			# add the value to the set
			self._in_memory_cache[key].add(value)
		else:
			# else, create a new set with the value as
			# the only element
			self._in_memory_cache[key] = {value}

	def _remove_from_value_cache(self, key, value):
		if self._in_memory_cache.get(key):
			# remove the value from the set of values associated with key
			self._in_memory_cache[key].remove(value)
			# if after removing the value, there are no more values
			# associated with the key, remove the key entirely from value_cache
			if not self._in_memory_cache[key]:
				self._in_memory_cache.pop(key)

	def _get_numequalto(self, value):
		names = self._retrieve(value)
		return len(names) if names else 0
