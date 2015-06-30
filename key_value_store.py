class KeyValueStore(object):
	"""a very generic key value store, which is used as
	1. the main in memory key value store
	2. the inverted key value store to implement _get_numequalto
	"""
	def __init__(self):
		# 925.930.7959
		self._in_memory_cache = dict()

	@property
	def keys(self):
		return self._in_memory_cache.keys()

	def __getitem__(self, key):
		return self._in_memory_cache[key]

	def __repr__(self):
		return str(self._in_memory_cache)

	def _add(self, key, value):
		if value is None:
			self._remove(key)
			return
		self._in_memory_cache[key] = value

	def _remove(self, key):
		if self._in_memory_cache.get(key):
			return self._in_memory_cache.pop(key)

	def _retrieve(self, key):
		return self._in_memory_cache.get(key)

	def _add_to_value_cache(self, key, value):
		if not key:
			return
		if self._in_memory_cache.get(key):
			self._in_memory_cache[key].add(value)
		else:
			self._in_memory_cache[key] = {value}

	def _remove_from_value_cache(self, key, value):
		if self._in_memory_cache.get(key):
			list_of_values = self._in_memory_cache[key]
			if (len(list_of_values)) > 1:
				self._in_memory_cache[key].remove(value)
			else:
				self._in_memory_cache.pop(key)

	def _get_numequalto(self, value):
		names = self._retrieve(value)
		return len(names) if names else 0
