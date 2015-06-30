class KeyValueStore(object):
	"""a very generic key value store, which is used as
	1. the main in memory key value store
	2. the inverted key value store to implement _get_numequalto
	3. an abstraction for a single transaction block"""
	def __init__(self):
		self._in_memory_cache = dict()

	def _add(self, key, value):
		self._in_memory_cache[key] = value

	def _remove(self, key):
		return self._memory_cache.pop(key)

	def _retrieve(self, key):
		value = self._in_memory_cache.get(key)
		return value if value else "NULL"

	def _add_to_value_cache(self, key, value):
		if self._in_memory_cache.get(key):
			self._in_memory_cache[key].add(value)
		else:
			self._in_memory_cache[key] = {value}

	def _remove_from_value_cache(self, key, value):
		if self._in_memory_cache.get(key):
			self._in_memory_cache[key].remove(value)

	def _get_numequalto(self, value):
		names = self._in_memory_cache.get(value)
		return len(names) if names else 0
