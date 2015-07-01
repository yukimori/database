import unittest
from redis_server import RedisServer


class TestRedisServer(unittest.TestCase):
	def __init__(self, *args, **kwargs):
		super(TestRedisServer, self).__init__(*args, **kwargs)
		self.input_files = ['input{}'.format(x) for x in xrange(1, 7)]
		self.redis_server = RedisServer()

	def setUp(self, *args, **kwargs):
		super(TestRedisServer, self).setUp(*args, **kwargs)

	def tearDown(self, *args, **kwargs):
		super(TestRedisServer, self).tearDown(*args, **kwargs)

	def test_set(self):
		pass

	def test_unset(self):
		pass

	def test_get(self):
		pass

	def test_numequalto(self):
		pass

	def test_begin(self):
		pass

	def test_end(self):
		pass

	def test_commit(self):
		pass

	def test_rollback(self):
		pass
