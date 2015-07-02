import sys
from server.redis_server import RedisServer


def read_input_from_file(filename):
	redis_server = RedisServer()
	with open(filename, 'r') as input_file:
		for line in input_file:
			command, args = line.split()[0], line.split()[1:]
			result = redis_server(command, *args)
			print result if result else "\n"


if __name__ == '__main__':
	if len(sys.argv) == 2:
		read_input_from_file(sys.argv[1])
	else:
		redis_server = RedisServer()
		line = raw_input()
		while (line):
			command, args = line.split()[0], line.split()[1:]
			result = redis_server(command, *args)
			print result if result else "\n"
			line = raw_input()
