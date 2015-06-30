from redis_server import RedisServer

if __name__ == '__main__':
	redis_server = RedisServer()
	line = raw_input()
	while (line):
		print line
		command, args = line.split()[0], line.split()[1:]
		redis_server(command, *args)
		line = raw_input()
