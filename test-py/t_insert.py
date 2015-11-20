import tarantool
server = tarantool.connect("localhost", 33013)
demo = server.space('tmp')


demo.insert(('DDDD', 'Delta'))
