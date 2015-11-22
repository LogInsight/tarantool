import tarantool
server = tarantool.connect("localhost", 33013)
demo = server.space('tmp')

for i in range(0, 100000):
    demo.insert( (i, 'Delta_%d' % i) )
