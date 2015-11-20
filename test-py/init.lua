box.cfg{listen=33013}
s = box.schema.space.create('tmp', {temporary=false, engine='wiredtiger'})
s:create_index('primary',{parts = {1,'STR', 2, 'STR'}})
-- box.schema.user.grant('guest', 'read,write,execute', 'universe')

s:insert{'Hello', 'world'}

s:drop()

