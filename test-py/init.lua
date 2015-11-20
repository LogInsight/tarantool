box.cfg{listen=33013}
box.schema.user.grant('guest', 'read,write,execute', 'universe')
s = box.schema.space.create('tmp', {temporary=true})
s:create_index('primary',{parts = {1,'STR', 2, 'STR'}})


