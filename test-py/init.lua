console = require('console')
box.cfg{listen=33013 }
t = box.schema.space.create('tmp', {temporary=true, if_not_exists=true})
t:create_index('primary', {if_not_exists=true, parts = {1,'NUM', 2, 'STR'}})

s = box.schema.space.create('wt', {temporary=false, if_not_exists=true, engine='wiredtiger', format={[1]={["type"]="num"},
    [2]={["type"]="str"}, [3]={["type"]="str"}} })
s:create_index('primary',{if_not_exists=true, parts = {1,'NUM'}})
-- this lead to a crash.
-- s:create_index('fact_1', {if_not_exists=true, parts = {2, 'STR'}})
box.schema.user.grant('guest', 'read,write,execute', 'universe')

s:insert{1, 'Hello', 'abc' }
s:insert{2, 'world', 'ni'}

-- s:drop()
-- console.start()

