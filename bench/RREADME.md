
### bench.lua
    * 需要安装cjson
    * 数据源: 微博数据
    * 测试方案: 将微博数据读入内存,然后逐条插入
    * 测试数据量: 10w, 30w
    * 测试结果: 写: 1w/s 读: 5W
    
### bench_ws.lua
    * 先编译third_party/wumpus, 将libws.a拷贝到 tarantool/lib 下
    * 编译tarantool, 将bench_ws.lua 拷贝到执行目录
    * 将third_party/wumpus/wumpus.cfg拷贝到执行目录,并更名为ws.cfg
    * 拷贝测试数据到执行目录
    * ./tarantool bench_ws.lua