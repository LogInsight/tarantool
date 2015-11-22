## 此处为一些尚无法决定的设计要素

1 字段的类型, 在 sophia 和 wt 存储引擎中, 均不支持 float | double 等, 而这些类型是一种很好的语法糖
    - 在传输协议层, 支持 float | double
  可以在 space 创建时，定义字段。参考
  http://tarantool.org/doc/book/box/box_space.html#lua-object.box.space._space

2 针对全文索引
  - 应该存在一个接口读取全部的未登录词
  - 应该存在一个索引, 读取 term, term 的倒排
  - 可以修改 term ...
  
  <docID, offset, text>, <unknow_term, count>, <term_id, sno, postings>, <term_id, term_type>
  term_type, 存在一些特殊的 term