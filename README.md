## 一、安装
```shell
pip install fcorm
```
## 二、测试代码
### 1.新建测试数据库
#### 1.1 创建数据库test
#### 1.2 新建几张测试表
```sql
CREATE TABLE `course` (
    `cid` int(11) NOT NULL AUTO_INCREMENT COMMENT '课程号，自增',
    `name` varchar(20) NOT NULL COMMENT '课程名',
    `tid` int(11) NOT NULL COMMENT '授课教师',
    PRIMARY KEY (`cid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `student` (
    `sid` int(11) NOT NULL AUTO_INCREMENT COMMENT '学号，自增',
    `name` varchar(20) NOT NULL COMMENT '学生名',
    `age` int(11) NOT NULL COMMENT '年龄',
    PRIMARY KEY (`sid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `study` (
    `sid` int(11) NOT NULL COMMENT '学号',
    `cid` int(11) NOT NULL COMMENT '课程号',
    `result` int(11) DEFAULT NULL COMMENT '成绩',
    PRIMARY KEY (`sid`,`cid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `teacher` (
    `tid` int(11) NOT NULL AUTO_INCREMENT COMMENT '教师号，自增',
    `name` varchar(20) NOT NULL COMMENT '教师名',
    `level` varchar(20) NOT NULL COMMENT '等级',
    PRIMARY KEY (`tid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```
#### 1.3 插入几条测试数据
```sql
INSERT INTO `course` VALUES (1, '计算机', 1);
INSERT INTO `student` VALUES (1, '张三', 18);
INSERT INTO `student` VALUES (2, '李四', 19);
INSERT INTO `study` VALUES (1, 1, 90);
INSERT INTO `study` VALUES (2, 1, 80);
INSERT INTO `teacher` VALUES (1, '老王', '教授');
```
### 2. 编写测试代码
```python3
from fcorm import Orm, Example
import pymysql


db = pymysql.connect(host='localhost',
                    user='root',
                    password='123456',
                    db='test',
                    charset='utf8',
                    cursorclass=pymysql.cursors.DictCursor)
                
stuOrm = Orm(db, 'student', 'sid')
# 查询所有学生
print(stuOrm.selectAll())
# [{'sid': 1, 'name': '张三', 'age': 18}, {'sid': 2, 'name': '李四', 'age': 19}]

# 查询所有学生的学号和名字
stuOrm.setSelectProperties(['sid', 'name'])
print(stuOrm.selectAll())
# [{'sid': 1, 'name': '张三'}, {'sid': 2, 'name': '李四'}]

# 查询学号为1的学生信息
stuOrm.clear()
print(stuOrm.selectByPrimaeyKey(1))
# {'sid': 1, 'name': '张三', 'age': 18}

# 查询所有学生，按年龄从大到小排序
stuOrm.orderByClause('age')
print(stuOrm.selectAll())
# [{'sid': 2, 'name': '李四', 'age': 19}, {'sid': 1, 'name': '张三', 'age': 18}]

# 查询所有学生的（学号，姓名，课程名，成绩，任课教师）
p = {'student':['sid','name'], 'course':['name'], 'study':['result'], 'teacher':['name']}
stuOrm.setSelectProperties(p).leftJoin('study', 'study.sid=student.sid').leftJoin('course', 'course.cid=study.cid').leftJoin('teacher', 'teacher.tid=course.tid')
print(stuOrm.selectAll())
# [{'sid': 2, 'name': '李四', 'course.name': '计算机', 'result': 80, 'teacher.name': '老王'}, 
#  {'sid': 1, 'name': '张三', 'course.name': '计算机', 'result': 90, 'teacher.name': '老王'}]

# 查询所有姓张的学生
stuOrm.clear()
example = Example().andLike('name', '张%')
print(stuOrm.selectByExample(example))
# [{'sid': 1, 'name': '张三', 'age': 18}]

# 分页查询
print(stuOrm.selectPageByExample(example))
# (1, [{'sid': 1, 'name': '张三', 'age': 18}])

# 分页查询2
print(stuOrm.selectPageAll())
# (2, [{'sid': 1, 'name': '张三', 'age': 18}, {'sid': 2, 'name': '李四', 'age': 19}])

# 插入数据
print(stuOrm.insertOne({'name':'王五', 'age':20}))
# 3

# 批量插入
print(stuOrm.insertList(['name', 'age'], [['老六', 21], ['老七', 22]]))
# 4

# 批量插入2
print(stuOrm.insertDictList([{'name':'老八', 'age':23}, {'name':'老九', 'age':24}]))
# 6

# 更新
print(stuOrm.updateByPrimaryKey({'name':'张三2', 'age':10, 'sid':1}))
# True

# 条件更新
print(stuOrm.updateByExample({'name':'张三3', 'age':10}, example))
# True

# 删除
print(stuOrm.deleteByPrimaryKey(1))
# True

# 条件删除
example.orEqualTo({'name':'老八'})
print(stuOrm.deleteByExample(example))
# True
```
## 三、源码
```html
https://github.com/l616769490/fc-orm
```