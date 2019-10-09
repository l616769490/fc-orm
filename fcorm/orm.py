import logging
from .constant import AUTO_INCREMENT_KEYS, PRIMARY_KEY
from .sql_utils import fieldStrAndPer, fieldSplit, joinList, pers

_log = logging.getLogger()

class Orm(object):
    def __init__(self, db, tableName, keyProperty = PRIMARY_KEY):
        ''' 操作数据库
        --
            测试表结构如下：
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
            
            测试代码和测试结果：
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
                # [{'sid': 1, 'name': '张三', 'age': 18}]

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
        --
    
            @param db: 数据库连接
            @param tableName: 表名
            @param keyProperty: 主键字段名。可以不填，不填默认主键名为id
        '''
        # 数据库连接
        self.db = db
        # 表名
        self.tableName = tableName
        # 主键名
        self.keyProperty = keyProperty
        # 主键策略
        self.generator = AUTO_INCREMENT_KEYS
        # 多表连接
        self.joinStr = ''
        # 查询字段
        self.properties = ' * '
        # 排序字段
        self.orderByStr = ''
        # 分组字段
        self.groupByStr = ''
        # HAVING字段
        self.havingStr = ''
        # 是否去重
        self.distinct = ''
    
    def setPrimaryGenerator(self, generator):
        ''' 设置表的主键生成策略，不设置则默认使用数据库自增主键
        --
            @param generator: 主键生成策略，默认自增。可传入一个方法，需要主键时自动调用该方法。
                            该方法不能传入参数，如果需要传参，请在外部调用后存入data
        '''
        if isinstance(generator, function):
            self.generator = generator
        return self

    #################################### 新增操作 ####################################

    def insertOne(self, data):
        ''' 向数据库写入一条数据
        --
            @example
                orm.insertOne({'name':'张三', 'age':18})
                
            @param data: 要插入的数据 字典格式
        '''
        if not data:
            raise Exception('数据为空！')

        cursor = self.db.cursor()
        try:
            # 如果主键不是自增，则生成主键
            if self.generator != AUTO_INCREMENT_KEYS:   
                if self.keyProperty not in data or data[self.keyProperty] == 0:    # 传入的data里面没有主键或者主键值为0
                    data[self.keyProperty] = self.generator()
            
            keys, ps, values = fieldSplit(data)
            sql = 'INSERT INTO `{}`({}) VALUES({})'.format(self.tableName, keys, ps)
            _log.info(sql)
            cursor.execute(sql, values)
            lastId = cursor.lastrowid
            self.db.commit()
            return lastId
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return -1
        finally:
            cursor.close()
    
    def insertList(self, keys, dataList):
        ''' 插入一组数据，注意：返回的是第一条数据的ID
        --
            @example
                orm.insertList(['name', 'age'], [['张三', 18], ['李四', 19]]})

            @param keys: 插入的字段名
            @param dataList: 插入的数据列表，和字段名一一对应
        '''
        if not dataList or not dataList[0]:
            raise Exception('数据为空！')

        cursor = self.db.cursor()
        try:
            # 如果主键不是自增，则生成主键
            if self.generator != AUTO_INCREMENT_KEYS:   
                if self.keyProperty not in keys:    # 传入的keys里面没有主键
                    keys.append(self.keyProperty)
                    for data in dataList:
                        data.append(self.generator())

            sql = 'INSERT INTO `{}`({}) VALUES({})'.format(self.tableName, joinList(keys), pers(len(keys)))
            _log.info(sql)
            cursor.executemany(sql, dataList)
            lastId = cursor.lastrowid
            self.db.commit()
            return lastId
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return -1
        finally:
            cursor.close()
    
    def insertDictList(self, dataList):
        ''' 插入一组数据，注意：返回的是第一条数据的ID
        --
            @example
                orm.insertDictList([{'name':'张三', 'age':18}, {'name':'李四', 'age':19}])

            @param dataList: 插入的数据列表
        '''
        if not dataList or not dataList[0]:
            raise Exception('数据为空！')

        cursor = self.db.cursor()
        try:
            values = []
            keys = ''
            ps = ''

            for data in dataList:
                if self.keyProperty not in data or data[self.keyProperty] == 0:    # 没有主键
                    if self.generator != AUTO_INCREMENT_KEYS:   # 如果主键不是自增，则生成主键
                        data[self.keyProperty] = self.generator
                keys, ps, vs = fieldSplit(data)
                values.append(vs)
            
            sql = 'INSERT INTO `{}`({}) VALUES({})'.format(self.tableName, keys, ps)
            _log.info(sql)
            cursor.executemany(sql, values)
            lastId = cursor.lastrowid
            self.db.commit()
            return lastId
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return -1
        finally:
            cursor.close()

    #################################### 更新操作 ####################################
    def updateByPrimaryKey(self, data, primaryValue = None):
        ''' 根据主键更新数据
        --
            @param data: 要更新的数据，字典格式
            @param primaryValue: 主键值，为None则从data中寻找主键
        '''
        if not primaryValue:
            primaryValue = data.pop(self.keyProperty, None)
        
        if not primaryValue:
            raise Exception('未传入主键值！')
        
        if not data:
            raise Exception('数据为空！')

        cursor = self.db.cursor()
        try:
            fieldStr, values = fieldStrAndPer(data)
            values.append(primaryValue)
            sql = 'UPDATE `{}` SET {} WHERE `{}`=%s'.format(self.tableName, fieldStr, self.keyProperty)
            _log.info(sql)
            cursor.execute(sql, values)
            self.db.commit()
            return True
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()
    
    def updateByExample(self, data, example):
        ''' 根据Example条件更新
        --
        '''
        if not example:
            raise Exception('未传入更新条件！')
        
        if not data:
            raise Exception('数据为空！')

        cursor = self.db.cursor()
        try:
            whereStr, values1 = example.whereBuilder()
            fieldStr, values2 = fieldStrAndPer(data)
            values2.extend(values1)
            sql = 'UPDATE `{}` SET {} WHERE {}'.format(self.tableName, fieldStr, whereStr)
            _log.info(sql)
            cursor.execute(sql, values2)
            self.db.commit()
            return True
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()
        
    #################################### 查询操作 ####################################
    def orderByClause(self, key, clause = 'DESC'):
        ''' ORDER BY key clause
        --
            @param key 排序字段
            @param clause DESC或者ASC
        '''
        if not self.orderByStr:
            self.orderByStr = ' ORDER BY ' + key + ' ' + clause + ' '
        else:
            self.orderByStr = self.orderByStr + ' , ' + key + ' ' + clause + ' '
        return self
    
    def groupByClause(self, key):
        ''' GROUP BY key clause
        --
            @param key 分组字段
        '''
        if not self.groupByStr:
            self.groupByStr = ' GROUP BY ' + key + ' '
        else:
            self.groupByStr = self.groupByStr + ' , ' + key
        return self
    
    def join(self, tName, onStr):
        ''' 多表连接查询，内连接
        --
            @param tName: 表名
            @param onStr: 条件
        '''
        self.joinStr = self.joinStr + ' JOIN ' + tName + ' ON ' + onStr + ' '
        return self

    def leftJoin(self, tName, onStr):
        ''' 多表连接查询，内连接
        --
            @param tName: 表名
            @param onStr: 条件
        '''
        self.joinStr = self.joinStr + ' LEFT JOIN ' + tName + ' ON ' + onStr + ' '
        return self

    def rightJoin(self, tName, onStr):
        ''' 多表连接查询，内连接
        --
            @param tName: 表名
            @param onStr: 条件
        '''
        self.joinStr = self.joinStr + ' RIGHT JOIN ' + tName + ' ON ' + onStr + ' '
        return self
    
    def setDistinct(self):
        ''' 设置去重
        '''
        self.distinct = ' DISTINCT '
        return self

    def setSelectProperties(self, properties):
        ''' 设置查询的列名，不设置默认采用【SELECT * FROM】
        --
            @param properties: 查询的列，list格式和dict格式
                @example:
                    ['name', 'age'] => SELECT `name`, `age` FROM
                    {'user':['name', 'age'], 'order':['orderId']}  => SELECT `user`.`name`, `user`.`age`, `order`:`orderId` FROM
        '''
        if isinstance(properties, list):
            self.properties = joinList(properties)
        elif isinstance(properties, dict):
            arr = []
            for k, v1 in properties.items():
                for v2 in v1:
                    arr.append('`{}`.`{}`'.format(k, v2))
            self.properties = joinList(arr, prefix='', suffix='')
        return self

    def selectAll(self):
        ''' 查询所有
        --
        '''
        cursor = self.db.cursor()

        try:
            strDict = {
                'distinctStr':self.distinct,
                'propertiesStr': self.properties,
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr
            }
            sql = '''SELECT {distinctStr} {propertiesStr} FROM {tableName} {joinStr} {orderByStr} {groupByStr}'''.format(**strDict)
            _log.info(sql)
            cursor.execute(sql)
            res = cursor.fetchall()
            self.db.commit()
            return res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()

    def selectByPrimaeyKey(self, primaryValue):
        ''' 根据主键查询
        --
            @param primaryValue: 主键值
        '''
        cursor = self.db.cursor()

        try:
            strDict = {
                'distinctStr':self.distinct,
                'propertiesStr': self.properties,
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'whereStr':'`{}`=%s'.format(self.keyProperty),
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr
            }
            sql = '''SELECT {distinctStr} {propertiesStr} FROM {tableName} {joinStr} 
                WHERE {whereStr} {orderByStr} {groupByStr}
                '''.format(**strDict)
            _log.info(sql)
            cursor.execute(sql, primaryValue)
            res = cursor.fetchone()
            self.db.commit()
            return res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()
    
    def selectByExample(self, example):
        ''' 根据Example条件进行查询
        --
        '''
        cursor = self.db.cursor()

        try:
            whereStr, values = example.whereBuilder()
            strDict = {
                'distinctStr':self.distinct,
                'propertiesStr': self.properties,
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'whereStr': whereStr,
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr
            }
            sql = '''SELECT {distinctStr} {propertiesStr} FROM {tableName} {joinStr} 
                WHERE {whereStr} {orderByStr} {groupByStr}
                '''.format(**strDict)
            _log.info(sql)
            cursor.execute(sql, values)
            res = cursor.fetchall()
            self.db.commit()
            return res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()
    
    def selectTransactByExample(self, transactProperties, example, transactName = '', transact = 'COUNT'):
        ''' 根据Example条件聚合查询
        --
            @param transactProperties: 统计字段
            @param example: 条件
            @param transactName: 重命名统计字段
            @param transact: 使用哪个函数，默认COUNT。可选SUM，MAX，MIN等
        '''
        cursor = self.db.cursor()

        try:
            whereStr, values = example.whereBuilder()
            strDict = {
                'distinctStr':self.distinct,
                'propertiesStr': self.properties,
                'countStr': '{}({}) {}'.format(transact, transactProperties, transactName),
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'whereStr': whereStr,
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr
            }
            sql = '''SELECT {distinctStr} {propertiesStr} , {countStr} FROM {tableName} {joinStr} 
                WHERE {whereStr} {orderByStr} {groupByStr}
                '''.format(**strDict)
            _log.info(sql)
            cursor.execute(sql, values)
            res = cursor.fetchall()
            self.db.commit()
            return res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()
    
    def selectPageAll(self, page = 1, pageNum = 10):
        ''' 分页查询
        --
        '''
        cursor = self.db.cursor()

        startId = (page - 1) * pageNum

        try:
            strDict = {
                'propertiesStr': self.keyProperty,
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr
            }
            
            sql = '''SELECT COUNT(`{propertiesStr}`) num FROM {tableName} {joinStr} 
                    {orderByStr} {groupByStr}
                    '''.format(**strDict)
            cursor.execute(sql)
            numRes = cursor.fetchone()
            num = numRes['num']

            if num == 0 or num < startId:
                return num, []
            
            strDict = {
                'distinctStr':self.distinct,
                'propertiesStr': self.properties,
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr,
                'limitStr':'LIMIT {}, {}'.format(startId, pageNum)
            }
            sql = '''SELECT {distinctStr} {propertiesStr} FROM {tableName} {joinStr} 
                    {orderByStr} {groupByStr} {limitStr}
                    '''.format(**strDict)
            _log.info(sql)
            cursor.execute(sql)
            res = cursor.fetchall()
            self.db.commit()
            return num, res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()

    def selectPageByExample(self, example, page = 1, pageNum = 10):
        ''' 根据Example条件分页查询
        --
        '''
        cursor = self.db.cursor()

        startId = (page - 1) * pageNum

        try:
            whereStr, values = example.whereBuilder()
            strDict = {
                'propertiesStr': self.keyProperty,
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'whereStr': whereStr,
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr
            }
            
            sql = '''SELECT COUNT(`{propertiesStr}`) num FROM {tableName} {joinStr} 
                    WHERE {whereStr} {orderByStr} {groupByStr}
                    '''.format(**strDict)
            cursor.execute(sql, values)
            numRes = cursor.fetchone()
            num = numRes['num']

            if num == 0 or num < startId:
                return num, []
            
            strDict = {
                'distinctStr':self.distinct,
                'propertiesStr': self.properties,
                'tableName': self.tableName,
                'joinStr': self.joinStr,
                'whereStr': whereStr,
                'orderByStr': self.orderByStr,
                'groupByStr': self.groupByStr,
                'limitStr':'LIMIT {}, {}'.format(startId, pageNum)
            }
            sql = '''SELECT {distinctStr} {propertiesStr} FROM {tableName} {joinStr} 
                    WHERE {whereStr} {orderByStr} {groupByStr} {limitStr}
                    '''.format(**strDict)
            _log.info(sql)
            cursor.execute(sql, values)
            res = cursor.fetchall()
            self.db.commit()
            return num, res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()

    #################################### 删除操作 ####################################
    def deleteByPrimaryKey(self, primaryValue):
        ''' 根据主键删除 
        '''
        
        if not primaryValue:
            raise Exception('未传入主键值！')

        cursor = self.db.cursor()
        try:
            sql = 'DELETE FROM `{}` WHERE `{}`=%s'.format(self.tableName, self.keyProperty)
            _log.info(sql)
            cursor.execute(sql, primaryValue)
            self.db.commit()
            return True
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()
            
    def deleteByExample(self, example):
        ''' 根据Example条件删除数据
        '''
        if not example:
            raise Exception('未传入更新条件！')

        cursor = self.db.cursor()
        try:
            whereStr, values = example.whereBuilder()
            sql = 'DELETE FROM `{}` WHERE {}'.format(self.tableName, whereStr)
            _log.info(sql)
            cursor.execute(sql, values)
            self.db.commit()
            return True
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()

    #################################### 原生SQL操作 ####################################
    def selectOneBySQL(self, sql, values = None):
        ''' 查询单个
        --
        '''
        cursor = self.db.cursor()

        try:
            _log.info(sql)
            if values:
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)
            res = cursor.fetchone()
            self.db.commit()
            return res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()
    
    def selectAllBySQL(self, sql, values = None):
        ''' 查询所有
        --
        '''
        cursor = self.db.cursor()

        try:
            _log.info(sql)
            if values:
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)
            res = cursor.fetchall()
            self.db.commit()
            return res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return False
        finally:
            cursor.close()

    def executeBySQL(self, sql, values = None):
        ''' 根据sql进行更新删除或者新增操作， 不能用于执行查询操作，因为不会返回查询结果，查询使用selectAllBySQL或者selectOneBySQL
        --
            @param sql: sql语句
            @param values: 参数
            @rerturn: 失败返回-1
        '''
        cursor = self.db.cursor()
        try:
            _log.info(sql)
            if values:
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)

            res = cursor.lastrowid
            
            self.db.commit()
            return res
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return -1
        finally:
            cursor.close()
    
    #################################### 清除关闭 ####################################
    def clear(self):
        ''' 清除数据，只保留数据库连接、表名、主键。清除掉主键策略/查询字段/分组字段/排序字段/多表连接/HAVING字段/去重等
        --
        '''
        # 多表连接
        self.joinStr = ''
        # 查询字段
        self.properties = ' * '
        # 排序字段
        self.orderByStr = ''
        # 分组字段
        self.groupByStr = ''
        # HAVING字段
        self.havingStr = ''
        # 是否去重
        self.distinct = ''
        return self
    
    def close(self):
        ''' 关闭数据库连接
        --
        '''
        self.db.close()