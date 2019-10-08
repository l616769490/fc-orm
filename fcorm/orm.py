import logging
from .constant import AUTO_INCREMENT_KEYS, PRIMARY_KEY
from .sql_utils import fieldStrAndPer, fieldSplit, joinList, pers

_log = logging.getLogger()

class Orm(object):
    def __init__(self, db, tableName, keyProperty = PRIMARY_KEY):
        ''' 操作数据库
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
            return 0
        finally:
            cursor.close()
    
    def insertList(self, keys, dataList):
        ''' 插入一组数据
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
            cursor.execute(sql, dataList)
            lastId = cursor.lastrowid
            self.db.commit()
            return lastId
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return 0
        finally:
            cursor.close()
    
    def insertDictList(self, dataList):
        ''' 插入一组数据
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
            # 如果主键不是自增，则生成主键
            if self.generator != AUTO_INCREMENT_KEYS:   
                for data in dataList:
                    if self.keyProperty not in data or data[self.keyProperty] == 0:    # 没有主键
                        data[self.keyProperty] = self.generator
                    keys, ps, vs = fieldSplit(data)
                    values.append(vs)
            
            sql = 'INSERT INTO `{}`({}) VALUES({})'.format(self.tableName, keys, ps)
            _log.info(sql)
            cursor.execute(sql, values)
            lastId = cursor.lastrowid
            self.db.commit()
            return lastId
        except Exception as e:
            _log.error(e)
            self.db.rollback()
            return 0
        finally:
            cursor.close()

    #################################### 更新操作 ####################################
    def updateByPrimaryKey(self, data, primaryValue = None):
        ''' 根据主键更新数据
        --
            @param data: 要更新的数据，字典格式
            @param primaryValue: 主键值，为None则从data中寻找主键
        '''
        pv = primaryValue

        if not pv:
            pv = data.pop(self.keyProperty, None)
        
        if not pv:
            raise Exception('未传入主键值！')
        
        if not data:
            raise Exception('数据为空！')

        cursor = self.db.cursor()
        try:
            fieldStr, values = fieldStrAndPer(data)
            values.append(pv)
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
            for k, v1 in properties:
                for v2 in v1:
                    arr.append('`{}`.`{}`'.format(k, v2))
            self.properties = joinList(arr, prefix='', suffix='')
        return self
    
    def _select(self, whereStr):
        ''' 生成查询字符串和数据
        --
            @param whereStr: where之后的字符串
        '''
        # SELECT之后 FROM之前的字符串
        perStr = ''
        # FROM之后 WHERE之前的字符串
        fromStr = ''
        # 值列表
        values = []

    def selectByPrimaeyKey(self, primaryValue):
        ''' 根据主键查询
        --
            @param primaryValue: 主键值
        '''
        #cursor = self.db.cursor()

        strDict = {
            'distinctStr':self.distinct,
            'propertiesStr': self.properties,
            'tableName': self.tableName,
            'joinStr': self.joinStr,
            'whereStr':'`{}`=%s'.format(self.keyProperty),
            'orderByStr': self.orderByStr,
            'groupByStr': self.groupByStr,
            'limitStr':''
        }
        
        sql = '''SELECT {distinctStr} {propertiesStr} FROM {tableName} {joinStr} 
                WHERE {whereStr} {orderByStr} {groupByStr} {limitStr}
                '''.format(**strDict)
        return sql, primaryValue
    
    def selectByExample(self, example):
        ''' 根据Example条件进行查询
        '''
    
    def selectCountByExample(self, countProperties, example):
        ''' 根据Example条件统计查询
        '''

    def selectPageByExample(self, example, page, pageNum):
        ''' 根据Example条件分页查询
        '''

    #################################### 删除操作 ####################################
    def deleteByPrimaryKey(self, example):
        ''' 根据主键删除 
        '''
    def deleteByExample(self, example):
        ''' 根据Example条件删除数据
        '''
    
    #################################### 清除关闭 ####################################
    def clear(self):
        ''' 清除数据，只保留数据库连接、表名、主键。清除掉主键策略/查询字段/分组字段/排序字段/多表连接/HAVING字段/去重等
        --
        '''
        # 主键策略
        self.generator = AUTO_INCREMENT_KEYS
        # 多表连接
        self.joinTable = []
        # 查询字段
        self.properties = None
        # 排序字段
        self.orderBy = []
        # 分组字段
        self.groupBy = []
        # HAVING字段
        self.havingStr = None
        # 是否去重
        self.distinct = False
        return self
    
    def close(self):
        ''' 关闭数据库连接
        --
        '''
        self.db.close()
