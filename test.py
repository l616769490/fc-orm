import pymysql
from fcorm import Orm2, Example, Transaction, joinList
# db = pymysql.connect('localhost', 'root', '123456', 'test', charset="utf8", cursorclass=pymysql.cursors.DictCursor)

# orm1 = Orm2('study', 'sid').selectByPrimaeyKey(1).selectPageAll(1, 10)

# orm2 = Orm2('student', 'sid').selectAll()
# transaction = Transaction(db).add(orm1).add(orm2)
# print(transaction.commit())

def setSelectProperties(ps):
    properties = ''
    if isinstance(ps, list):
        for i, v in enumerate(ps):
            if isinstance(v, tuple):
                vTuple1 = v[0]
                vTuple2 = v[1]
                if '.' in vTuple1:
                    vTuple1s = vTuple1.split('.')
                    vTuple1 = '`' + vTuple1s[0] + '`.`' + vTuple1s[1] + '`'
                ps[i] = ' {} {} '.format(vTuple1, vTuple2)
            elif '.' in v:
                vs = v.split('.')
                ps[i] = '`' + vs[0] + '`.`' + vs[1] + '`'
            else:
                ps[i] = '`' + v + '`'
        properties = joinList(ps, prefix='', suffix='')
    elif isinstance(ps, dict):
        arr = []
        for k, v1 in ps.items():
            for v2 in v1:
                if isinstance(v2, tuple):
                    arr.append('`{}`.`{}` `{}`'.format(k, v2[0], v2[1]))
                else:
                    arr.append('`{}`.`{}`'.format(k, v2))
        properties = joinList(arr, prefix='', suffix='')
    return properties

if __name__ == "__main__":
    ssss = setSelectProperties(['a.name', 'age', ('b.name', 'b_name')])
    print(ssss)