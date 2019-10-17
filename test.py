# import pymysql
# from fcorm import Orm, Example
# db = pymysql.connect('localhost', 'root', '123456', 'test', charset="utf8", cursorclass=pymysql.cursors.DictCursor)

# orm = Orm(db, 'student')
# example = Example().andBetween('age', 19, 22)
# res = orm.selectByExample(example)
# print(res)

s = ' '
if s == ' ':
    print('空格')
elif s >= 'a' and s <='z' or s >= 'A' and s <= 'Z':
    print('字母')
elif s >= '0' and s <= '9':
    print('数字')
else:
    print('符号') 