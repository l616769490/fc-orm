import pymysql
from fcorm import Orm2, Example, Transaction
db = pymysql.connect('localhost', 'root', '123456', 'test', charset="utf8", cursorclass=pymysql.cursors.DictCursor)

orm1 = Orm2('study', 'sid').selectByPrimaeyKey(1).selectPageAll(1, 10)

orm2 = Orm2('student', 'sid').selectAll()
transaction = Transaction(db).add(orm1).add(orm2)
print(transaction.commit())