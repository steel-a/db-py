from dbpy.tests.config import getConnStr
from dbpy.db_mysql import DB

def test_db_mysql():
    with DB(getConnStr()) as db: # Using transaction

        db.exec('drop table if exists test')
        db.exec('create table test (a int, b varchar(10))')
        db.exec('insert into test values (1,"aaa")')
        db.exec('insert into test values (2,"bbb")')

        assert db.getValue('select b from test where a = 1') == 'aaa'
        assert db.getValue('select a from test where a = 1') == 1

        dic = db.getRow('select * from test where a=1')
        assert dic['a']==1
        assert dic['b']=='aaa'

        lst = db.getListRows('select * from test')
        assert lst[0]['a']==1
        assert lst[0]['b']=='aaa'
        assert lst[1]['a']==2
        assert lst[1]['b']=='bbb'

        dic = db.getRowLists('select * from test')
        assert dic['a'][0]==1
        assert dic['a'][1]==2
        assert dic['b'][0]=='aaa'
        assert dic['b'][1]=='bbb'

        lst = db.fetchall('select * from test')
        assert lst[0][0]==1
        assert lst[0][1]=='aaa'
        assert lst[1][0]==2
        assert lst[1][1]=='bbb'


if __name__ == '__main__':
    test_db_mysql()