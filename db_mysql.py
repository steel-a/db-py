import mysql.connector
import re

class DB:

    def __init__(self, connString):
        dic = dict()
        for m in re.findall(r'[ \t]*([a-zA-Z0-9-]*)[ \t]*=[ \t]*\'([a-zA-Z0-9-+*\/=?!@#$%&()_{}<>:;.~^`" ]*)\'[ \t]*',connString):
            dic[m[0]] = m[1]

        self.cnx = mysql.connector.connect(**dic)
        self.exec("SET autocommit = ON")

    def __del__(self):
        self.cnx.close()

    def getValue(self, query:str):
        cursor = self.cnx.cursor()
        cursor.execute(query)
        
        record = cursor.fetchone()
        if(cursor.rowcount<1):
            return None
        elif(cursor.rowcount==1):
            return record[0]
        else:
            raise Exception("More than one result exception with query: ["+query+"]")

    def getRowDic(self, query:str) -> dict:
        cursor = self.cnx.cursor()
        cursor.execute(query)
        
        record = cursor.fetchone()
        columnNames = cursor.column_names
        if(cursor.rowcount<1):
            return None
        elif(cursor.rowcount==1):
            dic = dict()
            for i in range(0,len(columnNames)):
                dic[columnNames[i]] = record[i]
            return dic
        else:
            raise Exception("More than one result exception with query: ["+query+"]")


    def getListRowsDic(self, query:str) -> list:
        cursor = self.cnx.cursor()
        cursor.execute(query)
        
        columnNames = cursor.column_names

        lst = list()
        for record in cursor.fetchall():
            dic = dict()
            for i in range(0,len(columnNames)):
                dic[columnNames[i]] = record[i]
            lst.append(dic)
        return lst


    def getDicRowsList(self, query:str) -> dict:
        cursor = self.cnx.cursor()
        cursor.execute(query)
        
        columnNames = cursor.column_names
        dic = dict()
        for i in range(0,len(columnNames)):
            dic[columnNames[i]] = list()

        for record in cursor.fetchall():
            for i in range(0,len(columnNames)):
                dic[columnNames[i]].append(record[i])

        return dic


    def fetchall(self, query:str) -> list:
        cursor = self.cnx.cursor()
        cursor.execute(query)
        return cursor.fetchall()


    def exec(self, query:str) -> int:
        cursor = self.cnx.cursor()
        cursor.execute(query)
        return cursor.rowcount

    def startTransaction(self):
        self.exec("SET autocommit = OFF")
        self.exec("START TRANSACTION")

    def endTransaction(self):
        self.exec("COMMIT")
        self.exec("SET autocommit = ON")

    def rollback(self):
        try:
            self.exec("ROLLBACK")
            self.exec("SET autocommit = ON")
        except: pass

def f(val):
    if val is None: return 'null'
    if isinstance(val,str):
        if val == 'null': return 'null'
        return "'"+val+"'"
    else:
        return val
