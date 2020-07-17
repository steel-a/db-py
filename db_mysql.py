import mysql.connector
import re

class DB:

    def __init__(self, connString):
        dic = dict()
        for m in re.findall(r'[ \t]*([a-zA-Z0-9-]*)[ \t]*=[ \t]*\'([a-zA-Z0-9-+*\/=?!@#$%&()_{}\[\]<>:;.~^`" ]*)\'[ \t]*',connString):
            dic[m[0]] = m[1]

        self.conn = mysql.connector.connect(**dic)
        self.exec("SET autocommit = ON")


    def __del__(self):
        self.conn.close()


    def __enter__(self):
        self.startTransaction()
        return self


    def __exit__(self, type, value, tb):
        if tb is None:
            self.endTransaction()
            self.conn.close()
        else:
            self.rollback()
            self.conn.close()
            return False


    def getValue(self, query:str):
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        record = cursor.fetchone()
        if(cursor.rowcount<1):
            return None
        elif(cursor.rowcount==1):
            return record[0]
        else:
            raise Exception("More than one result exception with query: ["+query+"]")


    def getRow(self, query:str) -> dict:
        cursor = self.conn.cursor()
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


    def getListRows(self, query:str) -> list:
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        columnNames = cursor.column_names

        lst = list()
        for record in cursor.fetchall():
            dic = dict()
            for i in range(0,len(columnNames)):
                dic[columnNames[i]] = record[i]
            lst.append(dic)
        return lst


    def getRowLists(self, query:str) -> dict:
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        columnNames = cursor.column_names
        dic = dict()
        for i in range(0,len(columnNames)):
            dic[columnNames[i]] = list()

        for record in cursor.fetchall():
            for i in range(0,len(columnNames)):
                dic[columnNames[i]].append(record[i])

        return dic

    def getValuesSeparatedBy(self, query:str, separator:str) -> dict:
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        string = None
        for record in cursor.fetchall():
            if string == None: string = record[0]
            else: string = string+separator+record[0]

        return string


    def fetchall(self, query:str) -> list:
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()


    def exec(self, query:str) -> int:
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.rowcount


    def tryToExec(self, command:str):
        '''
        Try to execute a sql command.
        Return True if success and False otherwise
        '''
        try:
            self.conn.cursor().execute(command)
            return True
        except:
            return False


    def startTransaction(self):
        self.reconnect()
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


    def reconnect(self):
        if not self.conn.is_connected():
            self.conn.reconnect()


def f(val):
    if val is None: return 'null'
    if isinstance(val,str):
        if val == 'null': return 'null'
        return "'"+val+"'"
    else:
        return val

def createFKifNE(db:DB, table:str, column:str, destTable:str, destColumn:str,
                onUpdateAction:str='CASCADE', onDeleteAction:str='RESTRICT'):
    fkName = 'fk_'+table+'_'+column
    return db.exec(f"""
        IF NOT EXISTS (
            SELECT NULL 
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE
                CONSTRAINT_SCHEMA = DATABASE() AND
                CONSTRAINT_NAME   = '{fkName}' AND
                CONSTRAINT_TYPE   = 'FOREIGN KEY'
        )
        THEN
            ALTER TABLE `{table}` 
            ADD CONSTRAINT `{fkName}`
            FOREIGN KEY (`{column}`)
            REFERENCES `{destTable}` (`{destColumn}`)
            ON DELETE {onDeleteAction}
            ON UPDATE {onUpdateAction};
        END IF
        """)
