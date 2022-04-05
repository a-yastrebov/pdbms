import os
from TxtTable import TxtTable

class TxtDB:

    def __init__(self,path):
        self.path = path
        self.name = os.path.basename(self.path)
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        self.tables = self.__scan_for_tables()

    def __scan_for_tables(self):
        res = []
        tables_names = os.listdir(self.path)
        for i in tables_names:
            res.append(TxtTable(self.path + os.sep + i))
        return res

    def create_table(self,table_name,fields_names):
        for i in self.tables:
            if i.get_name() == table_name:
                raise FileExistsError("|In TxtDB.create_table()|Table with name " + str(table_name) + " already exists")
        table = TxtTable(self.path + os.sep + table_name)
        self.tables.append(table)
        table.set_up_table(fields_names)


    def insert(self,table_name,values):
        flag = False
        for i in self.tables:
            if i.get_name() == table_name:
                i.insert(values)
                flag = True
                break
        if not flag:
            raise FileNotFoundError("| In TxtDB.insert() | There is no table with name " + str(table_name))

    def select(self,table_name,conditions):
        res = None
        for i in self.tables:
            if i.get_name() == table_name:
                res = i.select(conditions)
                break
        if res is None:
            raise FileNotFoundError("| In TxtDB.select() | There is no table with name " + str(table_name))
        return res

    def delete(self,table_name,conditions):
        res = False
        for i in self.tables:
            if i.get_name() == table_name:
                res = True
                i.delete(conditions)
                break
        if not res:
            raise FileNotFoundError("| In TxtDB.select() | There is no table with name " + str(table_name))

    def count(self, table_name):
        res = None
        for i in self.tables:
            if i.get_name() == table_name:
                res = i.count()
                break
        if res is None:
            raise FileNotFoundError("| In TxtDB.select() | There is no table with name " + str(table_name))
        return res

    def get_name(self):
        return self.name

    def get_tables_names(self):
        res = []
        for i in self.tables:
            res.append(i.get_name())
        return res

    def get_table_fields(self,table_name):
        if table_name not in self.get_tables_names():
            raise FileNotFoundError("| In TxtDB.get_table_fields() | There is no table with name " + str(table_name))
        res = []
        for i in self.tables:
            if i.get_name() == table_name:
                res = i.get_fields()
                break
        return res