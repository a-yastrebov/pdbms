from TxtDB import TxtDB
import os


class Pdbms:

    def __init__(self, path):
        self.path = path
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        self.dbs = self.__scan_for_DBs()
        self.active_db = -1

    def __scan_for_DBs(self):
        res = []
        dbs_names = os.listdir(self.path)
        for i in dbs_names:
            res.append(TxtDB(self.path + os.sep + i))
        return res

    def CreateDb(self,dbname):
        for i in self.dbs:
            if i.get_name() == dbname:
                raise FileExistsError("| In Pdbms.CreateDb() | DB with name " + str(dbname) + " already exists")
        self.dbs.append(TxtDB(self.path + os.sep + dbname))

    def UseDb(self,dbname):
        flag = False
        for i in range(len(self.dbs)):
            if self.dbs[i].get_name() == dbname:
                self.active_db = i
                flag = True
                break
        if not flag:
            raise FileNotFoundError("| In Pdbms.UseDb() | There is no DB with name " + str(dbname))

    def CreateTable(self,tablename,fieldsnames):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.CreateTable() | DB is not chosen ")
        db = self.dbs[self.active_db]
        db.create_table(tablename,fieldsnames)

    def Insert(self,tablename,fields):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.CreateTable() | DB is not chosen ")
        db = self.dbs[self.active_db]
        db.insert(tablename,fields)

    def Select(self,tablename,queryoptions = []):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.CreateTable() | DB is not chosen ")
        db = self.dbs[self.active_db]
        return db.select(tablename,queryoptions)

    def Delete(self,tablename,queryoptions = []):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.CreateTable() | DB is not chosen ")
        db = self.dbs[self.active_db]
        db.delete(tablename,queryoptions)

    def Count(self, tablename):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.CreateTable() | DB is not chosen ")
        db = self.dbs[self.active_db]
        return db.count(tablename)

    def GetDbsNames(self):
        res = []
        for i in self.dbs:
            res.append(i.get_name())
        return res

    def GetActiveDbName(self):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.GetActiveDbName() | DB is not chosen ")
        return self.dbs[self.active_db].get_name()

    def GetTablesNames(self):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.GetTablesNames() | DB is not chosen ")
        return self.dbs[self.active_db].get_tables_names()

    def GetTableFields(self,tablename):
        if self.active_db == -1:
            raise RuntimeError("| In Pdbms.GetTableFields() | DB is not chosen ")
        return self.dbs[self.active_db].get_table_fields(tablename)