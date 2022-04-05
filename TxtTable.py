import os

LINE_DELETED = 0
LINE_EXISTS = 1

class TxtTable:

    def __init__(self, path):
        self.name = os.path.basename(path)
        self.path = path
        self.fields = self.__read_fields()

    def __read_fields(self):
        res = []
        if os.path.exists(self.path):
            file = None
            try:
                file = open(self.path, "r")
                res = file.readline().split()
            finally:
                if file is not None:
                    file.close()
        return res

    def __validate_values(self, values):
        res = True
        for i in values.keys():
            if i not in self.fields:
                res = False
                break
        return res

    def __validate_conditions(self, conditions):
        res = True
        for i in conditions:
            if i[1] not in self.fields:
                res = False
                break
        return res

    def set_up_table(self, fields):
        file = None
        try:
            file = open(self.path, "w")
            for i in fields:
                file.write(str(i))
                file.write(" ")
            file.write("\n")
        finally:
            if file is not None:
                file.close()
        self.fields = self.__read_fields()

    def insert(self, values):
        if not type(values) == dict:
            TypeError("| In TxtTable.insert() | Inserted tuple should be dictionary, but there is " + str(type(values)))
        if not self.__validate_values(values):
            raise ValueError("| In TxtTable.insert() | Unknown fields names in inserted tuple")
        ins = []
        for i in self.fields:
            ins.append(values.get(i))
        file = None
        try:
            file = open(self.path, "a")
            file.write(str(LINE_EXISTS))
            for i in ins:
                file.write(str(i))
                file.write(" ")
            file.write("\n")
        finally:
            if file is not None:
                file.close()

    def __convert_to_dict(self, array):
        res = {}
        for i in range(len(self.fields)):
            res[self.fields[i]] = array[i]
        return res

    @staticmethod
    def __check_conditions(values, conditions):
        res = True
        for i in conditions:
            attribute = values.get(i[1])
            temp = [str(x) for x in i[2]]
            if attribute not in temp and i[0]:
                res = False
                break
            elif attribute in temp and not i[0]:
                res = False
                break
        return res

    def select(self, conditions):
        if not self.__validate_conditions(conditions):
            raise ValueError("| In TxtTable.select() | Unknown fields names in conditions")
        res = []
        file = None
        try:
            file = open(self.path, "r")
            file.readline()
            temp = file.readline()
            temp, mark = self.remove_delete_mark(temp)
            while temp:
                temp = temp.split()
                temp = self.__convert_to_dict(temp)
                if TxtTable.__check_conditions(temp, conditions) and mark == LINE_EXISTS:
                    res.append(temp)
                temp = file.readline()
                temp, mark = self.remove_delete_mark(temp)
        finally:
            if file is not None:
                file.close()
        return res

    def count(self):
        res = []
        file = None
        try:
            file = open(self.path, "r")
            res = len(file.readlines())
            if res > 0:
                res -= 1
        finally:
            if file is not None:
                file.close()
        return res

    def delete(self, conditions):
        if not self.__validate_conditions(conditions):
            raise ValueError("| In TxtTable.select() | Unknown fields names in conditions")
        res = []
        file = None
        try:
            file = open(self.path, "r")
            header = file.readline()
            temp = file.readline()
            temp, mark = self.remove_delete_mark(temp)
            line = temp
            while temp:
                temp = temp.split()
                temp = self.__convert_to_dict(temp)
                if TxtTable.__check_conditions(temp, conditions):
                    res.append((LINE_DELETED, line))
                else:
                    res.append((LINE_EXISTS, line))
                temp = file.readline()
                temp, mark = self.remove_delete_mark(temp)
        finally:
            if file is not None:
                file.close()

        try:
            file = open(self.path, "w")
            file.write(header)
            for m, line in res:
                file.write(self.insert_delete_mark(line, m))
                #file.write("\n")
        finally:
            if file is not None:
                file.close()

    def get_name(self):
        return self.name

    def get_fields(self):
        return self.fields

    def remove_delete_mark(self, str):
        mark = LINE_EXISTS
        outstr = str
        if len(str) > 0:
            mark = str[0]
            outstr = str[1:]
        return outstr, int(mark)

    def insert_delete_mark(self, str, mark):
        return "{}{}".format(mark, str)
