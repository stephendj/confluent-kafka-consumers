class SqlTemplates:
    @staticmethod
    def get(filename):
        sql_f = open('./sql/%s' % filename)
        return sql_f.read()