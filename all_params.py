import psycopg2 as p2


class GetDataSql:
    def __init__(self, login, password):
        self.login = login
        self.password = password

    def get_log_pas(self, type_connect=None):
        if type_connect == 'prod':
            __db_prod = {
                "db_name": "infectdata",
                "user": self.login,
                "passw": self.password,
                "host": "10.3.81.158:5432"
            }
            return __db_prod
        else:
            __db_test = {
                "db_name": "infectdata",
                "user": self.login,
                "passw": self.password,
                "host": "10.2.174.4:5432"
            }
            return __db_test

    def get_data_prod(self, sql_request):
        try:

            params = self.get_log_pas('prod')
            req = f"postgresql://{params['user']}:{params['passw']}@{params['host']}/{params['db_name']}"
            # Подключение к БД
            connect = p2.connect(req)
            # Выполнение запрос к БД и запись результата в переменную
            with connect.cursor() as curs:
                curs.execute(sql_request)
                return curs.fetchall()

        except Exception as errr:
            print(errr, "Ошибка соединения")

    def get_data_test(self, sql_request, action='select', update_row=None):
        try:

            params = self.get_log_pas()
            req = f"postgresql://{params['user']}:{params['passw']}@{params['host']}/{params['db_name']}"
            # Подключение к БД
            connection = p2.connect(req)
            if action == 'select':
                # Выполнение запрос к БД и запись результата в переменную
                with connection.cursor() as curs:
                    curs.execute(sql_request)
                    return curs.fetchall()
            if action == 'update':
                update_row = 0
                with connection.cursor() as curs:
                    curs.execute(sql_request, update_row)
                    update_row = curs.rowcount
                    connection.commit()
                    print(f'Update {update_row} row(s).')

        except Exception as errr:
            print(errr, "Ошибка соединения")


def check_type(value):
    if value in ['False', 'True']:
        return value.lower()
    elif value.isdigit():
         return value
    else:
        return f'__{value}__'
