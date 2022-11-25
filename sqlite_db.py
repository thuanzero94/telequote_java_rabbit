import json
import sqlite3
import itertools

class SqlDB:
    LATEST_TABLE_NAME = 'latest_data'
    LATEST_TABLE_COLUMN = ('symbol', 'bid', 'ask', 'last', 'change', 'high', 'low', 'open', 'prev_close', 'timestamp')

    def __init__(self, db_name='latestDB.db'):
        # print('init MySQL Relay')
        self._database = db_name
        self._connection = None
        self.connect()
        self.create_latest_table()

    def create_latest_table(self):
        try:
            cur = self._connection.cursor()
            cur.execute(f"CREATE TABLE {self.LATEST_TABLE_NAME}({' VARCHAR(255), '.join(self.LATEST_TABLE_COLUMN) + ' VARCHAR(255)'})")
            cur.close()
        except Exception as e:
            print(str(e))
            pass

    def connect(self):
        ret = True
        try:
            self._connection = sqlite3.connect(self._database)
            print("DB Connect Success!")
        except Exception as e:
            ret = False
            msg = "[db_connect] Error - " + str(e)
            print(msg)
        return ret

    def disconnect(self):
        self._connection.close()

    def check_symbol_existed(self, symbol):
        query = f"SELECT * FROM {self.LATEST_TABLE_NAME} WHERE symbol='{symbol}'"
        cursor = self._connection.cursor()
        cursor.execute(query)
        if len(cursor.fetchall()):
            cursor.close()
            return True
        cursor.close()
        return False

    def update_latest_data(self, sql_data: dict):
        """

        :param query_type: update / insert
        :param sql_data: {'last': 56970.49, 'high': 59177.97, 'low': 56510.89, 'open': 56727.58, 'prev_close': 56727.58, 'symbol': 'BTC-USDT', 'change': 242.91, 'timestamp': '1638350960'}
        :return:
        """
        query = ''
        val = []

        query_type = 'update'
        if not self.check_symbol_existed(sql_data['symbol']):
            query_type = 'insert'

        if query_type == 'insert':
            query = f"INSERT INTO {self.LATEST_TABLE_NAME}({', '.join(self.LATEST_TABLE_COLUMN)}) " \
                       "VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"
            val = (sql_data['symbol'], sql_data['bid'], sql_data['ask'], sql_data['last'], sql_data['change'], sql_data['high'], sql_data['low'], sql_data['open'], sql_data['prev_close'], sql_data['timestamp'])
        elif query_type == 'update':
            query = f"UPDATE {self.LATEST_TABLE_NAME} SET bid='%s', last='%s', ask='%s', change='%s', `high`='%s', low='%s', `open`='%s', prev_close='%s', timestamp='%s' " \
                    "WHERE symbol='%s'"
            val = (sql_data['bid'], sql_data['last'], sql_data['ask'], sql_data['change'], sql_data['high'], sql_data['low'], sql_data['open'], sql_data['prev_close'], sql_data['timestamp'], sql_data['symbol'])
        try:
            print(f'[SQL][{self.LATEST_TABLE_NAME} {query_type}] ' + query % val)
            str_query = query % val
            cursor = self._connection.cursor()
            cursor.execute(str_query)
            self._connection.commit()
            cursor.close()
        except Exception as e:
            print(f'[{self.LATEST_TABLE_NAME} {query_type} Fail] {e}')
            self._connection.rollback()

    def get_all_to_dict(self):
        query = f"SELECT * FROM {self.LATEST_TABLE_NAME}"
        cursor = self._connection.cursor()
        cursor.execute(query)
        # result = cursor.fetchall()
        data = [dict(zip(self.LATEST_TABLE_COLUMN, row))
                for row in cursor.fetchall()]

        self._connection.commit()
        cursor.close()
        # print(data)
        return data


def demo_data():
    list_symbol = ['EUR A0-FX', 'GBP A0-FX', 'XAU A0-FX', 'SN1Z2', 'SN1C1', 'AUD A0-FX', 'CHF A0-FX', 'JPY A0-FX', 'GHSIZ2', 'GHSIH3', 'GHSIX2', 'GHSIC1']
    test_data = []
    for s in list_symbol:
        test_data.append({'symbol': s, 'bid': '4324', 'ask': '445', 'last': '56970.49', 'change': '242.91', 'high': '59177.97', 'low': '56510.89', 'open': '56727.58', 'prev_close': '56727.58', 'timestamp': '1638350960'})
    print(test_data)
    for d in test_data:
        sql_db.update_latest_data(d)

if __name__ == '__main__':
    # LATEST_TABLE_COLUMN = ('symbol', 'bid', 'ask', 'last', 'change', 'high', 'low', 'open', 'prev_close', 'timestamp')
    # print(' VARCHAR(255), '.join(LATEST_TABLE_COLUMN) + " VARCHAR(255)")
    sql_db = SqlDB()
    d = sql_db.get_all_to_dict()
    a = json.dumps(d, ensure_ascii=False)
    # demo_data()
    sql_db.disconnect()



