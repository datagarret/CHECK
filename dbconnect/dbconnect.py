import datetime
import pandas as pd
import pymysql
from CHECK.secret import secret

class DatabaseConnect:

    def __init__(self, db, default_cursor=pymysql.cursors.DictCursor):
        self.db_config = secret.Secret().db_config()
        self.db_config['database'] = db
        self.db_config['cursorclass'] = default_cursor
        self.db_config['local_infile'] = True


    def create_connection(self):
        connection = pymysql.connect(**self.db_config)
        return connection


    def query(self, sql, *params, output_format='raw', count_output=False):

        if output_format not in ['raw', 'df', 'none']:
            raise ValueError("output_format must be in ['raw', 'df', 'none']")
        if not isinstance(count_output, bool):
            raise ValueError('count_output must be bool')

        connection = self.create_connection()
        try:
            with connection.cursor() as cursor:
                if len(params) == 0:
                    count = cursor.execute(sql)
                else:
                    count = cursor.execute(sql, params)
                result = cursor.fetchall()
                connection.commit()
        finally:
            connection.close()

        if output_format == 'df':
            result = self.to_df(result)

        if count_output == True and output_format != 'none':
            return count, result
        elif count_output == False and output_format != 'none':
            return result
        elif count_output == True and output_format == 'none':
            return count
        else:
            return None

    def insert(self, sql, params):

        connection = self.create_connection()
        try:
            with connection.cursor() as cursor:

                if isinstance(params, pd.DataFrame):
                    #converts df to a list of lists and converts np.nan to None
                    params = params.where(pd.notnull(params), None)
                    params = params.to_numpy().tolist()

                if isinstance(params[0], list) or isinstance(params[0], tuple):
                    count = 0
                    for i in range(0, len(params), 50000):
                        if (i+50000) > len(params):
                            count += cursor.executemany(sql,
                                                        params[i:])
                        else:
                            count += cursor.executemany(sql,
                                                        params[i:i+50000])

                else:
                    cursor.execute(sql, params)
                    count = 1
                connection.commit()
        finally:
            connection.close()

        return count

    def to_df(self, result):

        if len(result) == 0:
            print('No results')
            return pd.DataFrame()

        if not isinstance(result[0], dict):
            raise ValueError('results must be in dict format')

        datetime_cols = []
        for col in result[0]:
            if isinstance(result[0][col], datetime.date):
                datetime_cols.append(col)
            elif isinstance(result[0][col], datetime.datetime):
                datetime_cols.append(col)

        result = pd.DataFrame(result)

        for col in datetime_cols:
            result.loc[:, col] = pd.to_datetime(result.loc[:, col])


        return result
