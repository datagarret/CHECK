import configparser
import MySQLdb
from sqlalchemy import create_engine
import pandas as pd
from CHECK.secret import secret

class DatabaseConnect():
    def __init__(self, database):
        _sec = secret.Secret()
        self.db_config = _sec.db_config()
        self.db_config['database'] = database
        self.engine = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(**self.db_config))

    def connection_obj(self):
        connection = self.engine.connect()
        return connection

    def query(self,sql_str, df_flag=True, parse_dates=None,
              chunksize=None, columns=None):
        '''
        sql_str(str): query to be fetched from data base
        returns a pandas dataframe that contains query results
        '''
        try:
            connection = self.connection_obj()
            if df_flag == True:
                df = self.to_dataframe(sql_str,connection,parse_dates,chunksize=None)
                return df
            else:
                return connection.execute(sql_str)
        except:
            raise Exception
        finally:
            connection.close()


    def to_dataframe(self,sql_str, connection, parse_dates=None, chunksize=None):
        df = pd.read_sql(sql_str, con=connection, parse_dates=parse_dates, chunksize=None)
        return df

    def insert(self,df,tbl,chunksize=None):
        '''
        df(pd.DataFrame): df to be inserted
        tbl(str): table in database to insert df into
        '''
        try:
            connection = self.connection_obj()
            df.to_sql(name=tbl,con=connection,if_exists='append',index=False,chunksize=None)
            connection.close()
        except:
            raise Exception

    def stored_procedure(self, proc_name, proc_params=None):
        '''Used to call a stored procedure;
        proc_params(list): list of parameters'''

        connection = self.engine.raw_connection()
        try:
            cursor = connection.cursor()
            if proc_params == None:
                cursor.callproc(proc_name)
            else:
                cursor.callproc(proc_name, proc_params)
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()


    def replace(self,df,tbl,chunksize=None):
        '''
        df(pd.DataFrame): df to be inserted
        tbl(str): table in database to insert df into
        '''
        try:
            connection = self.connection_obj()
            self.query("DELETE FROM {}".format(tbl), df_flag=False)
            df.to_sql(name=tbl,con=connection,if_exists='append',index=False,chunksize=None)
            connection.close()
        except:
            raise Exception

    def inline_import(self, sql_str, file_path):
        '''sql_str: (string) contains the inline file instructions
           file_path: (string) path to file to verify counts'''

        try:
            connection = MySQLdb.Connect(host=self.db_config['host'], user=self.db_config['user'],
                                         passwd=self.db_config['password'], db=self.db_config['database'])
            cursor = connection.cursor()
            n_rows = cursor.execute(sql_str)
            cursor.close()
            connection.commit()
            if file_path is not None:
                file_rows = sum([1 for i in open(file_path)])
                return n_rows, file_rows
            return n_rows
        except:
            raise Exception
        finally:
            connection.close()
