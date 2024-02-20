from typing import List
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text
from urllib.parse import quote


class QueryDatabaseKo:
    
    schema = 'query_ko'
    db = {
        "user": 'yjlee',
        "password": quote("Ascent123!@#"),
        "host": "analysis002.dev.ascentlab.io",
        "port": 10086,
    "database": schema,
    }
    
    DB_URL = f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?charset=utf8"
    engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20, pool_recycle=3600)    
    metadata = sqlalchemy.MetaData(bind=engine)
    print('create engine')
    
    @staticmethod
    def get_connection():
        for i in range(10):
            try:
                with QueryDatabaseKo.engine.connect() as connection:
                    pass
                return QueryDatabaseKo.engine, QueryDatabaseKo.metadata
            except Exception as e:                
                #logger.warning(f'{str(e)} MYSQL 연결 재시도 {i+1}...')
                DB_URL = f"mysql+mysqlconnector://{QueryDatabaseKo.db['user']}:{QueryDatabaseKo.db['password']}@{QueryDatabaseKo.db['host']}:{QueryDatabaseKo.db['port']}/{QueryDatabaseKo.db['database']}?charset=utf8"
                QueryDatabaseKo.engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20,pool_recycle=3600)    
                QueryDatabaseKo.metadata = sqlalchemy.MetaData(bind=QueryDatabaseKo.engine)
        raise Exception('MYSQL 연결 실패')

    @staticmethod
    def get_suggest_target_keywords() -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f"SELECT s FROM {QueryDatabaseKo.schema}.oi_suggest_labels;"
        suggest_target_keywords = pd.read_sql(query, con=engine)
        suggest_target_keywords = list(suggest_target_keywords['s'])
        suggest_target_keywords = [kw.replace("_", " ") for kw in suggest_target_keywords]
        engine.dispose()
        return suggest_target_keywords

    @staticmethod
    def get_target_keyword_by_user() -> pd.DataFrame:
        engine, metadata = QueryDatabaseKo.get_connection()
        query = f'''SELECT s as target_keyword, 
                    SUBSTRING(json_extract(properties, "$.user"), 2, LENGTH(json_extract(properties, "$.user")) - 2) as user_id,
                    SUBSTRING(json_extract(properties, "$.source[0]"), 2, LENGTH(json_extract(properties, "$.source[0]")) - 2) as source 
                    FROM {QueryDatabaseKo.schema}.oi_suggest_labels;
                '''
        target_keywords_by_user = pd.read_sql(query, con=engine)
        engine.dispose()
        return target_keywords_by_user
    
    @staticmethod
    def upsert_google_suggest_trend(args : list):
        '''
        args = [[job_id, category, info]]
        '''
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "google_suggest_trend"
        def _query_col_list():
            query = """
                `job_id`,
                `category`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `job_id`=VALUES(`job_id`),
                `category`=VALUES(`category`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  4 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_google_suggest_trend_target(args : list):
        engine, metadata = QueryDatabaseKo.get_connection()
        table_name = "google_suggest_trend_target"
        def _query_col_list():
            query = """
                `user_id`,
                `job_id`,
                `target_keyword`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `user_id`=VALUES(`user_id`),
                `job_id`=VALUES(`job_id`),
                `target_keyword`=VALUES(`target_keyword`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  5 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
    
class QueryDatabaseJa:
        
    schema = 'query_jp_ja'
    db = {
        "user": 'db',
        "password": quote("Ascent123!@#"),
        "host": "analysis004.dev.ascentlab.io",
        "port": 10086,
    "database": schema,
    }
    
    DB_URL = f"mysql+mysqlconnector://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}?charset=utf8"
    engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20, pool_recycle=3600)    
    metadata = sqlalchemy.MetaData(bind=engine)
    print('create engine')
    
    @staticmethod
    def get_connection():
        for i in range(10):
            try:
                with QueryDatabaseJa.engine.connect() as connection:
                    pass
                return QueryDatabaseJa.engine, QueryDatabaseJa.metadata
            except Exception as e:                
                #logger.warning(f'{str(e)} MYSQL 연결 재시도 {i+1}...')
                DB_URL = f"mysql+mysqlconnector://{QueryDatabaseJa.db['user']}:{QueryDatabaseJa.db['password']}@{QueryDatabaseJa.db['host']}:{QueryDatabaseJa.db['port']}/{QueryDatabaseJa.db['database']}?charset=utf8"
                QueryDatabaseJa.engine = sqlalchemy.create_engine(DB_URL, encoding="utf-8", pool_size=20,pool_recycle=3600)    
                QueryDatabaseJa.metadata = sqlalchemy.MetaData(bind=QueryDatabaseJa.engine)
        raise Exception('MYSQL 연결 실패')

    @staticmethod
    def get_suggest_target_keywords() -> pd.DataFrame:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f"SELECT s FROM {QueryDatabaseJa.schema}.oi_suggest_labels;"
        suggest_target_keywords = pd.read_sql(query, con=engine)
        suggest_target_keywords = list(suggest_target_keywords['s'])
        suggest_target_keywords = [kw.replace("_", " ") for kw in suggest_target_keywords]
        engine.dispose()
        return suggest_target_keywords

    @staticmethod
    def get_target_keyword_by_user() -> pd.DataFrame:
        engine, metadata = QueryDatabaseJa.get_connection()
        query = f'''SELECT s as target_keyword, 
                    SUBSTRING(json_extract(properties, "$.user"), 2, LENGTH(json_extract(properties, "$.user")) - 2) as user_id,
                    SUBSTRING(json_extract(properties, "$.source[0]"), 2, LENGTH(json_extract(properties, "$.source[0]")) - 2) as source 
                    FROM {QueryDatabaseJa.schema}.oi_suggest_labels;
                '''
        target_keywords_by_user = pd.read_sql(query, con=engine)
        return target_keywords_by_user
    
    @staticmethod
    def upsert_google_suggest_trend(args : list):
        '''
        args = [[job_id, category, info]]
        '''
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "google_suggest_trend"
        def _query_col_list():
            query = """
                `job_id`,
                `category`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `job_id`=VALUES(`job_id`),
                `category`=VALUES(`category`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  3 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()
        
    @staticmethod
    def upsert_google_suggest_trend_target(args : list):
        engine, metadata = QueryDatabaseJa.get_connection()
        table_name = "google_suggest_trend_target"
        def _query_col_list():
            query = """
                `user_id`,
                `job_id`,
                `target_keyword`,
                `cnt`,
                `info`
                """
            return query

        def _update_value():
            query = """
                `user_id`=VALUES(`user_id`),
                `job_id`=VALUES(`job_id`),
                `target_keyword`=VALUES(`target_keyword`),
                `cnt`=VALUES(`cnt`),
                `info`=VALUES(`info`)
            """
            return query

        len_cols =  5 #컬럼의 개수 count
        value_length = str("%s, " * len_cols)[:-2]

        query = f"""
            INSERT INTO {table_name} (
                {_query_col_list()}
                )
            VALUES ({value_length})
            ON DUPLICATE KEY UPDATE
                {_update_value()}
            """

        with engine.connect().execution_options(autocommit=True) as connection:
            result_proxy = connection.execute(query, args)
            result_proxy.close()
        engine.dispose()



        
if __name__ == "__main__":
    res = QueryDatabaseJa.get_target_keyword_by_user()
    print(res)
    except_user_id = ['hubble']
    print(res[(~res['user_id'].isnull()) & ~(res['user_id'].isin(except_user_id))])
    
    

