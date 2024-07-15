from snowflake.snowpark import Session, DataFrame, Window, WindowSpec
from os import listdir
import json
import numpy as np




def run_sql(sql_statement, session):
    """
    Create a function to simplify the execution of SQL text strings via Snowpark.
    sql_statement : SQL statement as text string
    session : Snowpark session.  If none, defaults session is assumed to be set in calling environmentß
    """
    result = session.sql(sql_statement).collect()
    print(sql_statement, '\n', result, '\n')
    return {sql_statement : result} 
    #result = session.sql(sql_statement).queries['queries'][0]
    #print(result)

def create_semantic_layer(session,db_name,schema_name):
    run_sql(f''' create schema if not exists {db_name}.{schema_name}''', session)
    run_sql(f''' create tag if not exists {db_name}.{schema_name}.SNOW_MS_{schema_name}_OBJECT''', session)
    run_sql(f''' create tag if not exists {db_name}.{schema_name}.SNOW_MS_{schema_name}_SEMANTIC_MODEL_METADATA''', session)
    run_sql(f''' create tag if not exists {db_name}.{schema_name}.SNOW_MS_{schema_name}_SEMANTIC_MODEL_DIMENSIONS''', session)
    run_sql(f''' create tag if not exists {db_name}.{schema_name}.SNOW_MS_{schema_name}_MEASURES''', session)
s

def create_metric(session,entity_name,db_name,schema_name,key,desc):

    # Transformer la liste key en une chaîne de caractères avec des valeurs séparées par des virgules

    run_sql(f'''CREATE TAG IF NOT EXISTS {db_name}.{schema_name}.SNOW_METRIC_STORE_ENTITY_{entity_name}
                    ALLOWED_VALUES '{key}'
                    COMMENT = '{desc}' ''',session)

