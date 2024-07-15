
from useful_fct import create_semantic_layer, run_sql
from snowflake.snowpark import Session, DataFrame, Window, WindowSpec
import json
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import table_function, lit, col, parse_json

class SemanticModel:
    def __init__(self,sm_name='',dmt_db_src='',dmt_schema_src='',dmt_table_src='',entities=[],timestamp_col='',description=''):
        self.sm_name=sm_name
        self.dmt_db_src=dmt_db_src
        self.dmt_schema_src=dmt_schema_src
        self.dmt_table_src=dmt_table_src
        self.entities=entities
        self.timestamp_col=timestamp_col
        self.description=description

class Dimension: 
       def __init__(self,dimension_name='', dimension_type='',type_param='',desc=''):
             self.dimension_type = dimension_type
             self.dimension_name=dimension_name
             self.type_param=type_param
             self.desc=desc 

class Measure:
      def __init__(self,measure_name='',descr='',agg='',exp=''):
            self.measure_name = measure_name
            self.descr=descr
            self.agg=agg
            self.exp=exp

class Entity:
        def __init__(self,entity_name='', join_keys=None,desc=''):
             self.join_keys = join_keys if join_keys is not None else []
             self.entity_name=entity_name
             self.desc=desc

class Dimension: 
       def __init__(self,dimension_name='', dimension_type='',type_param='',desc=''):
             self.dimension_type = dimension_type
             self.dimension_name=dimension_name
             self.type_param=type_param
             self.desc=desc



class Semantic_Layer:


    def __init__(self,session='',db_name='',schema_name=''): 
        self.session=session
        self.db_name=db_name
        self.schema_name=schema_name

    def create_semantic_layer(self): 
        create_semantic_layer(self.session,self.db_name,self.schema_name)

    def register_entity(self,entity):

        run_sql(f'''CREATE TAG IF NOT EXISTS {self.db_name}.{self.schema_name}.SNOW_METRIC_STORE_ENTITY_{entity.entity_name}
                    ALLOWED_VALUES '{entity.join_keys}'
                    COMMENT = '{entity.desc}\'''',self.session)

    def get_entities(self):
        query=self.session.sql(f'''SHOW TAGS LIKE 'SNOW_METRIC_STORE_ENTITY_%' IN SCHEMA {self.db_name}.{self.schema_name}''')
        df = query.with_column("entity_name", query['"name"'].substr(26,1000))  # Manipulation de la colonne "name"

        df=df.select(col("entity_name"),col('"allowed_values"').alias("keys"),col('"comment"').alias("description"))
        df.show()

    
    def register_semantic_model(self,sm):
        run_sql(f'''CREATE or replace view {self.db_name}.{self.schema_name}.{sm.sm_name}
                COMMENT = '{sm.description}'
                TAG (
                    {self.db_name}.{self.schema_name}.SNOW_MS_{self.schema_name}_OBJECT = '{{"type": "sm_view"}}',
{self.db_name}.{self.schema_name}.SNOW_MS_{self.schema_name}_SEMANTIC_MODEL_METADATA = '{{"entities": ["{sm.entities.entity_name}"], "timestamp_col": "{sm.timestamp_col}"}}',
{self.db_name}.{self.schema_name}.SNOW_METRIC_STORE_ENTITY_ORDERS = '{sm.entities.join_keys}'
                )
                AS (select * from {sm.dmt_db_src}.{sm.dmt_schema_src}.{sm.dmt_table_src})''',self.session)



    def get_dimensions(self,sm): 

        run_sql(f'''use schema {self.db_name}.{self.schema_name}; ''',self.session)

        query=self.session.table_function(f"{self.db_name}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS", lit(sm.sm_name), lit('table')).filter(col("LEVEL") == "COLUMN")

        df_selected=query.select(col("OBJECT_NAME").alias("SEMANTIC_MODEL"), col("COLUMN_NAME").alias("DIMENSION"),col("TAG_VALUE").alias("DIMENSION_DETAILS"))
        df_selected.show()
        return df_selected
    
    def register_dimension(self,sm,dimension):
        run_sql(f'''ALTER TABLE {self.db_name}.{self.schema_name}.{sm.sm_name} MODIFY COLUMN {dimension.dimension_name} SET TAG {self.db_name}.{self.schema_name}.SNOW_MS_{self.schema_name}_SEMANTIC_MODEL_DIMENSIONS = '{{"type":"{dimension.dimension_type}", "type_params":{dimension.type_param},"descripton":{dimension.desc}}}';
''',self.session)
    
    def register_measure(self,sm,measure):

        run_sql(f'''ALTER TAG {self.db_name}.{self.schema_name}.SNOW_MS_{self.schema_name}_MEASURES ADD ALLOWED_VALUES '{{{sm.sm_name}:{measure.measure_name}}}';
 ''',self.session)
        
        run_sql(f'''create tag if not exists {self.db_name}.{self.schema_name}.SNOW_MS_MEASURE_{sm.sm_name}_{measure.measure_name} COMMENT='{measure.measure_name} tag';
 ''',self.session)
        
        run_sql(f''' ALTER TABLE {self.db_name}.{self.schema_name}.{sm.sm_name} SET TAG {self.db_name}.{self.schema_name}.SNOW_MS_MEASURE_{sm.sm_name}_{measure.measure_name} = '{{"description":"{measure.descr}", "aggregation":"{measure.agg}", "expression":"{measure.exp}"}}';''',self.session)
    
    def get_measures(self,sm): 
        run_sql(f'''use schema {self.db_name}.{self.schema_name}; ''',self.session)
        query=self.session.table_function(f"{self.db_name}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS", lit(sm.sm_name), lit('table')).filter(col("LEVEL") == "TABLE").filter(col("TAG_NAME").startswith('SNOW_MS_MEASURE_'))

        df_selected=query.select(col("OBJECT_NAME").alias("SEMANTIC_MODEL"), col("TAG_NAME").alias("MEASURE"),col("TAG_VALUE").alias("MEASURE_DETAILS"))
        df_json= df_selected.with_column('parsed_json',parse_json(col('MEASURE_DETAILS')))
        df_json = df_json.with_column("aggregation", df_json["parsed_json"]["aggregation"]).with_column("description", df_json["parsed_json"]["description"]).with_column("expression", df_json["parsed_json"]["expression"]).distinct()
        df_json=df_json.select(col("SEMANTIC_MODEL"),col("MEASURE"),col("description"),col("aggregation"),col("expression"))
        df_json.show()
        return df_selected    