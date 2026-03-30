# Databricks notebook source
# MAGIC %md
# MAGIC ### Funcões utilizadas no Projeto

# COMMAND ----------

# DBTITLE 1,Library Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.functions import col, current_timestamp

# Configura a sessão do Spark para usar o horário do Brasil
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# COMMAND ----------

# DBTITLE 1,Parameters Definition
dbutils.widgets.text('path_origem','/Volumes/workspace/estudos/csvs/')
dbutils.widgets.text('catalogo','workspace')
dbutils.widgets.text('schema','estudos')
dbutils.widgets.dropdown('arquivo','customers.csv',['customers.csv','products.csv'])
dbutils.widgets.text('layer','bronze')

# COMMAND ----------

# DBTITLE 1,Load Parameters
def get_param(parametro):
    try:
        # Tenta primeiro pegar o parâmetro vindo do Job (com sufixo _param)
        return dbutils.widgets.get(f"{parametro}_param")
    except:
        # Se não encontrar, pega o parâmetro configurado no próprio Notebook
        return dbutils.widgets.get(parametro)

# Capturando as variáveis
path_origem = get_param('path_origem')
catalogo_destino = get_param('catalogo')
schema_destino = get_param('schema')
layer = get_param('layer')
arquivo = get_param('arquivo').lower()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Globais

# COMMAND ----------

# DBTITLE 1,d. Function to save DF - Delta table
#
#### dataframe: o DataFrame Spark a ser salvo
#### table_name: nome da tabela a ser salva
#### mode: modo de escrita (overwrite, append, etc.)
#
def save_df_to_table(dataframe,table_name,mode):

# Evolução de schema
    schema_option = 'overwriteSchema' if mode == 'overwrite' else 'mergeSchema'

    (
        dataframe
        .write
        .format('delta')
        .option(schema_option, 'true')
        .mode(mode)
        .saveAsTable(table_name)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze

# COMMAND ----------

# DBTITLE 1,a. Function to define CSV Schema
def get_customer_schema():
  return StructType([
        StructField('UUID',StringType(),True),
        StructField('Name',StringType(),True),
        StructField('Points',StringType(),True)
    ])

def get_product_schema():
  return StructType([
        StructField('1',StringType(),True),
        StructField('ChatMessage',StringType(),True),
        StructField('Mensagem_do_chat_da_Twitch',StringType(),True)
    ])



# COMMAND ----------

# DBTITLE 1,b. Function to read the CSV into a DataFrame
if 'customers' in arquivo:
    schema = get_customer_schema()
elif 'products' in arquivo:
    schema = get_product_schema()
else:
    raise ValueError("Arquivo não reconhecido para definição de schema")

def read_csv_data(arquivo):

    return(
        spark.read
        .format('csv')
        .option('header','true')
        .option('delimiter',';')
        .schema(schema)
        .load(path_origem + arquivo)
        .select(
            '*',
            '_metadata.file_name',
            current_timestamp().alias("ingestion_bronze")
        )
    )
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver

# COMMAND ----------

# DBTITLE 1,Function to Read Bronze Tables
def read_bronze (table_name):
    return spark.read.table(f'{catalogo_destino}.{schema_destino}.tab_{table_name}_bronze')

# COMMAND ----------

# DBTITLE 1,c1. Function to Convert Values in Categories
def points_map(col_name):
    # Converte tipo coluna e armazena em uma variável
    point_int = col(col_name).try_cast("int")
    
    return (
        when(point_int.isNull(), 'Valor Inválido') # Proteção contra nulos ou erros de cast
        .when(point_int < 1000, 'Abaixo de 1000')
        .when(point_int < 2000, 'Entre 1000 e 2000')
        .otherwise('Acima de 2000')
    )

# COMMAND ----------

# DBTITLE 1,c2. Function to Split Name
separar_nome = udf(lambda x: x.split(" ")[0].split("_")[0].upper() if x else None, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold

# COMMAND ----------

# DBTITLE 1,Function to Read Silver Tables
def read_silver (table_name):
    return spark.read.table(f'{catalogo_destino}.{schema_destino}.tab_{table_name}_silver')

# COMMAND ----------

# DBTITLE 1,e. Function to create a Gold- aggregated counts
def agg_gold (table_origem):
    query = f'''
   CREATE OR REPLACE TABLE {catalogo_destino}.{schema_destino}.tab_{table_origem}_gold AS
   SELECT 
         UUID
        ,MAX(Primeiro_Nome) as Nome
        ,COUNT(UUID) as Qtd_Participacoes
        ,SUM(Points) as Total_Pontos
        ,CAST(AVG(Points) AS DECIMAL(10,2)) as Media_Pontos
        ,MIN(Points) as Pontuacao_Minima
        ,MAX(Points) as Pontuacao_Maxima
        ,categoria_Pontos as Classificao
        ,current_timestamp() as ingestion_gold
    FROM {catalogo_destino}.{schema_destino}.tab_{table_origem}_silver
    GROUP BY 
         UUID
        ,categoria_Pontos
    '''
    
    spark.sql(query)
