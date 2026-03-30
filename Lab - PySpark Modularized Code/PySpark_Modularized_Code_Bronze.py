# Databricks notebook source
# DBTITLE 1,Library Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.functions import col, current_timestamp

# Configura a sessão do Spark para usar o horário do Brasil
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# COMMAND ----------

# DBTITLE 1,Listing Volume Files
arquivos = dbutils.fs.ls("/Volumes/workspace/estudos/csvs/")
for i in arquivos:
    print(f"Nome: {i.name}")

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

# DBTITLE 1,Load Functions
# MAGIC %run ./PySpark_Modularized_Code_Functions

# COMMAND ----------

# DBTITLE 1,Function to define CSV Schema
# MAGIC %skip
# MAGIC def get_customer_schema():
# MAGIC   return StructType([
# MAGIC         StructField('UUID',StringType(),True),
# MAGIC         StructField('Name',StringType(),True),
# MAGIC         StructField('Points',StringType(),True)
# MAGIC     ])
# MAGIC
# MAGIC def get_product_schema():
# MAGIC   return StructType([
# MAGIC         StructField('1',StringType(),True),
# MAGIC         StructField('ChatMessage',StringType(),True),
# MAGIC         StructField('Mensagem_do_chat_da_Twitch',StringType(),True)
# MAGIC     ])
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Function to read the CSV into a DataFrame
# MAGIC %skip
# MAGIC if 'customers.csv' in arquivo:
# MAGIC     schema = get_customer_schema()
# MAGIC elif 'products.csv' in arquivo:
# MAGIC     schema = get_product_schema()
# MAGIC else:
# MAGIC     raise ValueError("Arquivo não reconhecido para definição de schema")
# MAGIC
# MAGIC def read_csv_data(arquivo):
# MAGIC
# MAGIC     return(
# MAGIC         spark.read
# MAGIC         .format('csv')
# MAGIC         .option('header','true')
# MAGIC         .option('delimiter',';')
# MAGIC         .schema(schema)
# MAGIC         .load(path_origem + arquivo)
# MAGIC         .select(
# MAGIC             '*',
# MAGIC             '_metadata.file_name',
# MAGIC             current_timestamp().alias("ingestion_bronze")
# MAGIC         )
# MAGIC     )
# MAGIC     

# COMMAND ----------

# DBTITLE 1,Function to save DF - Delta table
# MAGIC %skip
# MAGIC #
# MAGIC #### dataframe: o DataFrame Spark a ser salvo
# MAGIC #### table_name: nome da tabela a ser salva
# MAGIC #### mode: modo de escrita (overwrite, append, etc.)
# MAGIC #
# MAGIC def save_df_to_table(dataframe,table_name,mode):
# MAGIC
# MAGIC # Evolução de schema
# MAGIC     schema_option = 'overwriteSchema' if mode == 'overwrite' else 'mergeSchema'
# MAGIC
# MAGIC     (
# MAGIC         dataframe
# MAGIC         .write
# MAGIC         .format('delta')
# MAGIC         .option(schema_option, 'true')
# MAGIC         .mode(mode)
# MAGIC         .saveAsTable(table_name)
# MAGIC     )
# MAGIC

# COMMAND ----------

# DBTITLE 1,File Read
lab_raw = (read_csv_data(arquivo))

# COMMAND ----------

# DBTITLE 1,Save as Bronze Table
arquivo = arquivo.replace(".csv", "")

save_df_to_table(lab_raw,f'{catalogo_destino}.{schema_destino}.tab_{arquivo}_{layer}','overwrite')

# COMMAND ----------

# DBTITLE 1,For Testing
querie = spark.sql(
    f"""
    SELECT
    *
    FROM {catalogo_destino}.{schema_destino}.tab_{arquivo}_{layer}
    """
)

display(querie)
