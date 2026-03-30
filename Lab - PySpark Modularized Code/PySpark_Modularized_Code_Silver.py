# Databricks notebook source
# DBTITLE 1,Library Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, udf, current_timestamp

# Configura a sessão do Spark para usar o horário do Brasil
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# COMMAND ----------

# DBTITLE 1,Parameters Definition
dbutils.widgets.dropdown('arquivo','customers',['customers','products'])
dbutils.widgets.text('catalogo','workspace')
dbutils.widgets.text('schema','estudos')
dbutils.widgets.text('layer','silver')



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
arquivo = get_param('arquivo').lower()
catalogo_destino = get_param('catalogo')
schema_destino = get_param('schema')
layer = get_param('layer')


# COMMAND ----------

# DBTITLE 1,Load Functions
# MAGIC %run ./PySpark_Modularized_Code_Functions

# COMMAND ----------

# DBTITLE 1,Function to Read Bronze Tables
# MAGIC %skip
# MAGIC def read_bronze (table_name):
# MAGIC     return spark.read.table(f'{catalogo_destino}.{schema_destino}.tab_{table_name}_bronze')
# MAGIC

# COMMAND ----------

# DBTITLE 1,Function to Convert Values in Categories
# MAGIC %skip
# MAGIC def points_map(col_name):
# MAGIC     # Converte tipo coluna e armazena em uma variável
# MAGIC     point_int = col(col_name).try_cast("int")
# MAGIC     
# MAGIC     return (
# MAGIC         when(point_int.isNull(), 'Valor Inválido') # Proteção contra nulos ou erros de cast
# MAGIC         .when(point_int < 1000, 'Abaixo de 1000')
# MAGIC         .when(point_int < 2000, 'Entre 1000 e 2000')
# MAGIC         .otherwise('Acima de 2000')
# MAGIC     )

# COMMAND ----------

# DBTITLE 1,Function to Split Name
# MAGIC %skip
# MAGIC separar_nome = udf(lambda x: x.split(" ")[0].split("_")[0].upper() if x else None, StringType())

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

# DBTITLE 1,Load DF - Silver
bronze_df = read_bronze(arquivo)

if 'customers' in arquivo:
    silver_df = (
    bronze_df
    .withColumn('Categoria_Pontos', points_map('Points'))
    .withColumn('Primeiro_Nome', separar_nome('Name'))
    .select(
        'UUID'
        ,'Primeiro_Nome'
        ,col('Points').try_cast('int').alias('Points')
        ,'Categoria_Pontos'
        ,current_timestamp().alias('ingestion_silver')
    )
)
elif 'products' in arquivo:
    silver_df = (
    bronze_df
    .select(
        col('1').alias('Cod_Product')
        ,col('ChatMessage').alias('Tipo_Mensagem')
        ,col('Mensagem_do_chat_da_Twitch').alias('Mensagem_Chat')
        ,current_timestamp().alias('ingestion_silver')
    )
)
else:
    raise ValueError("Arquivo não reconhecido para definição de schema")




# COMMAND ----------

# DBTITLE 1,Save as Silver Table
save_df_to_table(silver_df,f'{catalogo_destino}.{schema_destino}.tab_{arquivo}_{layer}','overwrite')

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
