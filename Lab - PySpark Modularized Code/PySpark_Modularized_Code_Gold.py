# Databricks notebook source
# DBTITLE 1,Library Imports
#from pyspark.sql.functions import col, when, udf, current_timestamp

# Configura a sessão do Spark para usar o horário do Brasil
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# COMMAND ----------

# DBTITLE 1,Parameters Definition
dbutils.widgets.text('arquivo','customers')
dbutils.widgets.text('catalogo','workspace')
dbutils.widgets.text('schema','estudos')
dbutils.widgets.text('layer','gold')

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

# DBTITLE 1,Function to Read Silver Tables
# MAGIC %skip
# MAGIC def read_silver (table_name):
# MAGIC     return spark.read.table(f'{catalogo_destino}.{schema_destino}.tab_{table_name}_silver')

# COMMAND ----------

# DBTITLE 1,Function to create a Gold- aggregated counts
# MAGIC %skip
# MAGIC def agg_gold (table_origem):
# MAGIC     query = f'''
# MAGIC    CREATE OR REPLACE TABLE {catalogo_destino}.{schema_destino}.tab_{table_origem}_gold AS
# MAGIC    SELECT 
# MAGIC          UUID
# MAGIC         ,MAX(Primeiro_Nome) as Nome
# MAGIC         ,COUNT(UUID) as Qtd_Participacoes
# MAGIC         ,SUM(Points) as Total_Pontos
# MAGIC         ,CAST(AVG(Points) AS DECIMAL(10,2)) as Media_Pontos
# MAGIC         ,MIN(Points) as Pontuacao_Minima
# MAGIC         ,MAX(Points) as Pontuacao_Maxima
# MAGIC         ,categoria_Pontos as Classificao
# MAGIC         ,current_timestamp() as ingestion_gold
# MAGIC     FROM {catalogo_destino}.{schema_destino}.tab_{table_origem}_silver
# MAGIC     GROUP BY 
# MAGIC          UUID
# MAGIC         ,categoria_Pontos
# MAGIC     '''
# MAGIC     
# MAGIC     spark.sql(query)

# COMMAND ----------

# DBTITLE 1,Function to save DF - Delta table
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

# DBTITLE 1,Load DF - Gold
silver_df = read_silver(arquivo)

# COMMAND ----------

# DBTITLE 1,agg_gold(arquivo)
arquivo = arquivo.replace(".csv", "")

agg_gold(arquivo)

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
