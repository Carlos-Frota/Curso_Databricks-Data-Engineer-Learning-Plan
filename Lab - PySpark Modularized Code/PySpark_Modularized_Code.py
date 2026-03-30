# Databricks notebook source
# MAGIC %md
# MAGIC Modularizar o código do Lab - PySpark Code Nao Modular nas seguintes funções:
# MAGIC
# MAGIC get_csv_schema - Define e retorna o schema para os arquivos CSV. 
# MAGIC
# MAGIC read_csv_data - Lê o arquivo CSV em um DataFrame com metadados.
# MAGIC
# MAGIC points_map - Convertem valores em categorias.
# MAGIC
# MAGIC save_df_to_delta - Salva os dados transformados em uma tabela Delta.
# MAGIC
# MAGIC agg_gold - Agrega os dados para a tabela Gold.

# COMMAND ----------

# DBTITLE 1,Importacao Bibliotecas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp

# Configura a sessão do Spark para usar o horário do Brasil
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")


# COMMAND ----------

# DBTITLE 1,Listando arquivos do Volume
arquivos = dbutils.fs.ls("/Volumes/workspace/estudos/csvs/")
for i in arquivos:
    print(f"Nome: {i.name}")

# COMMAND ----------

# DBTITLE 1,Definir parametros
dbutils.widgets.text('path_origem','/Volumes/workspace/estudos/csvs/')
dbutils.widgets.text('catalogo','workspace')
dbutils.widgets.text('schema','estudos')
dbutils.widgets.dropdown('arquivo','customers.csv',['customers.csv','products.csv'])
dbutils.widgets.dropdown('layer','bronze',['bronze','silver','gold'])


# COMMAND ----------

# DBTITLE 1,Carregar parametros
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

# DBTITLE 1,Testes - carregou o arquivo?
df = (
    spark.read
    .format('csv')
    .option('header',True)
    .option('delimiter',';')
    .load(path_origem + arquivo)
    )
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criação das Funcoes

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
if 'customers.csv' in arquivo:
    schema = get_customer_schema()
elif 'products.csv' in arquivo:
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

# DBTITLE 1,For testing
#path_origem + arquivo
lab_raw.display()
#schema
#arquivo = 'products.csv'
#arquivo = 'customers.csv'
#arquivo

# COMMAND ----------

# DBTITLE 1,c1. Function to Convert Values in Categories
from pyspark.sql.functions import col, when

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
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

separar_nome = udf(lambda x: x.split(" ")[0].split("_")[0].upper() if x else None, StringType())

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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utilizando as funções criadas

# COMMAND ----------

# DBTITLE 1,Leitura do CSV
lab_raw = (read_csv_data(arquivo))

# COMMAND ----------

# DBTITLE 1,Salvando como tabela Bronze
arquivo = arquivo.replace(".csv", "")

save_df_to_table(lab_raw,f'{catalogo_destino}.{schema_destino}.tab_{arquivo}_{layer}','overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze to Silver

# COMMAND ----------

# DBTITLE 1,Customers
silver_df = (
    lab_raw
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

# COMMAND ----------

# DBTITLE 1,Products

silver_df = (
    lab_raw
    .select(
        col('1').alias('Cod_Product')
        ,col('ChatMessage').alias('Tipo_Mensagem')
        ,col('Mensagem_do_chat_da_Twitch').alias('Mensagem_Chat')
        ,current_timestamp().alias('ingestion_silver')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Salvando como tabela Silver
arquivo = arquivo.replace(".csv", "")

save_df_to_table(silver_df,f'{catalogo_destino}.{schema_destino}.tab_{arquivo}_{layer}','overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver to Gold

# COMMAND ----------

# DBTITLE 1,Carregar DF Gold
arquivo = arquivo.replace(".csv", "")

agg_gold(arquivo)

# COMMAND ----------

# DBTITLE 1,for testing
# MAGIC %sql
# MAGIC
# MAGIC --lab_raw.display()
# MAGIC
# MAGIC select 
# MAGIC   * 
# MAGIC from 
# MAGIC   --workspace.estudos.tab_products_bronze
# MAGIC   --workspace.estudos.tab_customers_bronze
# MAGIC   --workspace.estudos.tab_products_silver
# MAGIC   --workspace.estudos.tab_customers_silver
# MAGIC   workspace.estudos.tab_customers_gold
# MAGIC   
# MAGIC
# MAGIC

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.estudos"))

# COMMAND ----------

# MAGIC %md
# MAGIC
