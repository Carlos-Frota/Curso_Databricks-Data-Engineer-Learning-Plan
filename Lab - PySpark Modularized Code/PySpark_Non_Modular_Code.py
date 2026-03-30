# Databricks notebook source
# DBTITLE 1,importar bibliotecas
from pyspark.sql.functions import when, col, current_timestamp, upper, split

# COMMAND ----------

# DBTITLE 1,Listando arquivos do Volume
arquivos = dbutils.fs.ls("/Volumes/workspace/estudos/csvs/")

for arquivo in arquivos:
    print(f"Nome: {arquivo.name}")

# COMMAND ----------

# DBTITLE 1,Definir parametros
path_origem = '/Volumes/workspace/estudos/csvs/'
catalogo_destino = 'estudos'

# COMMAND ----------

# DBTITLE 1,Ingerir CSV
# Ingest the CSV file and add metadata columns for the ingested data

lab_raw = (
    spark.read
    .format('csv')
    .option("header","true")
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load(path_origem)
    .select(
        "*"
        ,
        "_metadata.file_name",
        #"_metadata.modificationTime",
        current_timestamp().alias("ingestion_timestamp")
    )
    #.filter(col("file_name")=='customers.csv')
)

# COMMAND ----------

lab_raw.display()

# COMMAND ----------

# DBTITLE 1,Salvar os dados na tabela bronze
# Save the ingested data as a Bronze table in Delta format

(
    lab_raw
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"workspace.{catalogo_destino}.tab_lab_bronze")
)

# COMMAND ----------

# DBTITLE 1,Analisar dados tabela bronze
# MAGIC %sql
# MAGIC describe table workspace.estudos.tab_lab_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extras Bronze

# COMMAND ----------

# DBTITLE 1,Ajustar valores de points antes da conversão
from pyspark.sql.types import IntegerType

# Exibir o resultado da consulta antes de seguir o passo 2
df_sql = spark.sql(
    """
    SELECT DISTINCT 
        Points
    FROM 
        workspace.estudos.tab_lab_bronze
    """
)
display(df_sql)

lab_bronze = spark.table(f"workspace.{catalogo_destino}.tab_lab_bronze")

lab_silver = (
    lab_bronze
    .withColumn("Points_int", col("Points").try_cast(IntegerType()))
    .withColumn(
        "Categoria_Pontos",
        when(col("Points_int") < 1000, "Abaixo de 1000")
        .when((col("Points_int") >= 1000) & (col("Points_int") <= 2000), "Entre 1000 e 2000")
        .otherwise("Acima de 2000")
    )
)

# COMMAND ----------

# DBTITLE 1,Separar nome com funcao do PysPark
from pyspark.sql.functions import col, split, upper

# Lógica: Divide pelo espaço, pega o primeiro item (getItem(0)) e converte para maiúsculo
lab_silver = lab_silver.withColumn(
    "Primeiro_Nome", 
    upper(split(col("Nome_Coluna_Original"), " ").getItem(0))
)

# COMMAND ----------

# DBTITLE 1,Separar nome com Funcao com lambda
# USANDO LAMBDA
# QUANDO VOCÊ VAI UTILIZAR A FUNCAO UMA UNICA VEZ
# FUNCOES SIMPLES

# O PySpark processa dados de forma distribuída, então ele não consegue executar uma função Python pura (lambda) diretamente dentro do withColumn sem um "envelope" (wrapper).

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 1. Definimos a função lambda dentro da UDF
separar_nome = udf(lambda x: x.split(" ")[0].split("_")[0].upper() if x else None, StringType())

# 2. Aplicamos no DataFrame
lab_silver = lab_silver.withColumn("Primeiro_Nome", separar_nome(col("Name")))

# COMMAND ----------

# DBTITLE 1,Ler os dados da bronze
##
## Bronze -> Silver
##

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

lab_bronze = spark.table(f"workspace.{catalogo_destino}.tab_lab_bronze")

# Funcao em um dataframe udf
separar_nome = udf(lambda x: x.split(" ")[0].split("_")[0].upper() if x else None, StringType())

# 2. Aplicamos no DataFrame
lab_silver = lab_silver

lab_silver = (
    lab_bronze
    .withColumn("Points_int", 
                col("Points")
                .try_cast(IntegerType())
                )
    .withColumn("Categoria_Pontos",
        when(col("Points_int").isNull(), "Dados inválidos")
        .when(col("Points_int") < 1000, "Abaixo de 1000")
        .when((col("Points_int") >= 1000) & (col("Points_int") <= 2000), "Entre 1000 e 2000")
        .otherwise("Acima de 2000")
    )
    .withColumn("Primeiro_Nome_Spark", 
        upper(split(split(col("Name"), " ").getItem(0), "_").getItem(0))
    )
    .withColumn("Primeiro_Nome_lambda", 
        separar_nome(col("Name"))
    )
    # Drop unnecessary columns (metadata columns)
    .drop("file_name", "ingestion_timestamp")

)


# COMMAND ----------

# DBTITLE 1,Gravar dados na Silver
(
    lab_silver.write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f"workspace.{catalogo_destino}.tab_lab_silver")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extras Silver

# COMMAND ----------

# DBTITLE 1,Incluindo agregações - PASSO 1: Agregação e Criação da View
spark.sql(
    f"""
    SELECT 
        uuid,
        AVG(Points_int) AS media_ponto,
        SUM(Points_int) AS total_pontos,
        MIN(Points_int) AS pontuacao_minima,
        MAX(Points_int) AS pontuacao_maxima,
        COUNT(uuid) AS quantidade 
    FROM 
        workspace.{catalogo_destino}.tab_lab_silver
    GROUP BY 
        uuid
    """).createOrReplaceTempView("media_pontos")

# COMMAND ----------

# DBTITLE 1,Incluindo agregações - PASSO 2: Validação e Junção
# --- PASSO 2: Validação e Junção ---
Validacao = spark.sql(f"""
    SELECT 
        A.UUID,
        A.Primeiro_Nome_Spark as Name,
        
        -- Agregações necessárias para o GROUP BY funcionar
        MAX(B.quantidade) as Qtd_, 
        MAX(B.total_pontos) as total_pontos,
        MAX(B.media_ponto) as media_ponto,
        MAX(B.pontuacao_maxima) as pontuacao_maxima,
        MIN(B.pontuacao_minima) as pontuacao_minima,
        
        A.categoria_Pontos
    FROM 
        workspace.{catalogo_destino}.tab_lab_silver A
    LEFT JOIN 
        media_pontos B 
    ON 
        A.UUID = B.uuid 
    
    -- Agrupa pelo que não tem função (UUID, Nome, Categoria)
    GROUP BY ALL
    
    -- Ordena pelo Alias criado acima
    ORDER BY Qtd_ DESC 
""")

display(Validacao)

# COMMAND ----------

# DBTITLE 1,Incluindo Agregações - Windows Functions
Validacao = spark.sql(f"""
    SELECT DISTINCT
        UUID
        ,MAX(Primeiro_Nome_Spark) OVER(PARTITION BY uuid) as Nome
        ,COUNT(uuid) OVER(PARTITION BY uuid) as Qtd_Participacoes
        ,SUM(Points_int) OVER(PARTITION BY uuid) as Total_Pontos
        ,CAST(AVG(Points_int) OVER(PARTITION BY uuid) AS DECIMAL(10,2)) as Media_Pontos
        ,MIN(Points_int) OVER(PARTITION BY uuid) as Pontuacao_Mininma
        ,MAX(Points_int) OVER(PARTITION BY uuid) as Pontuacao_Maxima
        ,categoria_Pontos        
    FROM 
        workspace.{catalogo_destino}.tab_lab_silver
    ORDER BY Qtd_Participacoes DESC

""")

display(Validacao)

# COMMAND ----------

# DBTITLE 1,Validar agregações - incluir novos registro bronze
# MAGIC %sql
# MAGIC INSERT INTO workspace.ESTUDOS.tab_lab_bronze
# MAGIC (UUID, Points)
# MAGIC VALUES ('aa3eaf74-6d9c-4859-b733-5a18a3b2f71b','250');
# MAGIC VALUES ('2d3d2dce-d353-4961-ad39-46723efe2100','250');
# MAGIC VALUES ('ca95ef2a-5129-40f4-acbc-2ced25940032','250');
# MAGIC VALUES ('b2f9d026-0727-4125-b84b-c60af3148a15','250');
# MAGIC VALUES ('6e1bc660-02eb-49ac-aee6-592328504897','250');
# MAGIC VALUES ('95ef4c1e-b21a-468e-b419-ad08333c4948','250');
# MAGIC VALUES ('ca95ef2a-5129-40f4-acbc-2ced25940032','250');
# MAGIC VALUES ('e7c79c1a-7693-4e63-a844-c755e0bc3bd3','250');
# MAGIC VALUES ('c4f241f8-2140-4f72-b1a3-904fcc51e38c','250');

# COMMAND ----------

# DBTITLE 1,Ler dados da Silver
spark.sql(f"""
    CREATE OR REPLACE TABLE workspace.{catalogo_destino}.tab_lab_gold AS
    SELECT DISTINCT
        UUID
        ,MAX(Primeiro_Nome_Spark) OVER(PARTITION BY uuid) as Nome
        ,COUNT(uuid) OVER(PARTITION BY uuid) as Qtd_Participacoes
        ,SUM(Points_int) OVER(PARTITION BY uuid) as Total_Pontos
        ,CAST(AVG(Points_int) OVER(PARTITION BY uuid) AS DECIMAL(10,2)) as Media_Pontos
        ,MIN(Points_int) OVER(PARTITION BY uuid) as Pontuacao_Mininma
        ,MAX(Points_int) OVER(PARTITION BY uuid) as Pontuacao_Maxima
        ,categoria_Pontos        
    FROM 
        workspace.{catalogo_destino}.tab_lab_silver
    ORDER BY Qtd_Participacoes DESC
    """)

# COMMAND ----------

# DBTITLE 1,For Testing
#describe table workspace.estudos.tab_lab_bronze
#display(Validacao)
spark.table(f"workspace.{catalogo_destino}.tab_lab_gold").display()
