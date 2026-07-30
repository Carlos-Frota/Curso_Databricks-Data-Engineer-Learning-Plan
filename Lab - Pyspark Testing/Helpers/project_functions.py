from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import when, col

# Função que define a estrutura dos arquivos CSV para garantir tipos de dados consistentes.

def get_health_csv_schema():
    return StructType([
        StructField("id", IntegerType(), True),             # ID único do registro
        StructField("PII", StringType(), True),             # Informações Pessoais Identificáveis 
        StructField("date", DateType(), True),              # Data do registro
        StructField("HighCholest", IntegerType(), True),    # Indicador de colesterol
        StructField("HighBP", DoubleType(), True),          # Pressão sanguínea 
        StructField("BMI", DoubleType(), True),             # IMC (Índice de Massa Corporal)
        StructField("Age", DoubleType(), True),             # Idade 
        StructField("Education", DoubleType(), True),       # Nível de escolaridade
        StructField("income", IntegerType(), True)          # Faixa de renda
    ])

def high_cholest_map(col_name: str):
  """
  Mapeia o valor do colesterol categorias
  Args:
    col_name: nome da coluna que será transformada.
  Returns:
     pysparq.sql.Column: Nova coluna das categorias do colesterol
  """
  return (
      when(col(col_name) == 0, 'Normal')
      .when(col(col_name) == 1, 'Acima da Media')
      .when(col(col_name) == 2, 'Alto')
      .otherwise('Desconhecido')
  )

def group_ages_map(col_name: str):
  """
  Mapeia o valor da idade em faixas etarias
  Args:
    col_name: nome da coluna que será transformada.
  Returns:
     pysparq.sql.Column: Nova coluna das categorias do colesterol
  """
  return (
      when((col(col_name) >= 0) & (col(col_name) <= 9), '0-9')
      .when((col(col_name) >= 10) & (col(col_name) <= 19), '10-19')
      .when((col(col_name) >= 20) & (col(col_name) <= 29), '20-29')
      .when((col(col_name) >= 30) & (col(col_name) <= 39), '30-39')
      .when((col(col_name) >= 40) & (col(col_name) <= 49), '40-49')
      .when(col(col_name) >= 50, 'Mais de 50')
      .otherwise('Desconhecido')
  )