#Import Pytest e outras bibliotecas
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, LongType
from pyspark.sql.functions import when, col
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

#Importar as funções criadas
from Helpers import project_functions
#dbutils.import_notebook("Helpers.project_functions")

# @pytest.fixture é um fixture do pytest chamado spark com escopo de sessão, o que significa que ele será configurado uma vez por sessão de teste e compartilhado entre múltiplas funções de teste.

# Cria uma SparkSession usando SparkSession.builder.getOrCreate() (que recupera uma sessão existente ou cria uma nova caso não exista) e então fornece (yields) a sessão spark para as funções de teste que utilizam esse fixture, permitindo que elas acessem o ambiente Spark para seus testes.

@pytest.fixture(scope='session')
def spark():
    # Passa a funcao Spark para a funcao de teste
    # •	Se já existir uma sessão Spark → reutiliza Se não existir → cria uma nova 
    spark = SparkSession.builder.getOrCreate()
    # Entrega a sessão Spark para os testes 
    yield spark

def test_get_health_csv_schema_match():
    # Definir o schema esperado
    expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("PII", StringType(), True), 
        StructField("date", DateType(), True),
        StructField("HighCholest", IntegerType(), True),
        StructField("HighBP", DoubleType(), True), 
        StructField("BMI", DoubleType(), True),
        StructField("Age", DoubleType(), True), 
        StructField("Education", DoubleType(), True),
        StructField("income", IntegerType(), True)
    ])

    # Obtém o schema da nossa função
    actual_schema = project_functions.get_health_csv_schema()

    # Verifica se o schema atual corresponde ao esperado
    assertSchemaEqual(actual_schema,expected_schema)

def test_high_cholest_column_valid_map(spark):
    # Define um dataframe de teste e cria um dataframe dos dados
    # Obervar que temos que passar na funcao a sessao spark criada acima
    data = [
        (0,),
        (1,),
        (2,),
        (3,),
        (4,),
        (None,)
    ]
    sample_df = spark.createDataFrame(data,["value"])
   # Aplicar funcao nos dados de entrada
    actual_df = sample_df.withColumn("actual", project_functions.high_cholest_map("value"))

    # Criar DataFrame estático com os resultados esperados da função highcholest_map acima:
    expected_df = spark.createDataFrame(
        [
            (0,'Normal'),
            #(0, "Bad Value Cause Error"),	### <-- change the value to cause an error        
            (1,'Acima da Media'),
            (2, "Alto"),
            (3, "Desconhecido"),
            (4, "Desconhecido"),
            (None, "Desconhecido")
        ],
        schema = StructType(
            [
                StructField('value',LongType(),True),
                StructField('actual',StringType(),False),
            ])
    )
    # Checar se os resultados são iguais. Se não retorna erro
    assertDataFrameEqual(actual_df,expected_df)
    print("Test passed!")

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
