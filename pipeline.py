from argparse import PARSER
from itertools import count
import logging
import apache_beam as beam
import os
from apache_beam.transforms.core import Map
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pvalue import PCollection
from google.protobuf.descriptor_pb2 import FileOptions
from apache_beam.io.filesystem import FileSystem
from apache_beam.io import fileio
import pyarrow

from transform.transform import extrair

# Camadas do storage
STORAGE_BRONZE_PATH = "gs://yellow_trip_estudos/bronze/"
STORAGE_SILVER_PATH = "gs://yellow_trip_estudos/silver/"


currentPath = os.path.abspath(os.getcwd())
# Iniciando a conta de serviço Google
serviceAccount = currentPath+"/apachebeam.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serviceAccount


def mediaValores(valores):
    print(valores)


def run():

    schemaBigQuery = 'VendorID:STRING, pickup_datetime:STRING, dropoff_datetime:STRING, passenger_count:INTEGER, trip_distance:STRING'
    tabelaBigQuery = 'apachebeam-328222:mydatabase.yellowtripdata'



    parquetFinalSchema = pyarrow.schema(
        [('VendorID', pyarrow.string()), ('pickup_datetime', pyarrow.string()),
        ('dropoff_datetime', pyarrow.string()), ('passenger_count', pyarrow.int64()),
        ('trip_distance', pyarrow.string())]
    )


    options_config = {
        'project': 'apachebeam-328222',
        'runner':'DataflowRunner',
        'region':'southamerica-east1',
        'staging_location': 'gs://yellow_trip_estudos/jobs/temp',
        'temp_location': 'gs://yellow_trip_estudos/jobs/temp',
        'template_location': 'gs://yellow_trip_estudos/template/batch_job_yellowtrip',
        'job_name':'yellow-trip1-job-dataflow',
        'service_account_email':'apachebeam@apachebeam-328222.iam.gserviceaccount.com',
        'save_main_session': True,
        'requirements_file':'requirements.txt',
        'setup_file':'./setup.py'
    }

    pipeline_options = PipelineOptions.from_dictionary(options_config)


    p1 = beam.Pipeline(options=pipeline_options)

    # descobreExtraiArquivos = (
    #     p1
    #     | "Lista arquivos no diretorio" >> fileio.MatchFiles("{}{}".format(STORAGE_BRONZE_PATH,"*.zip"))
    #     | "Obtem apenas arquivos zipados" >> beam.Map(lambda x: x.path)
    #     | "Descompacta arquivos zipados" >> beam.ParDo(extrair()) 
    # )

    # carregaArquivos = (
    #     descobreExtraiArquivos
    #     | "Encontrando arquivos csv no diretorio Bronze" >> fileio.MatchFiles("{}{}".format(STORAGE_BRONZE_PATH,"*.csv"))
    #     | "Mapeando arquivos CSV encontrados" >> beam.Map(lambda x: x.path)
    #     | "Leitura dos arquivos CSV" >> beam.io.ReadAllFromText()
    # )

    # escritaParquet = (
    #     carregaArquivos
    #     | "Realiza decode adequado para parquet" >> beam.Map(lambda x: x.encode().decode('utf8'))
    #     | "Divide os dados em listas separadas por virgula" >> beam.Map(lambda x: x.split(",")) 
    #     | "Retirada do cabeçalho" >> beam.Filter(lambda x: x[3] != 'passenger_count')
    #     | "Seleção das colunas" >> beam.Map(lambda x: {"VendorID":x[0] , "pickup_datetime": x[1] , "dropoff_datetime": x[2], "passenger_count":int(x[3]), "trip_distance":x[4]} ) 
    #     | "Escrita do arquivo parquet" >> beam.io.WriteToParquet(STORAGE_SILVER_PATH, parquetFinalSchema)
    # )

    escritaBigQuery = (
        p1
        | "Carrega parquet" >> beam.io.ReadFromParquet(STORAGE_SILVER_PATH)
        | "Escrita no bigquery dos dados do parquet" >> beam.io.WriteToBigQuery(
                              table=tabelaBigQuery,
                              schema=schemaBigQuery,
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )   

    p1.run()

if __name__ == '__main__':
    run()
