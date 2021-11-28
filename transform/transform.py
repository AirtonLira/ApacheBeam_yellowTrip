from zipfile import is_zipfile
from zipfile import ZipFile
from google.cloud import storage

import apache_beam as beam
import io


class extrair(beam.DoFn):
    def process(self, zipfilename_with_path):
        zipfilename_with_path = zipfilename_with_path.replace("gs://yellow_trip_estudos/","")
        print("Extrair arquivo zip: {}".format(zipfilename_with_path))
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket("yellow_trip_estudos")

        blob = bucket.blob(zipfilename_with_path)

        zipbytes = io.BytesIO(blob.download_as_string())

        if is_zipfile(zipbytes):
            with ZipFile(zipbytes, 'r') as myzip:
                for contentfilename in myzip.namelist():
                    contentfile = myzip.read(contentfilename)
                    blob = bucket.blob(zipfilename_with_path.replace(".zip",""))
                    blob.upload_from_string(contentfile)

        print("Pasta ou arquivo para exclusão {}".format(zipfilename_with_path))
        storage_client = storage.Client()
        bucket = storage_client.get_bucket("yellow_trip_estudos")

        blob = bucket.blob(zipfilename_with_path)
        blob.delete()