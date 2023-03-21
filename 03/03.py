
import apache_beam as beam
import uuid

# Definicao de pipeline
p1 = beam.Pipeline()

# LER ARQUIVO E EXCLUIR CABECALHO

pCollection = (
  p1

  # STEP 1
  | "Importar Dados" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines=1)

  # STEP 2
  | "Separar por Virgulas" >> beam.Map(lambda record: record.split(","))

  # STEP 3
  | "Mostrar resultados" >> beam.Map(print)

  # STEP 4
  | "Grava resultados" >> beam.io.WriteToText(str(uuid.uuid4())+"_voos.txt")
)

p1.run()