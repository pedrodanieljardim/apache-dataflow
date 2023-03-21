import apache_beam as beam

# Definicao de pipeline
p1 = beam.Pipeline()
p2 = beam.Pipeline()

p1 | "Tupla instanciada" >> beam.Create([("Cassio", 32), ("Vics", 21)]) | "Print Tupla" >> beam.Map(print)

p1.run()

p2 | "Lista instanciada" >> beam.Create([1,2,3,4,5,6]) | "Print Lista" >> beam.Map(print)

p2.run()