import json
import typing
import logging
import apache_beam as beam

class TitanicRecord(typing.NamedTuple):
    survived: int
    pclass: int
    sex: str
    age: float
    sibsp: int
    parch: int
    fare: float
    embarked: str
    deck: str
    

beam.coders.registry.register_coder(TitanicRecord, beam.coders.RowCoder)

class ConvertCsvToTitanicRecord(beam.DoFn):

    def process(self, line):
        fields = 'survived,pclass,sex,age,sibsp,parch,fare,embarked,deck'.split(',')
        values = line.split(',')
        row = dict(zip(fields,values))
        for num_field in ('age','fare'):
            row[num_field] = float(row[num_field])
        for int_field in ('survived','pclass','sibsp','parch'):
            row[int_field] = int(row[int_field])
        yield TitanicRecord(**row)

        
class AgeFareMultPclass(beam.DoFn):

    def process(self, row):
        row_dict = row._asdict()
        for field in ('age', 'fare'):
            row_dict[field] = row_dict[field] * int(row_dict['pclass'])
        yield TitanicRecord(**row_dict)
##       

##
class ConvertToJson(beam.DoFn):

    def process(self, row):
        line = json.dumps(row._asdict())
        yield line

class ComputeStatistics(beam.PTransform):

    def expand(self, pcoll):
    
        results = (
            pcoll | 'ComputeStatistics' >> beam.GroupBy('embarked')
                                                .aggregate_field('age', min, 'min_age')
                                                .aggregate_field('age', max, 'max_age')
                                                .aggregate_field('age', sum, 'sum_age')
                | 'ToJson' >> beam.ParDo(ConvertToJson())
        )
        
        return results

class TitanicStats(beam.PTransform):

    def expand(self, pcoll):

        results = (
            pcoll | "ParseCSV" >> beam.ParDo(ConvertCsvToTitanicRecord())

                  | "ConvertToF" >> beam.ParDo(AgeFareMultPclass())
                  
                  | "ComputeStats" >> ComputeStatistics()
                  
        )

        return results

def run():

    p = beam.Pipeline()

    (p | 'ReadCSV' >> beam.io.ReadFromText('./titanic.csv')
       | 'ComputeStatistics' >> WeatherStats()
       | 'WriteJson' >> beam.io.WriteToText('./titanic', '.json')
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()

if __name__ == '__main__':
  run()
