import logging
import json
import unittest
import sys

from titanic_pipeline import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.testing.util import assert_that, equal_to

def main(out = sys.stderr, verbosity = 2):
    loader = unittest.TestLoader()
  
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(out, verbosity = verbosity).run(suite)


class ConvertToTitanicRecordTest(unittest.TestCase):

    def test_convert_to_csv(self):

        with TestPipeline() as p:

            LINES = ['0,1,male,46.0,1,0,61.175,S,E']
            EXPECTED_OUTPUT = [TitanicRecord(0,1,'male',46.0,1,0,61.175,'S','E')]

            input_lines = p | beam.Create(LINES)

            output = input_lines | beam.ParDo(ConvertCsvToTitanicRecord())

            assert_that(output, equal_to(EXPECTED_OUTPUT))

class AgeFareMultPclassTest(unittest.TestCase):

    def test_mult_units(self):

        with TestPipeline() as p:

            RECORDS = [TitanicRecord(0,3,'male',24.0,0,1,247.5208,'C','B'),
                       TitanicRecord(0,2,'male',54.0,0,1,77.2875,'S','D')]

            EXPECTED_RECORDS = [TitanicRecord(0,3,'male',72.0,0,1,742.5624,'C','B'),
                               TitanicRecord(0,2,'male',108.0,0,1,154.575,'S','D')]

            input_records = p | beam.Create(RECORDS)

            output = input_records | beam.ParDo(AgeFareMultPclass())
            
            assert_that(output, equal_to(EXPECTED_RECORDS))

class ComputeStatsTest(unittest.TestCase):
    
    def test_compute_statistics(self):

        with TestPipeline() as p:

            INPUT_RECORDS = [TitanicRecord(0,1,'female',50.0,0,0,28.7125,'C','C'),
                             TitanicRecord(1,1,'female',44.0,0,0,27.7208,'C','B'),
                             TitanicRecord(1,1,'female',31.0,1,0,113.275,'C','D'),
                             TitanicRecord(1,1,'female',58.0,0,1,153.4625,'S','C'),
                             TitanicRecord(1,1,'female',35.0,0,0,135.6333,'S','C')]

            EXPECTED_STATS = [json.dumps({'embarked': 'C', 'min_age': 31.0, 'max_age': 50.0, 'sum_age': 125.0 }),
                              json.dumps({'embarked': 'S', 'min_age': 35.0, 'max_age': 58.0, 'sum_age': 93.0 })]

            inputs = p | beam.Create(INPUT_RECORDS)

            output = inputs | ComputeStatistics()

            assert_that(output, equal_to(EXPECTED_STATS))

class TitanicStatsTransformTest(unittest.TestCase):

    def test_titanic_stats_transform(self):

        with TestPipeline() as p:

            INPUT_STRINGS = ["0,1,female,50.0,0,0,28.7125,C,C",
                             "1,1,female,44.0,0,0,27.7208,C,B",
                             "1,1,female,58.0,0,1,153.4625,S,C"]

            EXPECTED_STATS = [json.dumps({'embarked': 'C', 'min_age': 44.0, 'max_age': 50.0, 'sum_age': 94.0 }),
                              json.dumps({'embarked': 'S', 'min_age': 58.0, 'max_age': 58.0, 'sum_age': 58.0 })]

            inputs = p | beam.Create(INPUT_STRINGS)

            output = inputs | TitanicStats()

            assert_that(output, equal_to(EXPECTED_STATS))

        
    
    
if __name__ == '__main__':
    with open('testing.txt', 'w') as f:
        main(f)
