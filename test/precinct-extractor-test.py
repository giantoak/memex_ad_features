import nose
import sys
import os

path = os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__)))

sys.path.append(os.path.join(path,
    'preprocess'))

from precinct import PrecinctExtractor



def accuracy_test():
    def evaluate(p, query, answer):
        assert p.extract_ucr(query) == answer
    
    p = PrecinctExtractor()
    
    # a few tricky examples selected and hand-labeled from the data
    tests = {
            'Wyoming County Sheriff Department': 'wyoming county',
            'Wirt County Sheriff Office': 'wirt county',
            'Rock': 'rock',
            'Vt. State Police-Willist': 'willist',
            'Brandon - Fairwater Police Dept': 'brandon - fairwater'
            }
    
    for q, a in tests.iteritems():
        yield evaluate, p, q, a
