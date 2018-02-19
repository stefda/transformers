import os
import psycopg2

from lib.abstract_transformer import runBatchTransformer, runTransformer
from transformers.highway import HighwayTransformer
from transformers.bathymetry import BathymetryTransformer
from transformers.water import WaterTransformer
from transformers.bay import BayToLabelTransformer, BayLabelTransformer
from transformers.beach import BeachTransformer, BeachToLabelTransformer, BeachLabelTransformer
from transformers.marina import MarinaTransformer
from transformers.anchorage import AnchorageTransformer
from transformers.light import LightTransformer

import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--out', required=True, help='Path where to output files')
parser.add_argument('--host', default='localhost')
parser.add_argument('--port', default='5432')
parser.add_argument('--dbname', required=True)
parser.add_argument('-u', '--user', required=True)
parser.add_argument('-p', '--password', required=True)

args = parser.parse_args()

if __name__ == '__main__':
    transformers = [
        AnchorageTransformer,
        MarinaTransformer,
        LightTransformer,
        # BeachTransformer,
        # BeachToLabelTransformer,
        # BeachLabelTransformer,
        # BayToLabelTransformer,
        # BayLabelTransformer,
        # BathymetryTransformer,
        # WaterTransformer
    ]

    batchTransformers = [
        # HighwayTransformer
    ]

    conn = psycopg2.connect('host=%s port=%s dbname=%s user=%s password=%s'
                            % (args.host, args.port, args.dbname, args.user, args.password))

    for transformer in transformers:
        instance = transformer(conn, args.out)
        print 'running', transformer.__name__
        runTransformer(instance)

    for batchTransformer in batchTransformers:
        instance = batchTransformer(conn, args.out)
        print 'running', batchTransformer.__name__
        runBatchTransformer(instance, True)

    conn.close()
