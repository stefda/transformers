import os
import psycopg2.extras

from geojson import Feature, loads, dumps
from lib.abstract_transformer import AbstractTransformer
from lib.utils import record


class BathymetryTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
        SELECT gid, elev, ST_AsGeoJSON(geom) AS geom FROM deep_water_areas_subtracted
        """)

        rows = cur.fetchall()
        cur.close()

        return rows

    def transform(self, values):
        features = []
        for value in values:
            geometry = loads(value['geom'])
            features.append(Feature(geometry=geometry, properties={
                'gid': value['gid'],
                'elev': value['elev']
            }))

        return features

    def save(self, values):
        filename = os.path.join(self.path, 'bathymetry.json')
        fp = open(filename, 'w')

        for value in values:
            fp.write(record(dumps(value)))

        fp.close()
