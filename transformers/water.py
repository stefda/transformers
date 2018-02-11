import os
import psycopg2.extras
from geojson import Feature, dumps, loads

from lib.abstract_transformer import AbstractTransformer
from lib.utils import record


class WaterTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
        SELECT ST_AsGeoJSON(geom) AS geom FROM water_polygon_cyclades
        """)

        row = cur.fetchone()
        cur.close()
        return row

    def transform(self, value):
        geometry = loads(value['geom'])
        return Feature(geometry=geometry)

    def save(self, feature):
        filename = os.path.join(self.path, 'water.json')
        fp = open(filename, 'w')
        fp.write(record(dumps(feature)))
        fp.close()
