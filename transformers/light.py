import os
import psycopg2.extras

from geojson import loads, dumps, Feature, Point
from lib.abstract_transformer import AbstractTransformer
from lib.utils import record


class LightTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_hstore(cur)

        cur.execute("""
        SELECT other_tags, ST_AsGeoJSON(wkb_geometry) AS geom
        FROM points
        WHERE "other_tags" @> '"seamark:type"=>"light_minor"' OR "other_tags" @> '"seamark:type"=>"light_major"'
        """)

        rows = cur.fetchall()
        cur.close()

        return rows

    def transform(self, values):
        features = []
        for value in values:
            type = value['other_tags'].get('seamark:type')
            features.append(Feature(geometry=Point(loads(value['geom'])), properties={
                'type': 'minor' if type == 'light_minor' else 'major'
            }))

        return features

    def save(self, features):
        filename = os.path.join(self.path, 'light.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()
