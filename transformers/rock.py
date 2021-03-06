import os
import psycopg2.extras

from geojson import loads, dumps, Feature, Point
from lib.abstract_transformer import AbstractTransformer
from lib.utils import record


class RockTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_hstore(cur)

        cur.execute("""
        SELECT osm_id, ST_AsGeoJSON(wkb_geometry) AS geom
        FROM points
        WHERE "other_tags" @> '"seamark:type"=>"rock"'
        """)

        rows = cur.fetchall()
        cur.close()

        return rows

    def transform(self, values):
        features = []
        for value in values:
            features.append(Feature(geometry=Point(loads(value['geom'])), properties={
                'osm_ref': value['osm_id'] + 'n'
            }))

        return features

    def save(self, features):
        filename = os.path.join(self.path, 'rock.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()
