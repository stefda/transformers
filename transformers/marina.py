import os
import psycopg2.extras

from geojson import loads, dumps, Feature, Point
from lib.abstract_transformer import AbstractTransformer
from lib.polylabel import polylabel
from lib.utils import record
from lib.geo import proj_point


class MarinaTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_hstore(cur)

        cur.execute("""
        SELECT osm_way_id, ST_AsGeoJSON(ST_Transform(wkb_geometry, 3857)) AS geom
        FROM multipolygons
        WHERE "other_tags" @> '"seamark:type"=>"harbour"' AND "other_tags" @> '"seamark:harbour:category"=>"marina"'
        """)
        rows = cur.fetchall()
        cur.close()

        return rows

    def transform(self, values):
        features = []
        for value in values:
            polygon = loads(value['geom'])
            coordinates = polylabel(polygon['coordinates'][0])
            point = proj_point(Point(coordinates), 'EPSG:3857', 'EPSG:4326')
            features.append(Feature(geometry=point, properties={
                'osm_ref': value['osm_way_id'] + 'w'
            }))

        return features

    def save(self, features):
        filename = os.path.join(self.path, 'marina.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()
