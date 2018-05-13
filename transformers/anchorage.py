import os
import psycopg2.extras

from geojson import loads, dumps, Feature, Point
from lib.abstract_transformer import AbstractTransformer
from lib.polylabel import polylabel
from lib.utils import record
from lib.geo import proj_point


class AnchorageTransformer(AbstractTransformer):
    def load(self):
        values = {}
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_hstore(cur)

        cur.execute("""
        SELECT osm_id, ST_AsGeoJSON(ST_Transform(ST_MakePolygon(wkb_geometry), 3857)) AS geom
        FROM lines
        WHERE "other_tags" @> '"seamark:type"=>"anchorage"'
        """)
        values['polygons'] = cur.fetchall()

        cur.execute("""
        SELECT osm_id, ST_AsGeoJSON(wkb_geometry) AS geom
        FROM points
        WHERE "other_tags" @> '"seamark:type"=>"anchorage"'
        """)
        values['points'] = cur.fetchall()

        cur.close()

        return values

    def transform(self, values):
        features = []
        for value in values['polygons']:
            polygon = loads(value['geom'])
            coordinates = polylabel(polygon['coordinates'])
            point = proj_point(Point(coordinates), 'EPSG:3857', 'EPSG:4326')
            features.append(Feature(geometry=point, properties={
                'osm_ref': value['osm_id'] + 'n'
            }))

        for value in values['points']:
            point = loads(value['geom'])
            features.append(Feature(geometry=point, properties={
                'osm_ref': value['osm_id'] + 'n'
            }))

        return features

    def save(self, features):
        filename = os.path.join(self.path, 'anchorage.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()
