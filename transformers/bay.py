import os
import psycopg2.extras

from geojson import loads, dumps, Feature, Point
from lib.abstract_transformer import AbstractTransformer
from lib.polylabel import polylabel
from lib.utils import record

TABLE_SQL = """
CREATE TABLE bay_labels (
    gid SERIAL NOT NULL,
    osm_id character varying,
    name character varying,
    name_en character varying,
    geom geometry(POINT, 4326)
);
CREATE INDEX bay_labels_idx ON bay_labels USING gist(geom);
"""


class BayToLabelTransformer(AbstractTransformer):
    def setup(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
        DROP TABLE IF EXISTS bay_labels
        """)
        self.conn.commit()

        cur.execute(TABLE_SQL)
        self.conn.commit()

        cur.close()

    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_hstore(cur)
        cur.execute("""
        SELECT osm_id, osm_way_id, name, other_tags, ST_AsGeoJSON(ST_Transform(wkb_geometry, 3857)) AS geom
        FROM multipolygons
        WHERE "natural" = 'bay'
        """)

        rows = cur.fetchall()
        cur.close()

        return rows

    def transform(self, values):
        for value in values:
            polygon = loads(value['geom'])
            coordinates = polylabel(polygon['coordinates'][0])
            value['geom'] = Point(coordinates, crs={'type': 'name', 'properties': {'name': 'EPSG:3857'}})

        return values

    def save(self, values):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        for value in values:
            cur.execute("""
            INSERT INTO bay_labels (osm_id, name, name_en, geom)
            VALUES (%s, %s, %s, ST_Transform(ST_GeomFromGeoJSON(%s), 4326))
            """, [value['osm_id'] or value['osm_way_id'], value['name'], value['other_tags'].get('name:en'), dumps(value['geom'])])

        self.conn.commit()
        cur.close()


class BayLabelTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
        SELECT name, name_en, ST_AsGeoJSON(geom) AS geom FROM bay_labels
        """)

        row = cur.fetchall()
        cur.close()
        return row

    def transform(self, values):
        features = []
        for value in values:
            geometry = loads(value['geom'])
            features.append(Feature(geometry=geometry, properties={
                'name': value['name'],
                'name_en': value['name_en']
            }))

        return features

    def save(self, features):
        filename = os.path.join(self.path, 'bay_label.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()
