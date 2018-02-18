import os
import psycopg2.extras

from geojson import loads, dumps, Feature, Point, MultiPolygon
from lib.abstract_transformer import AbstractTransformer
from lib.polylabel import polylabel
from lib.utils import record

TABLE_SQL = """
CREATE TABLE beach_labels (
    gid SERIAL NOT NULL,
    osm_id character varying,
    name character varying,
    name_en character varying,
    geom geometry(POINT, 4326)
);
CREATE INDEX beach_labels_idx ON beach_labels USING gist(geom);
"""


class BeachTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_hstore(cur)
        cur.execute("""
        SELECT other_tags, ST_AsGeoJSON(wkb_geometry) AS geom
        FROM multipolygons
        WHERE "natural" = 'beach'
        """)

        rows = cur.fetchall()
        cur.close()

        return rows

    def transform(self, values):
        features = []
        for value in values:
            multipolygon = loads(value['geom'])
            features.append(Feature(geometry=multipolygon, properties={
                'surface': value['other_tags'].get('surface') if value['other_tags'] else None
            }))

        return features

    def save(self, features):
        filename = os.path.join(self.path, 'beach.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()


class BeachToLabelTransformer(AbstractTransformer):
    def setup(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
        DROP TABLE IF EXISTS beach_labels
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
        WHERE "natural" = 'beach'
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
            INSERT INTO beach_labels (osm_id, name, name_en, geom)
            VALUES (%s, %s, %s, ST_Transform(ST_GeomFromGeoJSON(%s), 4326))
            """, [
                value['osm_id'] or value['osm_way_id'],
                value['name'],
                value['other_tags'].get('name:en') if value['other_tags'] else None,
                dumps(value['geom'])])

        self.conn.commit()
        cur.close()


class BeachLabelTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
        SELECT name, name_en, ST_AsGeoJSON(geom) AS geom FROM beach_labels
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
        filename = os.path.join(self.path, 'beach_label.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()
