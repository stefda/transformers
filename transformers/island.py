import os
import psycopg2.extras

from geojson import loads, dumps, Feature, Point, MultiPolygon
from lib.abstract_transformer import AbstractTransformer
from lib.polylabel import polylabel
from lib.utils import record

TABLE_SQL = """
CREATE TABLE island_labels (
    gid SERIAL NOT NULL,
    osm_id character varying,
    osm_way_id character varying,
    name character varying,
    name_en character varying,
    area float NOT NULL,
    geom geometry(GEOMETRY, 4326)
);
CREATE INDEX island_labels_idx ON island_labels USING gist(geom);
"""


class IslandToLabelTransformer(AbstractTransformer):
    def setup(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
        DROP TABLE IF EXISTS island_labels
        """)
        self.conn.commit()

        cur.execute(TABLE_SQL)
        self.conn.commit()

        cur.close()

    def load(self):
        values = {}
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_hstore(cur)

        cur.execute("""
            SELECT
                osm_id, osm_way_id, name, other_tags,
                ST_AsGeoJSON(ST_Transform(wkb_geometry, 3857)) AS geom,
                round(ST_Area(ST_Transform(wkb_geometry, 3857)) / 1000000) AS area
            FROM multipolygons
            WHERE "place" = 'island'
        """)
        values['polygons'] = cur.fetchall()

        cur.execute("""
                SELECT osm_id, name, other_tags, ST_AsGeoJSON(ST_Transform(wkb_geometry, 3857)) AS geom
                FROM points
                WHERE "place" = 'island'
                """)
        values['points'] = cur.fetchall()

        cur.close()
        return values

    def transform(self, values):
        for value in values['polygons']:
            polygon = loads(value['geom'])
            coordinates = polylabel(polygon['coordinates'][0])
            value['geom'] = Point(coordinates, crs={'type': 'name', 'properties': {'name': 'EPSG:3857'}})

        for value in values['points']:
            point = loads(value['geom'])
            value['geom'] = Point(point['coordinates'], crs={'type': 'name', 'properties': {'name': 'EPSG:3857'}})

        return values

    def save(self, values):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        for value in values['polygons']:
            cur.execute("""
            INSERT INTO island_labels (osm_id, osm_way_id, name, name_en, geom, area)
            VALUES (%s, %s, %s, %s, ST_Transform(ST_GeomFromGeoJSON(%s), 4326), %s)
            """, [
                value['osm_id'],
                value['osm_way_id'],
                value['name'],
                value['other_tags'].get('name:en') if value['other_tags'] else None,
                dumps(value['geom']),
                value['area']])

        for value in values['points']:
            cur.execute("""
            INSERT INTO island_labels (osm_id, osm_way_id, name, name_en, geom, area)
            VALUES (%s, %s, %s, %s, ST_Transform(ST_GeomFromGeoJSON(%s), 4326), %s)
            """, [
                value['osm_id'],
                None,
                value['name'],
                value['other_tags'].get('name:en') if value['other_tags'] else None,
                dumps(value['geom']),
                0])

        self.conn.commit()
        cur.close()


class IslandLabelTransformer(AbstractTransformer):
    def load(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT osm_id, osm_way_id, name, name_en, ST_AsGeoJSON(geom) AS geom, area FROM island_labels
        """)

        row = cur.fetchall()
        cur.close()
        return row

    def transform(self, values):
        features = []
        for value in values:
            geometry = loads(value['geom'])
            features.append(Feature(geometry=geometry, properties={
                'osm_ref': value['osm_id'] + 'n' if value['osm_id'] else value['osm_way_id'] + 'w',
                'name': value['name'],
                'name_en': value['name_en'],
                'area': value['area']
            }))

        return features

    def save(self, features):
        filename = os.path.join(self.path, 'island_label.json')
        fp = open(filename, 'w')
        for feature in features:
            fp.write(record(dumps(feature)))

        fp.close()
