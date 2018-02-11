import os
import psycopg2.extras

from geojson import loads, dumps, Feature
from lib.abstract_transformer import AbstractBatchTransformer
from lib.utils import record, rm_if_exists

TABLE_SQL = """
CREATE TABLE highways (
    gid SERIAL NOT NULL,
    type varchar(20),
    geom geometry(POLYGON, 4326)
);
CREATE INDEX highways_geom_idx ON highways USING gist(geom);
"""


class HighwayTransformer(AbstractBatchTransformer):
    def getBatchSize(self):
        return 100

    def getSize(self):
        cur = self.conn.cursor()
        cur.execute("""
        SELECT COUNT(*) FROM lines WHERE highway IS NOT NULL
        """)

        count = cur.fetchone()[0]
        cur.close()

        return count

    def setup(self):
        filename = os.path.join(self.path, 'highway.json')
        rm_if_exists(filename)

        cur = self.conn.cursor()
        cur.execute("""
        DROP TABLE IF EXISTS highways
        """)
        self.conn.commit()
        cur.close()

    def load(self, offset, batchSize):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
        SELECT osm_id, highway, ST_AsGeoJSON(wkb_geometry) AS geom
        FROM lines
        WHERE highway IS NOT NULL 
        LIMIT %s OFFSET %s
        """, [batchSize, offset])
        rows = cur.fetchall()
        cur.close()

        return rows

    def transform(self, values):
        features = []
        for value in values:
            geometry = loads(value['geom'])
            features.append(Feature(geometry=geometry, parameters={
                'type': value['highway']
            }))

        return features

    def save(self, values):
        filename = os.path.join(self.path, 'highway.json')
        fp = open(filename, 'a')
        for value in values:
            fp.write(record(dumps(value)))

        fp.close()
