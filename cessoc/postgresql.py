"""
The postgresql provides standard postgresql functionality for cessoc services on openshift.
"""

import psycopg2 as pg  # psycopg2-binary


class OpenshiftPostgresql:
    def __init__(self, database, user, password, host, port):
        self.connection = pg.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )

    def query(self, query):
        """
        Sends a query to the postgresql database and returns the results.
        :param query: The query to be sent to the database
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        self.connection.commit()
        cursor.close()
        return results
