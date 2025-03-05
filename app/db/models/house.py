import psycopg2
from psycopg2.extras import RealDictCursor

class HouseDB:
    def __init__(self, dsn: str):
        """Initialize database connection with a DSN (Data Source Name)."""
        self.dsn = dsn

    def create_house(self, house_data: dict):
        """Insert a new house record into the database."""
        query = """
        INSERT INTO houses (zpid, price, unformatted_price, status, lot_area, home_type,
                            latitude, longitude, listing_agent, detail_url, broker_name, seller_id)
        VALUES (%(id)s, %(zpid)s, %(price)s, %(unformatted_price)s, %(status)s, %(lot_area)s,
                %(home_type)s, %(latitude)s, %(longitude)s, %(listing_agent)s,
                %(detail_url)s, %(broker_name)s, %(seller_id)s)
        RETURNING id;
        """
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, house_data)
                conn.commit()
                return cur.fetchone()  # Returns the new house ID

    def get_house(self, house_id: int):
        """Retrieve a house by ID."""
        query = "SELECT * FROM houses WHERE id = %s;"
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (house_id,))
                return cur.fetchone()

    def delete_house(self, house_id: int):
        """Delete a house by ID."""
        query = "DELETE FROM houses WHERE id = %s RETURNING id;"
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (house_id,))
                conn.commit()
                return cur.rowcount > 0  # Returns True if a row was deleted

    def update_house(self, house_id: int, house_data: dict):
        """Update a house record."""
        query = """
        UPDATE houses
        SET price = %(price)s, unformatted_price = %(unformatted_price)s, status = %(status)s,
            lot_area = %(lot_area)s, home_type = %(home_type)s, latitude = %(latitude)s,
            longitude = %(longitude)s, listing_agent = %(listing_agent)s, detail_url = %(detail_url)s,
            broker_name = %(broker_name)s, seller_id = %(seller_id)s
        WHERE id = %(id)s RETURNING id;
        """
        house_data["id"] = house_id
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, house_data)
                conn.commit()
                return cur.rowcount > 0  # Returns True if an update occurred
