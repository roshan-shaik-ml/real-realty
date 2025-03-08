import os
import logging
from decimal import Decimal
from dotenv import load_dotenv

from utils.logger import setup_logger
from typing import List, Dict, Any, Optional, Union

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv()

# Setup logger
logger = setup_logger()

default_dsn = os.getenv("postgresql_dsn")


class AddressRepository:
    def __init__(self, dsn: str = default_dsn):
        """Initialize database connection with a DSN (Data Source Name)."""
        self.dsn = dsn
        logger.info("AddressRepository initialized")

    def create(self, address_data: List[Any]) -> Union[Optional[Dict[str, Any]], None]:
        """Create a new address record.
        Args:
            address_data: List containing [id, street, city, state, zipcode, latitude, longitude, house_id]
        """
        query = """
            INSERT INTO address (id, street, city, state, zipcode, latitude, longitude, house_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, street, city, state, zipcode, latitude, longitude, house_id;
        """
        try:

            logger.info(f"Address data: {address_data}")
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Format the data
                    id, street, city, state, zipcode, lat, lon, house_id = address_data
                    formatted_data = (
                        str(id),
                        street,
                        city,
                        state,
                        zipcode,
                        Decimal(lat) if lat is not None else None,
                        Decimal(lon) if lon is not None else None,
                        str(house_id) if house_id is not None else None,
                    )
                    cur.execute(query, formatted_data)
                    conn.commit()
                    cur.execute("SELECT * FROM address WHERE id = %s", (id,))
                    logger.info(f"Address created: {cur.fetchone()}")
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error creating address: {str(e)}")
            raise

    def bulk_create(self, addresses: List[List[Any]]) -> None:
        """Bulk insert address, ignoring conflicts on (street, city, state, zipcode)."""
        query = """
            INSERT INTO address (id, street, city, state, zipcode, latitude, longitude, house_id)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # First verify that all house_ids exist
                    house_ids = [str(row[7]) for row in addresses if row[7] is not None]
                    if house_ids:
                        check_query = """
                            SELECT id FROM house WHERE id = ANY(%s)
                        """
                        cur.execute(check_query, (house_ids,))
                        existing_house_ids = {row["id"] for row in cur.fetchall()}

                        # Filter out addresses with non-existent house_ids
                        formatted_values = []
                        for row in addresses:
                            id, street, city, state, zipcode, lat, lon, house_id = row
                            if house_id is None or str(house_id) in existing_house_ids:
                                formatted_values.append(
                                    (
                                        str(id),
                                        street,
                                        city,
                                        state,
                                        zipcode,
                                        Decimal(lat) if lat is not None else None,
                                        Decimal(lon) if lon is not None else None,
                                        str(house_id) if house_id is not None else None,
                                    )
                                )
                    else:
                        formatted_values = [
                            (
                                str(row[0]),
                                row[1],
                                row[2],
                                row[3],
                                row[4],
                                Decimal(row[5]) if row[5] is not None else None,
                                Decimal(row[6]) if row[6] is not None else None,
                                str(row[7]) if row[7] is not None else None,
                            )
                            for row in addresses
                        ]

                    if not formatted_values:
                        logger.warning("No valid addresses to insert.")
                        return

                    execute_values(cur, query, formatted_values)
                    conn.commit()
                    logger.info(
                        f"{len(formatted_values)} addresses successfully created"
                    )

        except Exception as e:
            logger.error(f"Error bulk creating addresses: {str(e)}")
            raise

    def get_by_id(self, address_id: int) -> Optional[Dict[str, Any]]:
        """Get address by ID."""
        query = """
            SELECT id, street, city, state, zipcode, latitude, longitude
            FROM address
            WHERE id = %s;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (address_id,))
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error getting address {address_id}: {str(e)}")
            raise

    def update(
        self, address_id: int, address_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update address record."""
        query = """
            UPDATE address
            SET 
                street = %(street)s,
                city = %(city)s,
                state = %(state)s,
                zipcode = %(zipcode)s,
                latitude = %(latitude)s,
                longitude = %(longitude)s
            WHERE id = %(id)s
            RETURNING id, street, city, state, zipcode, latitude, longitude;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    address_data["id"] = address_id
                    # Convert lat/long to Decimal if present
                    if "latitude" in address_data:
                        address_data["latitude"] = Decimal(
                            str(address_data["latitude"])
                        )
                    if "longitude" in address_data:
                        address_data["longitude"] = Decimal(
                            str(address_data["longitude"])
                        )
                    cur.execute(query, address_data)
                    conn.commit()
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error updating address {address_id}: {str(e)}")
            raise

    def delete(self, address_id: int) -> bool:
        """Delete address by ID."""
        query = """
            DELETE FROM address
            WHERE id = %s
            RETURNING id;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (address_id,))
                    conn.commit()
                    return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Error deleting address {address_id}: {str(e)}")
            raise

    def search_by_zipcode(self, zipcode: str) -> List[Dict[str, Any]]:
        """Search address by zipcode."""
        query = """
            SELECT id, street, city, state, zipcode, latitude, longitude
            FROM address
            WHERE zipcode = %s;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (zipcode,))
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error searching address by zipcode {zipcode}: {str(e)}")
            raise
