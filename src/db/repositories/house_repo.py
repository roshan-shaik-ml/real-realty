import os
import uuid
import logging

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from dotenv import load_dotenv
from utils.logger import setup_logger
from typing import List, Dict, Any, Optional, Union

load_dotenv()

# Setup logger
logger = setup_logger()

default_dsn = os.getenv("postgresql_dsn")

# Register UUID adapter
psycopg2.extras.register_uuid()


class HouseRepository:
    def __init__(self, dsn: str = default_dsn):
        """Initialize database connection with a DSN (Data Source Name)."""
        self.dsn = dsn
        logger.info("HouseRepository initialized")

    def create(self, house_data: List[Any]) -> Union[Optional[Dict[str, Any]], None]:
        """Create a new house record.
        Args:
            house_data: List containing [id, zpid, price, status, beds, baths, area, type, url, broker_id]
        """
        query = """
            INSERT INTO house (id, zpid, price, status, beds, baths, area, type, url, broker_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (zpid) DO NOTHING
            RETURNING id, zpid, price, status, beds, baths, area, type, url, broker_id;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Format the data
                    id, zpid, price, status, beds, baths, area, type, url, broker_id = (
                        house_data
                    )
                    formatted_data = (
                        id,  # UUID will be handled by the adapter
                        zpid,
                        price,
                        status,
                        beds,
                        baths,
                        area,
                        type,
                        url if url is not None else None,
                        broker_id,  # UUID will be handled by the adapter
                    )

                    cur.execute(query, formatted_data)
                    conn.commit()

                    # Get the house from the database
                    cur.execute("SELECT * FROM house WHERE id = %s", (id,))
                    house = cur.fetchone()
                    if house:
                        logger.info(f"Successfully created house with ID: {id}")
                    else:
                        logger.warning(
                            f"House with ID {id} was not created (possibly due to zpid conflict)"
                        )
                    return house
        except Exception as e:
            logger.error(f"Error creating house: {str(e)}", exc_info=True)
            raise

    def bulk_create(self, house_data: List[List[Any]]) -> List[Dict[str, Any]]:
        """Bulk insert houses while handling UUIDs and formatting values correctly."""
        query = """
            INSERT INTO house (id, zpid, price, status, beds, baths, area, type, url, broker_id)
            VALUES %s
            ON CONFLICT (zpid) DO NOTHING
            RETURNING id, zpid, price, status, beds, baths, area, type, url, broker_id;
        """

        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Convert UUIDs to strings and handle None values
                    formatted_values = [
                        (
                            (
                                str(row[0]) if isinstance(row[0], uuid.UUID) else row[0]
                            ),  # Ensure UUID as str
                            row[1],  # zpid (required)
                            row[2],  # price (default 0)
                            row[3],  # status (required)
                            row[4],  # beds (default 0)
                            row[5],  # baths (default 0)
                            row[6],  # area (nullable)
                            row[7],  # type (required)
                            row[8] if row[8] is not None else None,  # url (nullable)
                            (
                                str(row[9])
                                if row[9] is not None and isinstance(row[9], uuid.UUID)
                                else None
                            ),  # broker_id (nullable)
                        )
                        for row in house_data
                    ]

                    # Execute batch insert
                    execute_values(cur, query, formatted_values)

                    conn.commit()
                    inserted_rows = cur.fetchall()
                    logger.info(
                        f"Successfully bulk inserted {len(inserted_rows)} houses"
                    )
                    return inserted_rows

        except Exception as e:
            logger.error(f"Error bulk inserting houses: {str(e)}", exc_info=True)
            raise
