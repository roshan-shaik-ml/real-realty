import os
import uuid
import logging
from dotenv import load_dotenv
from utils.logger import setup_logger
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv()

# Setup logger
logger = setup_logger()

default_dsn = os.getenv("postgresql_dsn")


class BrokerRepository:
    def __init__(self, dsn: str = default_dsn):
        """Initialize database connection with a DSN (Data Source Name)."""
        self.dsn = dsn
        logger.info("BrokerRepository initialized")

    def bulk_create(self, broker_names: List[str]) -> List[Dict[str, Any]]:
        """Create multiple brokers at once."""
        query = """
            INSERT INTO broker (id, name)
            VALUES %s
            ON CONFLICT (name) DO UPDATE 
            SET name = EXCLUDED.name
            RETURNING id, name;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Generate tuples of (uuid, name) for each unique name
                    unique_names = list(set(broker_names))  # Remove duplicates
                    values = [(str(uuid.uuid4()), name) for name in unique_names]
                    execute_values(cur, query, values)
                    conn.commit()
                    logger.info(f"brokers have been successfully created")
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error bulk creating brokers: {str(e)}")
            raise

    def bulk_update(self, broker_updates: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Update multiple brokers at once.
        broker_updates should be a list of dicts with 'id' and 'name' keys
        """
        query = """
            UPDATE broker AS b
            SET name = v.name
            FROM (VALUES %s) AS v(id, name)
            WHERE b.id = v.id::uuid
            RETURNING b.id, b.name;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Convert list of dicts to list of tuples (id, name)
                    values = [
                        (update["id"], update["name"]) for update in broker_updates
                    ]
                    execute_values(cur, query, values)
                    conn.commit()
                    logger.info(f"brokers have been successfully updated")
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error bulk updating brokers: {str(e)}")
            raise

    def bulk_get_or_create(self, broker_names: List[str]) -> List[Dict[str, Any]]:
        """Get or create multiple brokers at once."""
        query = """
            WITH input_brokers (name) AS (
                VALUES %s
            ),
            inserted_brokers AS (
                INSERT INTO broker (id, name)
                SELECT uuid_generate_v4(), ib.name
                FROM input_brokers ib
                LEFT JOIN broker b ON b.name = ib.name
                WHERE b.id IS NULL
                RETURNING id, name
            )
            SELECT id, name
            FROM inserted_brokers
            UNION ALL
            SELECT b.id, b.name
            FROM input_brokers ib
            JOIN broker b ON b.name = ib.name;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Convert list of names to list of single-item tuples
                    values = [(name,) for name in broker_names]
                    execute_values(cur, query, values)
                    conn.commit()
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error in bulk get_or_create for brokers: {str(e)}")
            raise

    def bulk_delete(self, broker_ids: List[str]) -> List[str]:
        """Delete multiple brokers at once."""
        query = """
            DELETE FROM broker
            WHERE id = ANY(%s)
            RETURNING id;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (broker_ids,))
                    conn.commit()
                    return [row["id"] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error bulk deleting brokers: {str(e)}")
            raise

    def create(self, broker_name: str) -> Optional[Dict[str, Any]]:
        """Create a new broker with an auto-generated UUID."""
        query = """
            INSERT INTO broker (id, name)
            VALUES (%s, %s)
            RETURNING id, name;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    broker_id = str(uuid.uuid4())  # Generate UUID
                    cur.execute(query, (broker_id, broker_name))
                    conn.commit()
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error creating broker: {str(e)}")
            raise

    def get_by_id(self, broker_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a broker by UUID."""
        query = """
            SELECT id, name
            FROM broker
            WHERE id = %s;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (broker_id,))
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error getting broker by id {broker_id}: {str(e)}")
            raise

    def get_by_name(self, broker_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve a broker by name."""
        query = """
            SELECT id, name
            FROM broker
            WHERE name = %s;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (broker_name,))
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error getting broker by name {broker_name}: {str(e)}")
            raise

    def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve all brokers with pagination."""
        query = """
            SELECT id, name
            FROM broker
            ORDER BY name
            LIMIT %s OFFSET %s;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (limit, offset))
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting all brokers: {str(e)}")
            raise

    def update(self, broker_id: str, new_name: str) -> Optional[Dict[str, Any]]:
        """Update a broker's name."""
        query = """
            UPDATE broker
            SET name = %s
            WHERE id = %s
            RETURNING id, name;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (new_name, broker_id))
                    conn.commit()
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error updating broker {broker_id}: {str(e)}")
            raise

    def delete(self, broker_id: str) -> bool:
        """Delete a broker by UUID."""
        query = """
            DELETE FROM broker
            WHERE id = %s
            RETURNING id;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (broker_id,))
                    conn.commit()
                    return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Error deleting broker {broker_id}: {str(e)}")
            raise

    def get_or_create(self, broker_name: str) -> Dict[str, Any]:
        """Get a broker by name or create if it doesn't exist."""
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # First try to get existing broker
                    get_query = """
                        SELECT id, name
                        FROM broker
                        WHERE name = %s;
                    """
                    cur.execute(get_query, (broker_name,))
                    broker = cur.fetchone()

                    if broker:
                        return broker

                    # If not found, create new broker
                    create_query = """
                        INSERT INTO broker (id, name)
                        VALUES (%s, %s)
                        RETURNING id, name;
                    """
                    broker_id = str(uuid.uuid4())
                    cur.execute(create_query, (broker_id, broker_name))
                    conn.commit()
                    return cur.fetchone()
        except Exception as e:
            logger.error(
                f"Error in get_or_create for broker name {broker_name}: {str(e)}"
            )
            raise

    def search_by_name(
        self, name_pattern: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search brokers by name pattern."""
        query = """
            SELECT id, name
            FROM broker
            WHERE name ILIKE %s
            ORDER BY name
            LIMIT %s;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (f"%{name_pattern}%", limit))
                    return cur.fetchall()
        except Exception as e:
            logger.error(
                f"Error searching brokers by name pattern {name_pattern}: {str(e)}"
            )
            raise
