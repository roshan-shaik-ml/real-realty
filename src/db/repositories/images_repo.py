import os
import uuid
from dotenv import load_dotenv
from utils.logger import setup_logger
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv()

# Setup logger
logger = setup_logger()

default_dsn = os.getenv("postgresql_dsn")


class ImagesRepository:
    def __init__(self, dsn: str = default_dsn):
        """Initialize database connection with a DSN (Data Source Name)."""
        self.dsn = dsn
        logger.info("ImagesRepository initialized")

    def create(self, house_id: str, url: str) -> Optional[Dict[str, Any]]:
        """Create a new image record with auto-generated UUID."""
        query = """
            INSERT INTO house_images (id, house_id, url)
            VALUES (%s, %s, %s)
            RETURNING id, house_id, url;
        """
        logger.info(f"Creating image for house {house_id} with url {url}")
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    image_id = str(uuid.uuid4())
                    cur.execute(query, (image_id, house_id, url))
                    conn.commit()
                    return cur.fetchone()
        except Exception as e:
            logger.error(f"Error creating image: {str(e)}")
            raise

    def bulk_create(self, images: List[List[Any]]) -> List[Dict[str, Any]]:
        """Create multiple image records at once.
        images should be a list of (house_id, imageUrl) tuples
        """
        query = """
            INSERT INTO images (id, house_id, image_url)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
            RETURNING id, house_id, image_url;
        """
        logger.info(f"Inserting {len(images)} images")
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Generate UUIDs and create values tuples
                    values = [
                        (str(id), str(house_id), url) for id, house_id, url in images
                    ]
                    execute_values(cur, query, values)
                    conn.commit()
                    logger.info(f"Inserted {len(values)} images")
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error bulk creating images: {str(e)}")
            raise

    def get_by_house_id(self, house_id: str) -> List[Dict[str, Any]]:
        """Get all images for a specific house."""
        query = """
            SELECT id, house_id, url
            FROM house_images
            WHERE house_id = %s
            ORDER BY id;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (house_id,))
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting images for house {house_id}: {str(e)}")
            raise

    def delete_by_house_id(self, house_id: str) -> List[str]:
        """Delete all images for a specific house."""
        query = """
            DELETE FROM house_images
            WHERE house_id = %s
            RETURNING id;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (house_id,))
                    conn.commit()
                    return [row["id"] for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error deleting images for house {house_id}: {str(e)}")
            raise

    def delete_by_id(self, image_id: str) -> bool:
        """Delete a specific image by its ID."""
        query = """
            DELETE FROM house_images
            WHERE id = %s
            RETURNING id;
        """
        try:
            with psycopg2.connect(self.dsn) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (image_id,))
                    conn.commit()
                    return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Error deleting image {image_id}: {str(e)}")
            raise
