import requests
import psycopg2
from psycopg2.extras import execute_values
import time
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZillowScraper:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="entities",
            user="postgres",
            password="password",
            host="localhost",
            port="5434"
        )
        self.cursor = self.conn.cursor()
        
        self.headers = {
            "origin": "https://www.zillow.com",
            "referer": "https://www.zillow.com/homes/for_sale/",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sec-gpc": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/133.0.0.0 Safari/537.36"
        }
        
    def get_query_body(self, page: int) -> Dict[str, Any]:
        return {
            "searchQueryState": {
                "pagination": {"currentPage": page},
                "mapBounds": {
                    "west": -119.21510774414062,
                    "east": -117.60835725585937,
                    "south": 33.56513304568846,
                    "north": 34.47457151586186
                },
                "filterState": {
                    "sortSelection": {"value": "globalrelevanceex"}
                },
                "isListVisible": True
            },
            "wants": {
                "cat1": ["mapResults", "listResults"],
                "cat2": ["total"]
            }
        }

    def fetch_page(self, page: int) -> List[Dict[str, Any]]:
        try:
            response = requests.post(
                "https://www.zillow.com/graphql/",
                json=self.get_query_body(page),
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            return data.get("data", {}).get("cat1", {}).get("searchResults", {}).get("listResults", [])
        except Exception as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            return []

    def process_houses(self, houses_data: List[Dict[str, Any]]):
        houses = []
        addresses = []
        images = []

        for house in houses_data:
            house_id = house.get("zpid")
            if not house_id:
                continue

            # Prepare house data
            houses.append((
                house_id,
                house.get("price"),
                house.get("unformattedPrice"),
                house.get("statusType"),
                house.get("lotAreaString"),
                house.get("hdpData", {}).get("homeInfo", {}).get("homeType"),
                house.get("latLong", {}).get("latitude"),
                house.get("latLong", {}).get("longitude"),
                house.get("listingAgent", {}).get("name"),
                house.get("detailUrl"),
                house.get("brokerName")
            ))

            # Prepare address data
            addresses.append((
                house_id,
                house.get("addressStreet"),
                house.get("addressCity"),
                house.get("addressState"),
                house.get("addressZipcode")
            ))

            # Prepare image data
            for img in house.get("carouselPhotos", []):
                if img.get("url"):
                    images.append((house_id, img.get("url")))

        return houses, addresses, images

    def insert_data(self, houses: List[tuple], addresses: List[tuple], images: List[tuple]):
        try:
            # Insert houses
            if houses:
                house_insert_query = """
                    INSERT INTO houses (zpid, price, unformatted_price, status, lot_area, home_type, 
                                    latitude, longitude, listing_agent, detail_url, broker_name)
                    VALUES %s
                    ON CONFLICT (zpid) DO UPDATE SET
                        price = EXCLUDED.price,
                        unformatted_price = EXCLUDED.unformatted_price,
                        status = EXCLUDED.status,
                        lot_area = EXCLUDED.lot_area,
                        home_type = EXCLUDED.home_type,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        listing_agent = EXCLUDED.listing_agent,
                        detail_url = EXCLUDED.detail_url,
                        broker_name = EXCLUDED.broker_name;
                """
                execute_values(self.cursor, house_insert_query, houses)

            # Insert addresses
            if addresses:
                address_insert_query = """
                    INSERT INTO addresses (house_id, street, city, state, zipcode)
                    VALUES %s
                    ON CONFLICT (house_id) DO UPDATE SET
                        street = EXCLUDED.street,
                        city = EXCLUDED.city,
                        state = EXCLUDED.state,
                        zipcode = EXCLUDED.zipcode;
                """
                execute_values(self.cursor, address_insert_query, addresses)

            # Insert images
            if images:
                image_insert_query = """
                    INSERT INTO house_images (house_id, image_url)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                """
                execute_values(self.cursor, image_insert_query, images)

            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error inserting data: {str(e)}")
            raise

    def scrape(self, max_pages: int = 100):
        total_houses = 0
        
        for page in range(1, max_pages + 1):
            logger.info(f"Scraping page {page}")
            
            houses_data = self.fetch_page(page)
            if not houses_data:
                logger.warning(f"No data found on page {page}, stopping...")
                break

            houses, addresses, images = self.process_houses(houses_data)
            total_houses += len(houses)
            
            self.insert_data(houses, addresses, images)
            logger.info(f"Processed {len(houses)} houses from page {page}")
            
            # Rate limiting
            time.sleep(2)  # Be nice to Zillow's servers
            
        logger.info(f"Scraping completed. Total houses processed: {total_houses}")

    def close(self):
        self.cursor.close()
        self.conn.close()

def main():
    scraper = ZillowScraper()
    try:
        scraper.scrape()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
