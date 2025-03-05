import requests
import psycopg2
from psycopg2.extras import execute_values
import time
from typing import List, Dict, Any, Optional
import logging
import random

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
            "accept": "*/*",
            "content-type": "application/json",
            "referer": "https://www.zillow.com/homes/12447_rid/?category=RECENT_SEARCH",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            # Add any required cookies here
        }
        
    def get_query_body(self, page: int) -> Dict[str, Any]:
        return  {
                    "searchQueryState": {
                    "pagination": {
                        "currentPage": page
                    },
                    "isMapVisible": True,
                    "mapBounds": {
                        "north": 42.009517,
                        "south": 32.528832,
                        "east": -114.131253,
                        "west": -124.482045
                    },
                    "filterState": {
                        "sortSelection": {
                            "value": "globalrelevanceex"
                        }
                    },
                    "isListVisible": True,
                    "regionSelection": [
                        {
                            "regionId": 9,
                            "regionType": 2
                        }
                    ],
                    "usersSearchTerm": "California",
                },
                "wants": {
                    "cat1": ["mapResults", "listResults" ],       
                    "cat2": ["total"]
                    },
                "requestId": 2,
                "isDebugRequest": False,
            }

    def fetch_page(self, page: int) -> List[Dict[str, Any]]:
        try:
            URL = "https://www.zillow.com/async-create-search-page-state"
            response = requests.put(
                url=URL,
                json=self.get_query_body(page),
                headers=self.headers
            )
            data = response.json()
            houses = data.get("cat1", {}).get("searchResults", {}).get("mapResults", [])
            logger.info(f"Fetched {len(houses)} houses from page {page}")
            if houses:
                logger.info(f"Sample house data: {houses[0]}")
            return houses
        except Exception as e:
            logger.error(f"Error fetching page {page}: {str(e)}")
            return []

    def generate_random_coordinate(self) -> tuple[float, float]:
        """Generate random coordinates within reasonable global bounds."""
        # Global reasonable bounds
        lat = random.uniform(-90, 90)  # Valid latitude range
        lon = random.uniform(-180, 180)  # Valid longitude range
        return lat, lon

    def generate_coordinate_hash(self, latitude: Optional[float], longitude: Optional[float]) -> str:
        """Generate a deterministic hash based on latitude and longitude coordinates."""
        # If either coordinate is missing, generate random ones
        if latitude is None or longitude is None:
            lat, lon = self.generate_random_coordinate()
        else:
            lat, lon = latitude, longitude

        # Round coordinates to 6 decimal places for consistency
        lat = round(float(lat), 6)
        lon = round(float(lon), 6)
        
        # Create a string combining both coordinates
        coord_str = f"{lat},{lon}"
        # Use built-in hash function and convert to hex for a consistent string representation
        return hex(hash(coord_str))[2:]  # Remove '0x' prefix

    def generate_random_zpid(self) -> str:
        """Generate a random zpid using timestamp and random number."""
        timestamp = int(time.time())
        random_num = random.randint(1000, 9999)
        return f"R{timestamp}{random_num}"

    def process_house_data(self, houses_data: List[Dict[str, Any]]) -> List[tuple]:
        houses = []
        for house in houses_data:
            # Get zpid from the house data or generate a random one
            zpid = house.get("id")
            if not zpid:
                zpid = self.generate_random_zpid()
                logger.info(f"Generated random zpid: {zpid}")

            # Get area value from hdpData.homeInfo.livingArea
            area = house.get("hdpData", {}).get("homeInfo", {}).get("livingArea")
            if not area:
                logger.warning("No area value found")

            # Clean price value
            price = house.get("price")
            if isinstance(price, str):
                try:
                    price = float(''.join(filter(str.isdigit, price)))
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse price value: {price}")
                    continue

            # Get home_type from hdpData.homeInfo.homeType
            home_type = house.get("hdpData", {}).get("homeInfo", {}).get("homeType")
            if not home_type:
                logger.warning("No home type found")
                continue  # Skip houses without a home type since it's required

            house_data = (
                zpid,
                price,
                house.get("statusType"),
                area,  # Store area as string
                home_type,
                house.get("latLong", {}).get("latitude"),
                house.get("latLong", {}).get("longitude"),
                house.get("listingAgent", {}).get("name"),
                house.get("detailUrl"),
                house.get("brokerName"),
                None  # seller_id is NULL for now
            )
            houses.append(house_data)
            if len(houses) % 100 == 0:  # Log every 100 houses to avoid spam
                logger.info(f"Processed houses count: {len(houses)}")
        return houses

    def process_address_data(self, houses_data: List[Dict[str, Any]]) -> List[tuple]:
        addresses = []
        for house in houses_data:
            house_id = house.get("zpid")
            if not house_id:
                continue

            addresses.append((
                house_id,
                house.get("addressStreet"),
                house.get("addressCity"),
                house.get("addressState"),
                house.get("addressZipcode")
            ))
        return addresses

    def process_image_data(self, houses_data: List[Dict[str, Any]]) -> List[tuple]:
        images = []
        for house in houses_data:
            house_id = house.get("zpid")
            if not house_id:
                continue

            for img in house.get("carouselPhotos", []):
                if img.get("url"):
                    images.append((house_id, img.get("url")))
        return images

    def process_houses(self, houses_data: List[Dict[str, Any]]) -> tuple[List[tuple], List[tuple], List[tuple]]:
        houses = self.process_house_data(houses_data)
        # addresses = self.process_address_data(houses_data)
        # images = self.process_image_data(houses_data)
        return houses, None, None

    def insert_data(self, houses: List[tuple], addresses: Optional[List[tuple]], images: Optional[List[tuple]]):
        try:
            # Insert houses
            if houses:
                # Get current count before insert
                self.cursor.execute("SELECT COUNT(*) FROM houses;")
                before_count = self.cursor.fetchone()[0]
                logger.info(f"Houses in database before insert: {before_count}")

                house_insert_query = """
                    INSERT INTO houses (zpid, price, status, area, home_type, 
                                    latitude, longitude, listing_agent, detail_url, broker_name, seller_id)
                    VALUES %s
                    ON CONFLICT (zpid) DO NOTHING;
                """
                execute_values(self.cursor, house_insert_query, houses)
                
                # Get the number of rows affected
                self.cursor.execute("SELECT COUNT(*) FROM houses;")
                after_count = self.cursor.fetchone()[0]
                
                # Get the number of rows updated
                self.cursor.execute("SELECT COUNT(*) FROM houses WHERE zpid LIKE 'R%';")
                random_zpid_count = self.cursor.fetchone()[0]
                
                logger.info(f"Houses in database after insert: {after_count}")
                logger.info(f"New houses added: {after_count - before_count}")
                logger.info(f"Houses with random zpids: {random_zpid_count}")

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
            time.sleep(2)  # Being nice to Zillow's servers
            
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
