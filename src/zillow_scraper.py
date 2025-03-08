import requests
import time
from typing import List, Dict, Any, Optional
import logging
import os
import json
from utils.parser import Parser
from db.repositories.house_repo import HouseRepository
from db.repositories.broker_repo import BrokerRepository
from db.repositories.address_repo import AddressRepository
from db.repositories.images_repo import ImagesRepository
from utils.logger import setup_logger

# Setup logger
logger = setup_logger()

house_repo = HouseRepository()
broker_repo = BrokerRepository()
address_repo = AddressRepository()
image_repo = ImagesRepository()

parser = Parser()

class ZillowScraper:
    
    URL = "https://www.zillow.com/async-create-search-page-state"
    headers = {
            "accept": "*/*",
            "content-type": "application/json",
            "referer": "https://www.zillow.com/homes/12447_rid/?category=RECENT_SEARCH",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            # Add any required cookies here
        }
    current_dir = os.path.dirname(os.path.abspath(__file__))
    payload_path = os.path.join(current_dir, "utils", "payloads", "california.json")
    
    def __init__(self):
        self.broker_data = []
        logger.info("ZillowScraper initialized")
        
    def get_query_body(self, page: int) -> Dict[str, Any]:
        with open(self.payload_path, "r") as f:
            payload = json.load(f)
        
        # Set the page number you wanna extract
        payload["searchQueryState"]["pagination"]["currentPage"] = page
        return payload

    def fetch_page(self, page: int) -> List[Dict[str, Any]]:
        try:
            response = requests.put(
                url=self.URL,
                json=self.get_query_body(page),
                headers=self.headers
            )
            data = response.json()

            # Get the houses data from the response
            houses = data.get("cat1", {}).get("searchResults", {}).get("listResults", [])
            logger.info(f"Fetched {len(houses)} houses from page {page}")
            
            # Save the response to a file
            with open(f"response_{page}.json", "w") as f:
                json.dump(data, f)
            return houses
        except Exception as e:
            logger.error(f"Error fetching page {page}: {str(e)}", exc_info=True)
            return []
    
    def process_broker_data(self, houses_data: List[Dict[str, Any]]) -> List[tuple]:
        for house in houses_data:
            brokerName = house.get("brokerName")
            if brokerName:
                self.broker_data.append(brokerName)
        
        self.insert_broker_data()
    
    def insert_broker_data(self):
        try:
            if self.broker_data:
                broker_repo.bulk_create(self.broker_data)
                logger.info(f"Successfully processed {len(self.broker_data)} brokers")
            else:
                logger.warning("No broker data to insert")
        except Exception as e:
            logger.error(f"Error inserting broker data: {str(e)}", exc_info=True)

    def process_houses_data(self, houses_data: List[Dict[str, Any]]) -> None:
        for house in houses_data:
            try:
                # Parse the house data
                house_data = parser.parse_house_data(house)
                if not house_data:
                    logger.warning("Failed to parse house data")
                    continue

                # Get the broker name from the house data
                broker_name = house.get("brokerName")
                if broker_name:
                    broker = broker_repo.get_by_name(broker_name)
                    house_data.append(broker.get("id"))
                else:
                    house_data.append(None)
               
                # Create house one by one
                try:
                    created_house = house_repo.create(house_data)
                    if not created_house:
                        logger.warning(f"House already exists: {house_data[0]}")
                        continue
                except Exception as e:
                    logger.error(f"Error creating house {house_data[0]}: {str(e)}", exc_info=True)
                    continue

                # Process address data and continue if None
                address_data = parser.parse_address_data(house, house_data[0])
                if not address_data:
                    logger.warning(f"No address data for house {house_data[0]}")
                    continue
                    
                # Create address one by one
                try:
                    address_repo.create(address_data)
                except Exception as e:
                    logger.error(f"Error creating address for house {house_data[0]}: {str(e)}", exc_info=True)
                
                # Process image data and continue if None
                image_data = parser.parse_image_data(house, house_data[0])  # Use house UUID (id)
                if not image_data:
                    logger.warning(f"No image data for house {house_data[0]}")
                    continue    

                try:
                    image_repo.bulk_create(image_data)
                    logger.info(f"Successfully processed {len(image_data)} images for house {house_data[0]}")
                except Exception as e:
                    logger.error(f"Error creating images for house {house_data[0]}: {str(e)}", exc_info=True)
                    continue
                
            except Exception as e:
                logger.error(f"Error processing house data: {str(e)}", exc_info=True)
                continue

    def scrape(self, max_pages: int = 20):
        logger.info(f"Starting scraping process for {max_pages} pages")
        
        for page in range(1, max_pages + 1):
            logger.info(f"Scraping page {page}")
            
            houses_data = self.fetch_page(page)
            if not houses_data:
                logger.warning(f"No data found on page {page}, stopping...")
                break
            
            self.process_broker_data(houses_data)
            self.process_houses_data(houses_data)
            
            # insert data
            parser.reset_data(self.broker_data)
            
            # Rate limiting
            time.sleep(2)  # Being nice to Zillow's servers
            
def main():
    scraper = ZillowScraper()
    try:
        logger.info("Starting Zillow scraper")
        scraper.scrape()
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}", exc_info=True)
    finally:
        logger.info("Zillow scraper closed")

if __name__ == "__main__":
    main()