#!/usr/bin/env python3
"""
Google Jobs Scraper - Main Execution Script
Professional job market data collection and analysis
"""

import logging
import time
from src.scraper import GoogleJobsScraper
from src.utils import DataProcessor, ReportGenerator

def main():
    """Main execution function"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Google Jobs Scraper")
    
    # Initialize scraper
    scraper = GoogleJobsScraper(headless=True)
    
    if not scraper.init_driver():
        logger.error("Failed to initialize scraper. Exiting.")
        return
    
    all_jobs_data = []
    
    try:
        # Load city configuration
        cities = DataProcessor.load_city_config()
        logger.info(f"Loaded configuration for {len(cities)} cities")
        
        # Scrape jobs for each city
        for i, city_config in enumerate(cities, 1):
            city_name = city_config['name']
            logger.info(f"Processing city {i}/{len(cities)}: {city_name}")
            
            city_jobs = scraper.scrape_city(
                city=city_name,
                country_code=city_config['country_code'],
                language=city_config['language']
            )
            
            all_jobs_data.extend(city_jobs)
            logger.info(f"Completed {city_name}: {len(city_jobs)} jobs collected")
            
            # Rate limiting between cities
            if i < len(cities):
                logger.info("Waiting before next city...")
                time.sleep(5)
        
        # Process and save results
        if all_jobs_data:
            # Save raw data
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            DataProcessor.save_to_csv(all_jobs_data, f"jobs_data_{timestamp}.csv")
            
            # Generate analysis
            analysis = DataProcessor.analyze_job_market(all_jobs_data)
            ReportGenerator.generate_market_report(analysis)
            
            logger.info(f"Scraping completed: {len(all_jobs_data)} total jobs collected")
            logger.info("Data saved to 'data/' directory")
        else:
            logger.warning("No job data collected")
    
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
    
    finally:
        scraper.close()
        logger.info("Scraper shutdown complete")

if __name__ == "__main__":
    main()