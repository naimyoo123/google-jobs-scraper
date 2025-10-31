"""
Google Jobs Scraper
Professional web scraper for job market analysis
"""

import time
import pandas as pd 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class GoogleJobsScraper:
    """Professional scraper for Google Jobs listings with market analysis capabilities"""
    
    def __init__(self, headless: bool = True, timeout: int = 30):
        self.headless = headless
        self.timeout = timeout
        self.driver = None
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup professional logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
    
    def init_driver(self):
        """Initialize Chrome WebDriver with professional configurations"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless=new")
        
        # Professional browser configuration
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Language and location preferences
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_argument("--accept-lang=en-US")
        
        try:
            service = Service()
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("WebDriver initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            return False
    
    def scroll_to_load_all_jobs(self, max_scroll_attempts: int = 50) -> int:
        """
        Advanced scrolling mechanism to load all available jobs
        Uses multiple detection methods to ensure complete loading
        """
        logger.info("Starting advanced job loading sequence")
        
        current_job_count = 0
        scroll_attempts = 0
        no_new_jobs_count = 0
        max_no_new_jobs = 3
        
        # Wait for initial job elements
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".PUpOsf"))
            )
        except TimeoutException:
            logger.warning("No job elements found initially")
            return 0
        
        # Find scrollable container
        container = self._find_scroll_container()
        
        while (scroll_attempts < max_scroll_attempts and 
               no_new_jobs_count < max_no_new_jobs):
            
            job_elements = self.driver.find_elements(By.CSS_SELECTOR, ".PUpOsf")
            new_count = len(job_elements)
            
            logger.debug(f"Scroll attempt {scroll_attempts + 1}: {new_count} jobs found")
            
            if new_count == current_job_count:
                no_new_jobs_count += 1
                logger.debug(f"No new jobs detected ({no_new_jobs_count}/{max_no_new_jobs})")
            else:
                no_new_jobs_count = 0
                logger.info(f"Loaded {new_count - current_job_count} new jobs")
            
            current_job_count = new_count
            
            # Multiple scrolling strategies
            self._execute_scroll_strategies(container, job_elements)
            
            # Wait for content to load
            time.sleep(2)
            
            # Check for new content
            if not self._detect_new_content(container, current_job_count):
                logger.debug("No new content detected after scroll")
            
            scroll_attempts += 1
        
        final_count = len(self.driver.find_elements(By.CSS_SELECTOR, ".PUpOsf"))
        logger.info(f"Job loading complete: {final_count} total jobs found")
        return final_count
    
    def _find_scroll_container(self):
        """Find the appropriate scroll container using multiple selectors"""
        container_selectors = [
            'div[jsname="iTtkOe"]',
            'div[role="main"]',
            'div.mQ25we',
            'div#rcnt'
        ]
        
        for selector in container_selectors:
            try:
                container = self.driver.find_element(By.CSS_SELECTOR, selector)
                logger.debug(f"Using scroll container: {selector}")
                return container
            except NoSuchElementException:
                continue
        
        # Fallback to body
        return self.driver.find_element(By.TAG_NAME, 'body')
    
    def _execute_scroll_strategies(self, container, job_elements):
        """Execute multiple scrolling strategies for reliability"""
        strategies = [
            self._scroll_to_bottom(container),
            self._scroll_to_last_element(job_elements),
            self._scroll_js_smooth(container)
        ]
        
        for strategy in strategies:
            try:
                strategy()
                break
            except Exception as e:
                logger.debug(f"Scroll strategy failed: {e}")
                continue
    
    def _scroll_to_bottom(self, container):
        """Scroll container to bottom"""
        def execute():
            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", container)
        return execute
    
    def _scroll_to_last_element(self, job_elements):
        """Scroll to last job element"""
        def execute():
            if job_elements:
                last_job = job_elements[-1]
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({behavior: 'smooth', block: 'end'});", 
                    last_job
                )
        return execute
    
    def _scroll_js_smooth(self, container):
        """Smooth JavaScript scroll"""
        def execute():
            self.driver.execute_script("""
                arguments[0].scrollTo({
                    top: arguments[0].scrollHeight,
                    behavior: 'smooth'
                });
            """, container)
        return execute
    
    def _detect_new_content(self, container, current_count: int) -> bool:
        """Detect if new content has loaded after scrolling"""
        try:
            WebDriverWait(self.driver, 5).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, ".PUpOsf")) > current_count
            )
            return True
        except TimeoutException:
            return False
    
    def extract_job_data(self, job_element, index: int, city: str) -> Dict:
        """
        Extract comprehensive job data from listing
        Includes error handling and data validation
        """
        job_data = {
            'job_id': f"{city.lower()}_{index + 1}",
            'city': city,
            'timestamp': pd.Timestamp.now().isoformat(),
            'title': '',
            'company': '',
            'location': '',
            'application_link': '',
            'description': '',
            'scraping_status': 'success'
        }
        
        try:
            # Extract basic job information
            job_data.update(self._extract_basic_info(job_element))
            
            # Click and get detailed information
            self.driver.execute_script("arguments[0].click();", job_element)
            time.sleep(1)
            
            # Extract detailed information from job panel
            job_data.update(self._extract_detailed_info())
            
            logger.debug(f"Successfully extracted job {index + 1} in {city}")
            
        except Exception as e:
            logger.warning(f"Failed to extract job {index + 1} in {city}: {e}")
            job_data['scraping_status'] = f'error: {str(e)[:100]}'
        
        return job_data
    
    def _extract_basic_info(self, job_element) -> Dict:
        """Extract basic job information from list element"""
        info = {}
        try:
            info['title'] = job_element.text.strip()
        except Exception:
            info['title'] = 'Unknown'
        
        return info
    
    def _extract_detailed_info(self) -> Dict:
        """Extract detailed information from job details panel"""
        info = {}
        
        try:
            # Wait for details panel
            active_pane = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.BIB1wf[style*='display: block']"))
            )
            
            # Extract company and location
            info.update(self._extract_company_location(active_pane))
            
            # Extract application link
            info['application_link'] = self._extract_application_link(active_pane)
            
            # Extract and expand description
            info['description'] = self._extract_description(active_pane)
            
        except Exception as e:
            logger.debug(f"Could not extract detailed info: {e}")
        
        return info
    
    def _extract_company_location(self, active_pane) -> Dict:
        """Extract company and location information"""
        try:
            info_element = active_pane.find_element(By.CSS_SELECTOR, "div.waQ7qe")
            text = info_element.text.strip().replace('\n', ' | ')
            
            # Basic parsing (can be enhanced with NLP)
            parts = text.split('•')
            company = parts[0].strip() if len(parts) > 0 else 'Unknown'
            location = parts[1].strip() if len(parts) > 1 else 'Unknown'
            
            return {
                'company': company,
                'location': location
            }
        except Exception:
            return {'company': 'Unknown', 'location': 'Unknown'}
    
    def _extract_application_link(self, active_pane) -> str:
        """Extract application link using multiple strategies"""
        link_selectors = [
            ".//a[contains(@title, 'Apply')]",
            ".//a[contains(@title, 'Postuler')]",
            ".//a[contains(@href, 'http') and contains(@class, 'LgbsSe')]",
            ".//a[target='_blank']"
        ]
        
        for selector in link_selectors:
            try:
                links = active_pane.find_elements(By.XPATH, selector)
                for link in links:
                    href = link.get_attribute('href')
                    if href and 'http' in href and 'google.com' not in href:
                        return href
            except Exception:
                continue
        
        return 'Not found'
    
    def _extract_description(self, active_pane) -> str:
        """Extract and expand job description"""
        try:
            # Try to expand description if available
            self._expand_description(active_pane)
            
            # Extract description text
            desc_container = active_pane.find_element(By.CSS_SELECTOR, "div.NgUYpe")
            description = desc_container.text.strip()
            
            # Clean description
            description = description.replace("Description du poste", "")\
                                    .replace("Job Description", "")\
                                    .replace("...", "").strip()
            
            return " ".join(description.split())
            
        except Exception:
            return "Description not available"
    
    def _expand_description(self, active_pane):
        """Expand job description if truncated"""
        expand_selectors = [
            ".//div[@role='button' and .//*[contains(text(), 'Voir la description complète')]]",
            ".//div[@role='button' and .//*[contains(text(), 'See full description')]]",
            "div.nNzjpf-cS4Vcb-PvZLI-enNyge-KE6vqe-ma6Yeb"
        ]
        
        for selector in expand_selectors:
            try:
                if 'XPath' in selector:
                    expand_btn = active_pane.find_element(By.XPATH, selector)
                else:
                    expand_btn = active_pane.find_element(By.CSS_SELECTOR, selector)
                
                self.driver.execute_script("arguments[0].click();", expand_btn)
                time.sleep(1)
                logger.debug("Expanded job description")
                break
            except Exception:
                continue
    
    def scrape_city(self, city: str, country_code: str = "ma", language: str = "fr") -> List[Dict]:
        """
        Scrape all jobs for a specific city
        Returns comprehensive job data for market analysis
        """
        logger.info(f"Starting scrape for {city}")
        
        jobs_data = []
        
        try:
            # Generate and navigate to direct jobs URL
            url = self._generate_jobs_url(city, country_code, language)
            self.driver.get(url)
            
            # Load all available jobs
            total_jobs = self.scroll_to_load_all_jobs()
            
            if total_jobs == 0:
                logger.warning(f"No jobs found for {city}")
                return jobs_data
            
            logger.info(f"Scraping {total_jobs} jobs in {city}")
            
            # Extract data for each job
            job_elements = self.driver.find_elements(By.CSS_SELECTOR, ".PUpOsf")
            
            for i in range(min(total_jobs, len(job_elements))):
                try:
                    job_data = self.extract_job_data(job_elements[i], i, city)
                    jobs_data.append(job_data)
                    
                    # Progress logging
                    if (i + 1) % 10 == 0:
                        logger.info(f"Progress: {i + 1}/{total_jobs} jobs scraped in {city}")
                        
                except Exception as e:
                    logger.error(f"Failed to process job {i + 1} in {city}: {e}")
                    continue
            
            logger.info(f"Completed scraping {len(jobs_data)} jobs in {city}")
            
        except Exception as e:
            logger.error(f"Failed to scrape {city}: {e}")
        
        return jobs_data
    
    def _generate_jobs_url(self, city: str, country_code: str, language: str) -> str:
        """Generate direct Google Jobs URL for location"""
        city_encoded = city.lower().replace(' ', '+')
        return (f"https://www.google.com/search?q=jobs+in+{city_encoded}"
                f"&udm=8&gl={country_code}&hl={language}")
    
    def close(self):
        """Cleanup resources"""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver closed")