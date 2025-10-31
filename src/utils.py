"""
Utility functions for Google Jobs Scraper
Data processing and analysis utilities
"""

import pandas as pd
import json
import logging
from typing import List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class DataProcessor:
    """Process and analyze scraped job data"""
    
    @staticmethod
    def save_to_csv(jobs_data: List[Dict], filename: str):
        """Save job data to CSV with proper formatting"""
        if not jobs_data:
            logger.warning("No data to save")
            return
        
        df = pd.DataFrame(jobs_data)
        
        # Ensure data directory exists
        Path('data').mkdir(exist_ok=True)
        
        filepath = f"data/{filename}"
        df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"Data saved to {filepath}")
    
    @staticmethod
    def analyze_job_market(jobs_data: List[Dict]) -> Dict[str, Any]:
        """Generate market analysis from job data"""
        if not jobs_data:
            return {}
        
        df = pd.DataFrame(jobs_data)
        
        analysis = {
            'total_jobs': len(df),
            'cities_covered': df['city'].nunique(),
            'companies_represented': df['company'].nunique(),
            'success_rate': (df['scraping_status'] == 'success').mean() * 100,
            'top_companies': df['company'].value_counts().head(10).to_dict(),
            'city_distribution': df['city'].value_counts().to_dict(),
            'data_quality': {
                'has_title': df['title'].notna().mean() * 100,
                'has_company': df['company'].notna().mean() * 100,
                'has_location': df['location'].notna().mean() * 100,
                'has_description': (df['description'] != '').mean() * 100,
            }
        }
        
        return analysis
    
    @staticmethod
    def load_city_config(config_file: str = "config/cities.json") -> List[Dict]:
        """Load city configuration from JSON file"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_file} not found, using default cities")
            return [
                {"name": "Casablanca", "country_code": "ma", "language": "fr"},
                {"name": "Rabat", "country_code": "ma", "language": "fr"},
                {"name": "Marrakech", "country_code": "ma", "language": "fr"}
            ]

class ReportGenerator:
    """Generate professional reports from scraped data"""
    
    @staticmethod
    def generate_market_report(analysis: Dict, output_file: str = "market_analysis.md"):
        """Generate a professional market analysis report"""
        report = [
            "# Job Market Analysis Report",
            f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Executive Summary",
            f"- **Total Jobs Analyzed**: {analysis.get('total_jobs', 0):,}",
            f"- **Cities Covered**: {analysis.get('cities_covered', 0)}",
            f"- **Companies Represented**: {analysis.get('companies_represented', 0)}",
            f"- **Data Quality Score**: {analysis.get('success_rate', 0):.1f}%",
            "",
            "## Detailed Analysis",
            "### Geographic Distribution",
        ]
        
        # Add city distribution
        for city, count in analysis.get('city_distribution', {}).items():
            report.append(f"- {city}: {count:,} jobs")
        
        report.extend([
            "",
            "### Top Employers",
        ])
        
        # Add top companies
        for company, count in analysis.get('top_companies', {}).items():
            report.append(f"- {company}: {count:,} listings")
        
        report.extend([
            "",
            "### Data Quality Metrics",
        ])
        
        # Add data quality metrics
        quality = analysis.get('data_quality', {})
        for metric, score in quality.items():
            report.append(f"- {metric.replace('_', ' ').title()}: {score:.1f}%")
        
        # Save report
        with open(f"data/{output_file}", 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        logger.info(f"Market report saved to data/{output_file}")