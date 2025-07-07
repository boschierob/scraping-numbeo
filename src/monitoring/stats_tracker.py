"""
Statistics tracking for scraping operations
"""
import logging
import time
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class StatsTracker:
    """Tracks scraping statistics and generates reports"""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.stats = {
            'total_cities': 0,
            'cities_processed': 0,
            'cities_successful': 0,
            'cities_failed': 0,
            'total_categories': 0,
            'categories_successful': 0,
            'categories_failed': 0,
            'total_tables': 0,
            'tables_successful': 0,
            'tables_failed': 0,
            'total_files_created': 0,
            'total_requests': 0,
            'blocked_requests': 0,
            'errors': []
        }
        self.city_stats = {}
        self.category_stats = {}
        
    def start_scraping(self):
        """Start timing the scraping operation"""
        self.start_time = datetime.now()
        logger.info("Scraping session started")
    
    def end_scraping(self):
        """End timing the scraping operation"""
        self.end_time = datetime.now()
        logger.info("Scraping session ended")
    
    def get_duration(self) -> float:
        """Get total duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def record_city_start(self, city_name: str, country_name: str):
        """Record start of processing a city"""
        city_key = f"{country_name}_{city_name}"
        self.city_stats[city_key] = {
            'start_time': datetime.now().isoformat(),
            'categories_processed': 0,
            'categories_successful': 0,
            'categories_failed': 0,
            'tables_found': 0,
            'tables_successful': 0,
            'files_created': 0,
            'errors': []
        }
        self.stats['cities_processed'] += 1
        logger.debug(f"Started processing city: {city_key}")
    
    def record_city_end(self, city_name: str, country_name: str, success: bool):
        """Record end of processing a city"""
        city_key = f"{country_name}_{city_name}"
        if city_key in self.city_stats:
            self.city_stats[city_key]['end_time'] = datetime.now().isoformat()
            if success:
                self.stats['cities_successful'] += 1
            else:
                self.stats['cities_failed'] += 1
            logger.debug(f"Finished processing city: {city_key} (success: {success})")
    
    def record_category_result(self, city_name: str, country_name: str, 
                             category: str, success: bool, tables_found: int = 0, 
                             tables_successful: int = 0, files_created: int = 0):
        """Record result of processing a category"""
        city_key = f"{country_name}_{city_name}"
        
        # Update city stats
        if city_key in self.city_stats:
            self.city_stats[city_key]['categories_processed'] += 1
            if success:
                self.city_stats[city_key]['categories_successful'] += 1
            else:
                self.city_stats[city_key]['categories_failed'] += 1
            self.city_stats[city_key]['tables_found'] += tables_found
            self.city_stats[city_key]['tables_successful'] += tables_successful
            self.city_stats[city_key]['files_created'] += files_created
        
        # Update global stats
        self.stats['categories_successful' if success else 'categories_failed'] += 1
        self.stats['total_tables'] += tables_found
        self.stats['tables_successful'] += tables_successful
        self.stats['total_files_created'] += files_created
        
        # Update category stats
        if category not in self.category_stats:
            self.category_stats[category] = {
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'total_tables': 0,
                'successful_tables': 0
            }
        
        self.category_stats[category]['processed'] += 1
        if success:
            self.category_stats[category]['successful'] += 1
        else:
            self.category_stats[category]['failed'] += 1
        self.category_stats[category]['total_tables'] += tables_found
        self.category_stats[category]['successful_tables'] += tables_successful
    
    def record_error(self, error_type: str, message: str, city: str = None, category: str = None):
        """Record an error"""
        error_info = {
            'type': error_type,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'city': city,
            'category': category
        }
        self.stats['errors'].append(error_info)
        
        # Add to city stats if applicable
        if city:
            city_key = city.replace('_', ' ')  # Convert back from key format
            for key in self.city_stats:
                if city_key in key:
                    self.city_stats[key]['errors'].append(error_info)
                    break
        
        logger.error(f"Error recorded: {error_type} - {message}")
    
    def record_request(self, blocked: bool = False):
        """Record a request made"""
        self.stats['total_requests'] += 1
        if blocked:
            self.stats['blocked_requests'] += 1
    
    def get_success_rate(self) -> Dict[str, float]:
        """Calculate success rates"""
        rates = {}
        
        if self.stats['cities_processed'] > 0:
            rates['cities'] = (self.stats['cities_successful'] / self.stats['cities_processed']) * 100
        
        if self.stats['categories_successful'] + self.stats['categories_failed'] > 0:
            total_categories = self.stats['categories_successful'] + self.stats['categories_failed']
            rates['categories'] = (self.stats['categories_successful'] / total_categories) * 100
        
        if self.stats['total_tables'] > 0:
            rates['tables'] = (self.stats['tables_successful'] / self.stats['total_tables']) * 100
        
        return rates
    
    def generate_report(self, output_folder: Path) -> Path:
        """Generate a comprehensive scraping report"""
        report_file = output_folder / "scraping_report.json"
        try:
            output_folder.mkdir(parents=True, exist_ok=True)  # S'assure que le dossier existe
            report_data = {
                'session_info': {
                    'start_time': self.start_time.isoformat() if self.start_time else None,
                    'end_time': self.end_time.isoformat() if self.end_time else None,
                    'duration_seconds': self.get_duration(),
                    'duration_formatted': self._format_duration(self.get_duration())
                },
                'summary_stats': self.stats,
                'success_rates': self.get_success_rate(),
                'city_stats': self.city_stats,
                'category_stats': self.category_stats,
                'errors': self.stats['errors']
            }
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            
            # Also create a human-readable text report
            text_report_file = output_folder / "scraping_report.txt"
            self._generate_text_report(text_report_file, report_data)
            
            logger.info(f"Scraping report generated: {report_file}")
            return report_file
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            # Retourne quand même le chemin attendu pour permettre l'upload
            return report_file
    
    def _generate_text_report(self, file_path: Path, report_data: Dict):
        """Generate human-readable text report"""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("NUMBEO SCRAPING REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            # Session info
            session = report_data['session_info']
            f.write(f"Session Duration: {session['duration_formatted']}\n")
            f.write(f"Start Time: {session['start_time']}\n")
            f.write(f"End Time: {session['end_time']}\n\n")
            
            # Summary stats
            stats = report_data['summary_stats']
            f.write("SUMMARY STATISTICS\n")
            f.write("-" * 20 + "\n")
            f.write(f"Cities Processed: {stats['cities_processed']}\n")
            f.write(f"Cities Successful: {stats['cities_successful']}\n")
            f.write(f"Cities Failed: {stats['cities_failed']}\n")
            f.write(f"Categories Successful: {stats['categories_successful']}\n")
            f.write(f"Categories Failed: {stats['categories_failed']}\n")
            f.write(f"Tables Found: {stats['total_tables']}\n")
            f.write(f"Tables Successful: {stats['tables_successful']}\n")
            f.write(f"Files Created: {stats['total_files_created']}\n")
            f.write(f"Total Requests: {stats['total_requests']}\n")
            f.write(f"Blocked Requests: {stats['blocked_requests']}\n\n")
            
            # Success rates
            rates = report_data['success_rates']
            f.write("SUCCESS RATES\n")
            f.write("-" * 15 + "\n")
            for metric, rate in rates.items():
                f.write(f"{metric.title()}: {rate:.1f}%\n")
            f.write("\n")
            
            # Errors
            if stats['errors']:
                f.write("ERRORS\n")
                f.write("-" * 7 + "\n")
                for error in stats['errors']:
                    f.write(f"[{error['timestamp']}] {error['type']}: {error['message']}\n")
                    if error.get('city'):
                        f.write(f"  City: {error['city']}\n")
                    if error.get('category'):
                        f.write(f"  Category: {error['category']}\n")
                    f.write("\n")
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format"""
        if seconds < 60:
            return f"{seconds:.1f} seconds"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f} minutes"
        else:
            hours = seconds / 3600
            return f"{hours:.1f} hours"
    
    def is_scraping_successful(self, min_success_rate: float = 50.0) -> bool:
        """Determine if scraping was successful based on success rate"""
        rates = self.get_success_rate()
        if not rates:
            return False
        
        # Check if any major metric meets the minimum success rate
        for metric, rate in rates.items():
            if rate >= min_success_rate:
                return True
        
        return False 