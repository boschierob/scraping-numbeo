"""
File saving utilities for scraped data
"""
import pandas as pd
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import json
from ..config.settings import get_output_folder, CSV_EXTENSION, OUTPUT_DIR

logger = logging.getLogger(__name__)

class FileSaver:
    """Handles saving scraped data to CSV files"""
    
    def __init__(self, output_folder: Optional[Path] = None):
        self.output_folder = output_folder or get_output_folder()
        self.output_folder.mkdir(parents=True, exist_ok=True)
        self.saved_files = []
        
    def save_csv(self, data: pd.DataFrame, filename: str, category: str = "") -> Optional[Path]:
        """
        Save data to CSV file
        
        Args:
            data: DataFrame to save
            filename: Base filename
            category: Category name for organization
            
        Returns:
            Path to saved file or None if failed
        """
        try:
            if data.empty:
                logger.warning(f"No data to save for {filename}")
                return None
                
            # Create category subfolder if specified
            save_folder = self.output_folder
            if category:
                save_folder = self.output_folder / category
                save_folder.mkdir(exist_ok=True)
            
            # Generate filename
            safe_filename = self._sanitize_filename(filename)
            file_path = save_folder / f"{safe_filename}{CSV_EXTENSION}"
            
            # Save file
            data.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"Saved CSV: {file_path}")
            
            self.saved_files.append(file_path)
            return file_path
            
        except Exception as e:
            logger.error(f"Error saving CSV {filename}: {e}")
            return None
    
    def save_category_data(self, city_name: str, country_name: str, 
                          category: str, tables: List[pd.DataFrame]) -> List[Path]:
        """
        Save all tables for a specific category as separate CSVs, using the section/category title as suffix
        """
        saved_files = []
        if not tables:
            logger.warning(f"No tables to save for {category}")
            return saved_files
        
        # Compter les occurrences de chaque table_caption pour savoir si on doit suffixer
        table_caption_counts = {}
        for df in tables:
            if 'table_caption' in df.columns:
                cap = df['table_caption'].iloc[0] if not df.empty else None
                if cap:
                    table_caption_counts[cap] = table_caption_counts.get(cap, 0) + 1
        
        for idx, df in enumerate([df for df in tables if not df.empty]):
            # Déterminer le suffixe à partir de la colonne 'table_caption', sinon 'section', sinon 'Category'
            suffix = None
            if 'table_caption' in df.columns:
                cap_val = df['table_caption'].dropna().unique()
                if len(cap_val) > 0 and cap_val[0]:
                    suffix = cap_val[0]
            if not suffix and 'section' in df.columns:
                section_val = df['section'].dropna().unique()
                if len(section_val) > 0 and section_val[0]:
                    suffix = section_val[0]
            if not suffix and 'Category' in df.columns:
                cat_val = df['Category'].dropna().unique()
                if len(cat_val) > 0 and cat_val[0]:
                    suffix = cat_val[0]
            if not suffix:
                suffix = str(idx+1)
            # Si plusieurs DataFrames ont le même table_caption, suffixer par _list ou _table
            if 'table_caption' in df.columns and table_caption_counts.get(suffix, 0) > 1:
                if 'data_type' in df.columns:
                    dtype = df['data_type'].iloc[0]
                    suffix = f"{suffix}_{dtype}"
            # Nettoyer le suffixe pour le nom de fichier
            safe_suffix = self._sanitize_filename(str(suffix).replace(' ', '_'))
            filename = f"{country_name}_{city_name}_{category}_{safe_suffix}"
            file_path = self.save_csv(df, filename, category)
            if file_path:
                saved_files.append(file_path)
        return saved_files
    
    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename for safe file system usage
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        # Replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove leading/trailing spaces and dots
        filename = filename.strip(' .')
        
        # Limit length
        if len(filename) > 200:
            filename = filename[:200]
        
        return filename
    
    def get_saved_files(self) -> List[Path]:
        """Get list of all saved files"""
        return self.saved_files.copy()
    
    def get_output_folder(self) -> Path:
        """Get the output folder path"""
        return self.output_folder
    
    def cleanup_empty_files(self):
        """Remove any empty files that were created"""
        for file_path in self.saved_files:
            if file_path.exists() and file_path.stat().st_size == 0:
                try:
                    file_path.unlink()
                    logger.debug(f"Removed empty file: {file_path}")
                except Exception as e:
                    logger.warning(f"Could not remove empty file {file_path}: {e}")

def make_city_output_folder(city, region, country, base_output_dir=None):
    """
    Génère le chemin du dossier d'output pour une ville, au format :
    output/ville(-region)-pays-timestamp
    """
    base_output_dir = base_output_dir or OUTPUT_DIR
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    city = (city or '').strip() or 'UnknownCity'
    country = (country or '').strip() or 'UnknownCountry'
    region = (region or '').strip()
    parts = [city]
    if region:
        parts.append(region)
    parts.append(country)
    folder_name = "-".join([str(p).replace(" ", "-") for p in parts if p])
    folder_name = f"{folder_name}-{timestamp}"
    output_folder = Path(base_output_dir) / folder_name
    output_folder.mkdir(parents=True, exist_ok=True)
    # --- Ajout meta.json ---
    meta = {
        "city": city,
        "region": region if region else None,
        "country": country,
        "datestamp": timestamp
    }
    with open(output_folder / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return output_folder 