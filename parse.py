#!/usr/bin/env python3
import gzip
import re
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_field(soup, label_text, extract_image=False):
    """Extract text or image URL from a div following a label."""
    try:
        label = soup.find('label', string=re.compile(label_text, re.IGNORECASE))
        if not label:
            label = soup.find('label', class_='form-label', string=re.compile(label_text, re.IGNORECASE))
        
        if label and (value_div := label.find_next_sibling('div')):
            if extract_image:
                if button := value_div.find('button', onclick=True):
                    if match := re.search(r"viewDocument\('([^']+)'\)", button.get('onclick', '')):
                        return match.group(1)
            else:
                text = value_div.get_text(strip=True)
                return text if text and text != '--' else None
        return None
    except Exception as e:
        logger.warning(f"Error extracting {label_text}: {e}")
        return None

def parse_html_file(file_path):
    """Parse HTML file and extract complaint data."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        if not (panel := soup.find('div', class_='panel panel-default')):
            logger.warning(f"No complaint panel found in {file_path}")
            return None
        
        complaint_id = Path(file_path).stem
        
        # Define field mappings
        fields = {
            'complaint_id': extract_field(panel, r'Complaint\s*ID') or complaint_id,
            'category': extract_field(panel, r'Category'),
            'sub_category': extract_field(panel, r'Sub\s*category'),
            'grievance_date': extract_field(panel, r'Grievance\s*Date'),
            'ward_name': extract_field(panel, r'Ward\s*Name'),
            'address': extract_field(panel, r'Address'),
            'description': extract_field(panel, r'Description'),
            'grievance_status': extract_field(panel, r'Grievance\s*Status'),
            'staff_remarks': extract_field(panel, r'Staff\s*Remarks'),
            'staff_name': extract_field(panel, r'Staff\s*Name'),
            'contact_details': extract_field(panel, r'Contact\s*Details'),
            'image': extract_field(panel, r'Image'),
            'staff_recent_added_image': extract_field(panel, r'Staff\s*Recent\s*Added\s*Image', extract_image=True)
        }
        
        logger.info(f"Parsed {file_path}: {fields['complaint_id']}")
        return fields
        
    except Exception as e:
        logger.error(f"Error parsing {file_path}: {e}")
        return None

def load_existing_data(output_file):
    """Load existing complaint IDs and data from Parquet file."""
    if not output_file.exists():
        logger.info("No existing Parquet file found")
        return set(), []
    
    try:
        df = pd.read_parquet(output_file)
        existing_ids = set(df['complaint_id'].astype(str))
        existing_data = df.to_dict('records')
        logger.info(f"Loaded {len(existing_ids)} existing complaint IDs")
        return existing_ids, existing_data
    except Exception as e:
        logger.warning(f"Error reading existing file: {e}. Creating new file.")
        return set(), []

def get_files_to_process(raw_dir, existing_ids):
    """Get HTML files that need processing."""
    html_files = [f for f in raw_dir.glob("*.html") if f.stat().st_size > 0]
    files_to_process = [f for f in html_files if f.stem not in existing_ids]
    
    logger.info(f"Found {len(html_files)} HTML files, processing {len(files_to_process)} new ones")
    return files_to_process

def parse_all_html_files():
    """Parse HTML files and save to Parquet with incremental updates."""
    raw_dir, data_dir = Path("raw"), Path("data")
    
    if not raw_dir.exists():
        logger.error(f"Raw directory {raw_dir} does not exist")
        return
    
    data_dir.mkdir(exist_ok=True)
    output_file = data_dir / "combined.parquet"
    
    # Load existing data and get files to process
    existing_ids, existing_data = load_existing_data(output_file)
    files_to_process = get_files_to_process(raw_dir, existing_ids)
    
    if not files_to_process:
        print("No new files to process. All HTML files already parsed.")
        return
    
    # Parse new files
    new_data = []
    for file_path in files_to_process:
        if data := parse_html_file(file_path):
            if str(data['complaint_id']) not in existing_ids:
                new_data.append(data)
            else:
                logger.warning(f"Duplicate ID {data['complaint_id']}, skipping")
    
    if not new_data and not existing_data:
        logger.error("No data to save")
        return
    
    # Combine and save data
    df = pd.DataFrame(existing_data + new_data)
    df['grievance_date'] = pd.to_datetime(df['grievance_date'], format='%d/%m/%Y %H:%M')
    df = df[['complaint_id', 'category', 'sub_category', 'description', 'grievance_date', 'ward_name', 'address', 'grievance_status', 'staff_remarks', 'staff_name', 'contact_details', 'image', 'staff_recent_added_image']]
    df.sort_values(by='grievance_date', inplace=True, ascending=False)
    df.to_parquet(output_file, index=False)

    # Create a copy for the final dataset with renamed columns
    final_df = df.rename(columns={
        'complaint_id': 'Complaint ID',
        'category': 'Category',
        'sub_category': 'Sub Category',
        'description': 'Description',
        'grievance_date': 'Grievance Date',
        'ward_name': 'Ward Name',
        'address': 'Address',
        'grievance_status': 'Grievance Status',
        'staff_remarks': 'Staff Remarks',
        'staff_name': 'Staff Name',
        'contact_details': 'Contact Details',
        'image': 'Image',
        'staff_recent_added_image': 'Staff Recent Added Image'
    })
    
    # Drop the specified columns
    columns_to_drop = ['Description', 'Address', 'Contact Details', 'Image', 'Staff Recent Added Image']
    final_df = final_df.drop(columns=columns_to_drop)
    
    # Save final Parquet and compressed CSV
    final_df.to_parquet('data/citizen-grievances.parquet')
    final_df.to_csv('data/citizen-grievance.csv.gz', compression='gzip', index=False)
    
    logging.info(f"Successfully saved {len(final_df)} records")

if __name__ == "__main__":
    parse_all_html_files()