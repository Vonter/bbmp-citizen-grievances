#!/usr/bin/env python3
import json
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
                return text if text and text not in ('--', 'null') else None
        return None
    except Exception as e:
        logger.warning(f"Error extracting {label_text}: {e}")
        return None

_RATING_REMARKS_RE = re.compile(
    r'<label[^>]*>\s*Rating Remarks\s*</label>\s*<div[^>]*>\s*(.*?)\s*</div>',
    re.DOTALL | re.IGNORECASE
)
_RATING_RE = re.compile(
    r'<label[^>]*>\s*Rating\s*</label>\s*<div[^>]*>\s*(\d+)\s*</div>',
    re.DOTALL | re.IGNORECASE
)

def _html_label_value(html_text, pattern):
    """Extract and normalize the captured group from a compiled label pattern."""
    if match := pattern.search(html_text):
        text = match.group(1).strip()
        return text if text and text not in ('--', 'null') else None
    return None

def extract_rating_remarks(html_text):
    """Extract Rating Remarks from raw HTML (field may live outside the parsed panel)."""
    return _html_label_value(html_text, _RATING_REMARKS_RE)

def extract_rating(html_text):
    """Extract Rating integer from raw HTML (field may live outside the parsed panel)."""
    if text := _html_label_value(html_text, _RATING_RE):
        return int(text) or None
    return None

def load_list_metadata():
    """Build a lookup dict from raw/list/*.json keyed by grievance ID (int)."""
    metadata = {}
    list_dir = Path("raw/list")
    if not list_dir.exists():
        return metadata
    for json_file in sorted(list_dir.glob("*.json")):
        try:
            data = json.loads(json_file.read_text())
            entities = (data.get("ResponseBody") or {}).get("ReportEntity") or []
            for entity in entities:
                gid = entity.get("Grievance_Number")
                if gid:
                    metadata[int(gid)] = entity
        except Exception as e:
            logger.warning(f"Error loading {json_file}: {e}")
    logger.info(f"Loaded list metadata for {len(metadata)} grievances")
    return metadata


def parse_html_file(file_path, list_metadata=None):
    """Parse HTML file and extract complaint data, enriched with list metadata."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_text = f.read()
        soup = BeautifulSoup(html_text, 'html.parser')

        if not (panel := soup.find('div', class_='panel panel-default')):
            logger.warning(f"No complaint panel found in {file_path}")
            return None

        complaint_id = Path(file_path).stem

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
            'staff_recent_added_image': extract_field(panel, r'Staff\s*Recent\s*Added\s*Image', extract_image=True),
            'rating_remarks': extract_rating_remarks(html_text),
            'rating': extract_rating(html_text),
            'zone_name': None,
            'complainant_name': None,
            'complainant_mobile': None,
            'grievance_mode': None,
            'employee_designation': None,
        }

        if list_metadata:
            entity = list_metadata.get(int(complaint_id))
            if entity:
                fields['zone_name'] = entity.get('ZoneName') or None
                fields['complainant_name'] = entity.get('ComplainantName', '').strip() or None
                fields['complainant_mobile'] = entity.get('Complainant_MobileNumber') or None
                fields['grievance_mode'] = entity.get('Grievance_Mode') or None
                fields['employee_designation'] = entity.get('Employee_Designation') or None

        logger.info(f"Parsed {file_path}: {fields['complaint_id']}")
        return fields

    except Exception as e:
        logger.error(f"Error parsing {file_path}: {e}")
        return None

REQUIRED_COLUMNS = {'rating_remarks', 'rating', 'zone_name', 'grievance_mode'}
TERMINAL_STATUSES = {"closed", "rejected", "resolved", "non relevant"}

def load_existing_data(output_file):
    """Load all existing complaint records as a dict keyed by complaint_id string."""
    if not output_file.exists():
        logger.info("No existing Parquet file found")
        return {}

    try:
        df = pd.read_parquet(output_file)
        if missing := REQUIRED_COLUMNS - set(df.columns):
            logger.info(f"Existing file missing columns {missing}, triggering full reparse")
            return {}
        records = {str(r['complaint_id']): r for r in df.to_dict('records')}
        logger.info(f"Loaded {len(records)} existing complaint IDs")
        return records
    except Exception as e:
        logger.warning(f"Error reading existing file: {e}. Creating new file.")
        return {}

def get_files_to_process(raw_dir, existing_records):
    """Get HTML files for new complaints and non-terminal complaints with updated HTML."""
    html_files = [f for f in raw_dir.glob("*.html") if f.stat().st_size > 0]
    files_to_process = [
        f for f in html_files
        if f.stem not in existing_records
        or str(existing_records[f.stem].get('grievance_status') or '').strip().lower() not in TERMINAL_STATUSES
    ]
    logger.info(f"Found {len(html_files)} HTML files, processing {len(files_to_process)} new/updated ones")
    return files_to_process

def parse_all_html_files():
    """Parse HTML files and save to Parquet with incremental updates."""
    raw_dir, data_dir = Path("raw"), Path("data")

    if not raw_dir.exists():
        logger.error(f"Raw directory {raw_dir} does not exist")
        return

    data_dir.mkdir(exist_ok=True)
    output_file = data_dir / "combined.parquet"

    existing_records = load_existing_data(output_file)
    files_to_process = get_files_to_process(raw_dir, existing_records)

    if not files_to_process:
        print("No new files to process. All HTML files already parsed.")
        return

    list_metadata = load_list_metadata()

    # Parse and upsert: new complaints are added, non-terminal ones are updated
    for file_path in files_to_process:
        if data := parse_html_file(file_path, list_metadata):
            existing_records[str(data['complaint_id'])] = data

    if not existing_records:
        logger.error("No data to save")
        return

    # Combine and save data
    df = pd.DataFrame(existing_records.values())
    df['grievance_date'] = pd.to_datetime(df['grievance_date'], format='%d/%m/%Y %H:%M')
    df['rating'] = pd.array(df['rating'], dtype=pd.Int8Dtype())
    df = df[[
        'complaint_id', 'complainant_name', 'complainant_mobile', 'category', 'sub_category',
        'description', 'grievance_date', 'zone_name', 'ward_name', 'address',
        'grievance_mode', 'grievance_status', 'staff_remarks', 'staff_name', 'employee_designation',
        'contact_details', 'image', 'staff_recent_added_image', 'rating_remarks', 'rating',
    ]]
    df.sort_values(by='complaint_id', inplace=True, ascending=False)
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
        'staff_recent_added_image': 'Staff Recent Added Image',
        'rating_remarks': 'Rating Remarks',
        'rating': 'Rating',
        'zone_name': 'Zone Name',
        'complainant_name': 'Complainant Name',
        'complainant_mobile': 'Complainant Mobile',
        'grievance_mode': 'Grievance Mode',
        'employee_designation': 'Staff Designation',
    })

    columns_to_drop = [
        'Description', 'Address', 'Contact Details', 'Image', 'Staff Recent Added Image',
        'Rating Remarks', 'Complainant Name', 'Complainant Mobile',
    ]
    final_df = final_df.drop(columns=columns_to_drop)
    final_df = final_df[[
        'Complaint ID', 'Category', 'Sub Category', 'Grievance Date',
        'Zone Name', 'Ward Name', 'Grievance Mode', 'Grievance Status',
        'Staff Remarks', 'Staff Name', 'Staff Designation', 'Rating',
    ]]

    # Save final Parquet and compressed CSV
    final_df.to_parquet('data/citizen-grievances.parquet', index=False)
    final_df.to_csv('data/citizen-grievances.csv.gz', compression='gzip', index=False)
    
    logging.info(f"Successfully saved {len(final_df)} records")

if __name__ == "__main__":
    parse_all_html_files()