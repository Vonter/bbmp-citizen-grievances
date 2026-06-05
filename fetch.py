#!/usr/bin/env python3

import json
import requests
import time
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

LIST_API_URL = "https://mahitikanaja.karnataka.gov.in/Bbmp/api/Bbmp/Bbmp"
DETAIL_URL = "https://www.smartoneblr.com/WssBBMPComplaintRequestDetails.htm"

LIST_START = date(2020, 1, 1)
TERMINAL_STATUSES = {"closed", "rejected", "resolved", "non relevant"}
PARQUET_PATH = Path("data/combined.parquet")


def month_iter():
    d = LIST_START.replace(day=1)
    end = date.today().replace(day=1)
    while d <= end:
        yield d.year, d.month
        d = (d.replace(day=28) + timedelta(days=4)).replace(day=1)


def month_range_strings(year, month):
    from_date = date(year, month, 1)
    to_date = (from_date.replace(day=28) + timedelta(days=4)).replace(day=1)
    return (
        f"{from_date.day}/{from_date.month}/{from_date.year}",
        f"{to_date.day}/{to_date.month}/{to_date.year}",
    )


def fetch_list_for_month(year, month, session):
    from_str, to_str = month_range_strings(year, month)
    response = session.post(
        LIST_API_URL,
        json={"FromDate": from_str, "ToDate": to_str, "ServiceID": 4338, "DepartmentID": 2091, "ReqType": "Sahaya"},
        headers={"Content-Type": "application/json; charset=utf-8"},
        timeout=300,
    )
    response.raise_for_status()
    return response.json()


def ids_from_list_data(data):
    entities = (data.get("ResponseBody") or {}).get("ReportEntity") or []
    return {int(gid) for e in entities if (gid := e.get("Grievance_Number"))}


def load_parquet_ids():
    """Return (all_ids, non_terminal_ids) from the existing parquet."""
    try:
        df = pd.read_parquet(PARQUET_PATH, columns=["complaint_id", "grievance_status"])
        all_ids = set(df["complaint_id"].dropna().astype(int))
        non_terminal_ids = set(
            df[~df["grievance_status"].str.lower().isin(TERMINAL_STATUSES)]["complaint_id"].dropna().astype(int)
        )
        return all_ids, non_terminal_ids
    except Exception as e:
        print(f"Error loading parquet: {e}")
        return set(), set()


def fetch_lists(refetch_ids=frozenset()):
    list_dir = Path("raw/list")
    list_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    today = date.today()
    all_ids = set()

    for year, month in month_iter():
        filename = list_dir / f"{year:04d}-{month:02d}.json"
        is_current = (year == today.year and month == today.month)

        cached_data = json.loads(filename.read_text()) if filename.exists() else {}
        cached_ids = ids_from_list_data(cached_data)
        needs_refetch = bool(cached_ids & refetch_ids)

        if cached_data and not is_current and not needs_refetch:
            print(f"Loaded {year}-{month:02d}: {len(cached_ids)} grievances")
            all_ids |= cached_ids
            continue

        if is_current:
            reason = "current month"
        elif needs_refetch:
            reason = "has non-terminal IDs"
        else:
            reason = "not cached"
        print(f"Fetching list for {year}-{month:02d} ({reason})...")
        try:
            data = fetch_list_for_month(year, month, session)
            filename.write_text(json.dumps(data, indent=2))
            month_ids = ids_from_list_data(data)
            print(f"  Got {len(month_ids)} grievances")
            all_ids |= month_ids
            time.sleep(1)
        except Exception as e:
            print(f"  Error: {e}")
            all_ids |= cached_ids

    print(f"Total grievance IDs from lists: {len(all_ids)}")
    return all_ids


def extract_panel_content(html_content):
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        panel_div = soup.find("div", class_="panel")
        return str(panel_div) if panel_div else None
    except Exception:
        return None


def fetch_complaint_details(grievance_ids, existing_ids, refetch_ids=frozenset()):
    Path("raw").mkdir(exist_ok=True)
    already_have = {int(f.stem) for f in Path("raw").glob("*.html")}
    to_fetch = sorted(
        gid for gid in grievance_ids
        if gid not in existing_ids and (gid not in already_have or gid in refetch_ids)
    )
    print(f"Fetching {len(to_fetch)} complaint details...")

    base_form = {
        "compno": "^", "assMntNo": "^", "alfaNo": "^", "SbassMntNo": "^",
        "pageNameV": "waterTaxSearch.htm^", "mobnoFlg": "^", "mobNumber": "^",
        "sessionLangCode": "^", "RefNo": "CSCRefNo^", "deptId": "BBMP^",
        "searchBy": "refNoDiv^", "mobNum": "",
    }

    for complaint_id in to_fetch:
        form_data = {**base_form, "complainantNo": f"{complaint_id}^", "applicationNo": f"{complaint_id}^"}
        params = {"_show": "Show", "complainantNo": complaint_id}

        try:
            response = requests.post(DETAIL_URL, params=params, data=form_data, timeout=10)
            response.raise_for_status()

            if "Grievance Status" in response.text:
                panel_content = extract_panel_content(response.text)
                if panel_content:
                    Path(f"raw/{complaint_id}.html").write_text(panel_content, encoding="utf-8")
                    print(f"Saved: {complaint_id}")
                else:
                    print(f"No panel: {complaint_id}")
            else:
                print(f"Invalid: {complaint_id}")

        except Exception as e:
            print(f"Error {complaint_id}: {e}")

        time.sleep(0.1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--refetch", action="store_true",
                        help="Refetch list files and details for non-terminal grievances")
    args = parser.parse_args()

    all_ids, non_terminal_ids = load_parquet_ids()
    refetch_ids = non_terminal_ids if args.refetch else frozenset()
    existing_ids = all_ids - refetch_ids

    if args.refetch:
        print(f"Loaded {len(existing_ids)} existing IDs (will refetch {len(non_terminal_ids)} non-terminal)")
    else:
        print(f"Loaded {len(all_ids)} existing IDs")

    grievance_ids = fetch_lists(refetch_ids=refetch_ids)
    fetch_complaint_details(grievance_ids, existing_ids=existing_ids, refetch_ids=refetch_ids)
