import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime

# --- CONFIGURATION ---
START_YEAR = 2000
END_YEAR = 2025
DELAY = 1  # seconds between requests
BASE_YEAR_URL = "https://www.planecrashinfo.com/{year}/{year}-{page}.htm"

# Fields to extract from each crash page
FIELDS = ["Date", "Time", "Location", "Operator", "Flight #", "Route",
          "AC Type", "Registration", "cn / ln", "Aboard", "Fatalities", "Ground", "Summary"]

all_crashes = []

location_cache = {}

def clean_location(location):
    """
    Remove prefixes 'Near', 'Off', 'Over' from location string before geocoding.
    """
    if not location:
        return location
    # Remove the prefix if it exists
    cleaned = re.sub(r"^(Near|Off|Over)\s+", "", location, flags=re.IGNORECASE)
    return cleaned.strip()

def geocode_location(location):
    """Convert address string to (latitude, longitude) with caching"""
    if not location:
        return None, None
    if location in location_cache:
        return location_cache[location]
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": clean_location(location), "format": "json", "limit": 1}
    try:
        response = requests.get(url, params=params, headers={"User-Agent": "PlaneCrashScraper/1.0"})
        response.raise_for_status()
        data = response.json()
        if len(data) == 0:
            lat, lon = None, None
        else:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
        location_cache[location] = (lat, lon)
        return lat, lon
    except:
        location_cache[location] = (None, None)
        return None, None


def normalize_time(time_str):
    """
    Convert time string to 24-hour format (HH:MM).
    Handles '1600', '4:00 PM', '04:00', etc.
    Returns None if cannot parse.
    """
    if not time_str or time_str.strip() in ["?", ""]:
        return None
    try:
        # Remove spaces
        time_str = time_str.strip().upper()
        # Handle times like '1600' (no colon)
        if re.match(r"^\d{3,4}$", time_str):
            # e.g., '1600' -> '16:00'
            if len(time_str) == 3:
                time_str = "0" + time_str
            return f"{time_str[:2]}:{time_str[2:]}"
        else:
            # Parse times with colon and/or AM/PM
            dt = datetime.strptime(time_str, "%I:%M %p")  # e.g., 4:00 PM
            return dt.strftime("%H:%M")
    except:
        try:
            # fallback for HH:MM format
            dt = datetime.strptime(time_str, "%H:%M")
            return dt.strftime("%H:%M")
        except:
            return None


# Helper function to parse passengers and crew
def parse_passengers_crew(text):
    """
    Extract total, passengers, crew from text like:
    '8 (passengers:? crew:?)' or '7 (passengers:6 crew:1)'
    """
    total = passengers = crew = None
    
    # Extract total number before parentheses
    total_match = re.match(r"(\d+)", text)
    if total_match:
        total = int(total_match.group(1))
    
    # Extract passengers inside parentheses
    pc_match = re.search(r"passengers:([\d?]+)", text)
    if pc_match:
        passengers = pc_match.group(1)
        if passengers == "?":
            passengers = None
        else:
            passengers = int(passengers)
    
    # Extract crew inside parentheses
    crew_match = re.search(r"crew:([\d?]+)", text)
    if crew_match:
        crew = crew_match.group(1)
        if crew == "?":
            crew = None
        else:
            crew = int(crew)
    
    return total, passengers, crew


def scrape():
    for year in range(START_YEAR, END_YEAR + 1):
        page = 1
        while True:
            url = BASE_YEAR_URL.format(year=year, page=page)
            print(f"Fetching {url}")
            response = requests.get(url)
            if response.status_code != 200:
                print(f"No more pages for {year} at page {page}")
                break

            soup = BeautifulSoup(response.content, "html.parser")

            # Find the main crash table
            table = soup.find("table", {"border": "0", "cellpadding": "3"})
            if not table:
                print(f"No crash table found at {url}")
                break

            crash_data = {}
            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) != 2:
                    continue
                label = cols[0].get_text(strip=True).replace(":", "")
                value = cols[1].get_text(strip=True)
                if label in FIELDS:
                    if label == "Time":
                        value = normalize_time(value)
                        crash_data[label] = value
                    elif label == "Aboard":
                        total, passengers, crew = parse_passengers_crew(value)
                        crash_data["Aboard Total"] = total
                        crash_data["Aboard Passengers"] = passengers
                        crash_data["Aboard Crew"] = crew
                    elif label == "Fatalities":
                        total, passengers, crew = parse_passengers_crew(value)
                        crash_data["Fatalities Total"] = total
                        crash_data["Fatalities Passengers"] = passengers
                        crash_data["Fatalities Crew"] = crew
                    elif value == "?":
                        crash_data[label] = None
                    else:
                        crash_data[label] = value

            if crash_data:
                crash_data["Url"] = url
                all_crashes.append(crash_data)
                print(f"Scraped crash from {url}")

            page += 1
            time.sleep(DELAY)

    for crash in all_crashes:
        if "Location" in crash and crash["Location"]:
            lat, lon = geocode_location(crash["Location"])
            crash["Latitude"] = lat
            crash["Longitude"] = lon
            time.sleep(DELAY)

    # Save all crashes to CSV
    df = pd.DataFrame(all_crashes)
    df.to_csv(f"plane_crashes_{START_YEAR}_{END_YEAR}.csv", index=False)
    print(f"Scraping complete! Saved to plane_crashes_{START_YEAR}_{END_YEAR}.csv")


if __name__ == "__main__":
    scrape()
