import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time
import json

class TexasFoodTruckScraper:
    def __init__(self, api_key):
        self.api_key = os.getenv('YELP_API_KEY')
        self.base_url = "https://api.yelp.com/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "accept": "application/json"
        }
        self.api_calls = {
            'search': 0,
            'details': 0
        }
        
        self.texas_cities = [
            "Laredo, TX",           
            "Brownsville, TX",      
            "McAllen, TX",          
            "El Paso, TX",          
            "Eagle Pass, TX",       
            "Del Rio, TX",          
            "Harlingen, TX",        
            "Mission, TX",          
            "Pharr, TX",            
        ]

    def search_food_trucks(self, location):
        """Single search per city"""
        self.api_calls['search'] += 1
        
        endpoint = f"{self.base_url}/businesses/search"
        params = {
            "location": location,
            "categories": "foodtrucks,mexican,tacos",
            "limit": 50,
            "sort_by": "rating",
            "radius": 40000,
            "attributes": "mobile"
        }
        
        try:
            print(f"\n🔍 Searching food trucks in {location}...")
            print(f"API Calls - Search: {self.api_calls['search']}, Details: {self.api_calls['details']}")
            
            response = requests.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            
            return response.json().get('businesses', [])
            
        except requests.exceptions.RequestException as e:
            print(f"Error searching {location}: {e}")
            return []

    def get_business_details(self, business_id):
        """Get details for a specific business"""
        self.api_calls['details'] += 1
        
        endpoint = f"{self.base_url}/businesses/{business_id}"
        
        try:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error getting details: {e}")
            return None

    def collect_food_truck_data(self):
        """Collect all food truck data"""
        all_trucks = []
        
        for city in self.texas_cities:
            businesses = self.search_food_trucks(city)
            
            for business in businesses:
                details = self.get_business_details(business['id'])
                
                if details:
                    truck_info = {
                        'name': details.get('name'),
                        'phone': details.get('display_phone') or details.get('phone'),
                        'rating': details.get('rating'),
                        'review_count': details.get('review_count'),
                        'address': ', '.join(details.get('location', {}).get('display_address', [])),
                        'city': city,
                        'zip_code': details.get('location', {}).get('zip_code'),
                        'categories': ', '.join([cat['title'] for cat in details.get('categories', [])]),
                        'price': details.get('price', 'N/A'),
                        'url': details.get('url'),
                        'coordinates': f"{details.get('coordinates', {}).get('latitude', 'N/A')}, {details.get('coordinates', {}).get('longitude', 'N/A')}"
                    }
                    
                    all_trucks.append(truck_info)
                    print(f"✅ Found: {truck_info['name']} - {truck_info['phone']}")
                    print(f"   📍 {truck_info['address']}")
                    print(f"   API Calls - Search: {self.api_calls['search']}, Details: {self.api_calls['details']}")
                
                time.sleep(0.2)  # Rate limiting
            
            time.sleep(1)  # Delay between cities
        
        return pd.DataFrame(all_trucks)

    def save_results(self, df):
        """Save results to CSV"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        csv_file = f'texas_food_trucks_{timestamp}.csv'
        df.to_csv(csv_file, index=False)        
        
        return csv_file

def main():    
    try:
        print("Starting Texas food truck search...")
        scraper = TexasFoodTruckScraper(API_KEY)
        
        # Collect data
        results = scraper.collect_food_truck_data()
        
        if not results.empty:
            # Save results
            csv_file, report_file = scraper.save_results(results)
            
            print("\n Search completed!")
            print(f"Total API calls made:")
            print(f"- Search calls: {scraper.api_calls['search']}")
            print(f"- Detail calls: {scraper.api_calls['details']}")
            print(f"- Total calls: {sum(scraper.api_calls.values())}")
            print(f"\nFood trucks found: {len(results)}")
            print(f"\n CSV saved to: {csv_file}")
            print(f"Report saved to: {report_file}")
            
        else:
            print("\n No food trucks found")
            
    except Exception as e:
        print(f"\n Error: {e}")

if __name__ == "__main__":
    main()