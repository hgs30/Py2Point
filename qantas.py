from supabase import create_client, Client
from decouple import config
from curl_cffi import requests
from datetime import datetime
import json

REWARD_PROGRAM = "Qantas Frequent Flyer"
USE_LIVE_PRICING = True

supabaseUrl: str = config("SUPABASE_URL")
supabaseKey: str = config("SUPABASE_KEY")
supabase: Client = create_client(supabaseUrl, supabaseKey)

marketPricingUrl: str = "https://api.qantas.com/market-pricing/mpp/v1/calendar?origin={departing}&destination={arriving}&tripType=O&travelClass={fare}&isClassic=true&startDate={when}&language=en"
livePricingUrl: str = "https://api.qantas.com/api/flight/livepricing/v1/calendar?origin={departing}&destination={arriving}&startDate={when}&endDate={end}&tripType=O&travelClass={fare}&classicReward=true"

def fetch_market_flight_data(departing: str, arriving: str, when: str, fare: str):
    print("Fetching market {fare} flight data for departing {departing} and arriving {arriving} starting at {when}".format(
        departing=departing, arriving=arriving, when=when, fare=fare))
    url = marketPricingUrl.format(departing=departing, arriving=arriving, when=when, fare=fare)
    response = requests.get(url, impersonate="chrome")
    if response.status_code == 200:
        data = response.json()
        reward_flights_data = data["days"]
        return reward_flights_data
    else:
        print("Error fetching flight received code {code}".format(code=response.status_code))
        return []

def fetch_live_flight_data(departing: str, arriving: str, when: str, fare: str, end: str):
    print("Fetching live {fare} flight data for departing {departing} and arriving {arriving} starting at {when}".format(
        departing=departing, arriving=arriving, when=when, fare=fare))
    url = livePricingUrl.format(departing=departing, arriving=arriving, when=when, end=end, fare=fare)
    response = requests.get(url, impersonate="chrome")
    if response.status_code == 200:
        data = response.json()
        reward_flights_data = data["days"]
        return reward_flights_data
    else:
        print("Error fetching flight received code {code}".format(code=response.status_code))
        return []


def transform_market_flight_row(raw_row: dict, fare, reward_program_id, route_id, currency_id) -> dict:
    departure_date = datetime.strptime(raw_row["departureDate"], "%d%m%y")
    return {
        "program": reward_program_id,
        "route": route_id,
        "points": raw_row["basePoints"],
        "taxes": raw_row["totalTax"],
        "currency": currency_id,
        "date": departure_date.strftime("%Y-%m-%d"),
        "fare": fare
    }

def transform_live_flight_row(raw_row: dict, fare, reward_program_id, route_id, currency_id) -> dict:
    departure_date = datetime.strptime(raw_row["departureDate"], "%d%m%y")
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    return {
        "program": reward_program_id,
        "route": route_id,
        "points": raw_row["totalPoints"],
        "taxes": raw_row["totalTax"],
        "currency": currency_id,
        "date": departure_date.strftime("%Y-%m-%d"),
        "fare": fare,
        "updated_at": current_datetime
    }


def upload_to_db(rows):
    print("Uploading data to database")
    insert_response = supabase.table("reward_flight").upsert(rows, count="estimated", on_conflict="program,route,date,fare").execute()
    print("Uploaded {} rows to DB".format(insert_response.count))


def fetch_routes():
    # Until I work out how the API works, only get routes that depart AU
    response = (supabase
                .table("route")
                .select("id, departing!inner(code, country!inner(code)), arriving!inner(code)")
                .eq("departing.country.code", "AU")
                .execute())

    data = json.loads(response.json())
    airport_pairs = [(item["id"], item["departing"]["code"], item["arriving"]["code"]) for item in data["data"]]
    return airport_pairs


def format_today_date():
    # Get today's date
    today = datetime.today()
    # Format the date as ddmmYY
    formatted_date = today.strftime("%d%m%y")
    return formatted_date

def date_plus_two_years():
    today = datetime.today()
    future_date = today.replace(year=today.year + 2)
    formatted_date = future_date.strftime("%d%m%y")
    return formatted_date

def fetch_qantas_reward_program_id():
    response = supabase.table("reward_program").select("id, name").eq("name", REWARD_PROGRAM).execute()
    data = json.loads(response.json())
    return data["data"][0]["id"]


def fetch_qantas_fare_mappings(qantas_reward_program_id):
    response = supabase.table("fare_mapping").select("program, fare!inner(id), code").eq("program",
                                                                                         qantas_reward_program_id).execute()
    data = json.loads(response.json())
    code_fare_pairs = [(item['code'], item['fare']['id']) for item in data['data']]
    return code_fare_pairs

def fetch_currency_id():
    response = (supabase
                .table("country")
                .select("code, currency!inner(id)")
                .eq("code", "AU")
                .execute())
    data = json.loads(response.json())
    return data["data"][0]["currency"]["id"]

def main():
    fetch_qantas_reward_program_id()
    airport_pairs = fetch_routes()
    currency_id = fetch_currency_id()
    today = format_today_date()
    qantas_reward_program_id = fetch_qantas_reward_program_id()
    fare_mappings = fetch_qantas_fare_mappings(qantas_reward_program_id)
    end = date_plus_two_years()

    for airport_pair in airport_pairs:
        rows = []
        for fare_mapping in fare_mappings:
            if USE_LIVE_PRICING:
                flights = fetch_live_flight_data(airport_pair[1], airport_pair[2], today, fare_mapping[0], end)
                for flight in flights:
                    rows.append(transform_live_flight_row(flight, fare_mapping[1], qantas_reward_program_id, airport_pair[0], currency_id))
            else:
                flights = fetch_market_flight_data(airport_pair[1], airport_pair[2], today, fare_mapping[0])
                for flight in flights:
                    rows.append(transform_market_flight_row(flight, fare_mapping[1], qantas_reward_program_id, airport_pair[0], currency_id))
        if len(rows) > 0:
            upload_to_db(rows)


if __name__ == "__main__":
    main()
