from decouple import config
from supabase import create_client, Client
from curl_cffi import requests
from datetime import datetime
import json

REWARD_PROGRAM = "Qantas Frequent Flyer"

supabaseUrl: str = config("SUPABASE_URL")
supabaseKey: str = config("SUPABASE_KEY")
supabase: Client = create_client(supabaseUrl, supabaseKey)

flightDataUrl: str = "https://api.qantas.com/market-pricing/mpp/v1/calendar?origin={departing}&destination={arriving}&tripType=O&travelClass={fare}&isClassic=true&startDate={when}&language=en"


def fetch_flight_data(departing: str, arriving: str, when: str, fare: str):
    print("Fetching {fare} flight data for departing {departing} and arriving {arriving} starting at {when}".format(
        departing=departing, arriving=arriving, when=when, fare=fare))
    url = flightDataUrl.format(departing=departing, arriving=arriving, when=when, fare=fare)
    response = requests.get(url, impersonate="chrome")
    if response.status_code == 200:
        data = response.json()
        reward_flights_data = data["days"]
        return reward_flights_data
    else:
        print("Error fetching flight received code {code}".format(code=response.status_code))
        return []


def create_reward_flight_row(raw_row: dict, fare, reward_program_id, route_id) -> dict:
    departure_date = datetime.strptime(raw_row["departureDate"], "%d%m%y")
    return {
        "program": reward_program_id,
        "route": route_id,
        "points": raw_row["basePoints"],
        "taxes": raw_row["totalTax"],
        "currency": 41,
        "date": departure_date.strftime("%Y-%m-%d"),
        "fare": fare
    }


def upload_to_db(rows):
    insert_response = supabase.table("reward_flight").insert(rows).execute()
    print("Uploaded {} rows to DB".format(insert_response.count))


def fetch_routes():
    response = supabase.table("route").select("id, arriving!inner(code), departing!inner(code)").execute()
    data = json.loads(response.json())
    airport_pairs = [(item["id"], item["arriving"]["code"], item["departing"]["code"]) for item in data["data"]]
    return airport_pairs


def format_today_date():
    # Get today's date
    today = datetime.today()
    # Format the date as ddmmYY
    formatted_date = today.strftime("%d%m%y")
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


def main():
    fetch_qantas_reward_program_id()
    airport_pairs = fetch_routes()
    today = format_today_date()
    qantas_reward_program_id = fetch_qantas_reward_program_id()
    fare_mappings = fetch_qantas_fare_mappings(qantas_reward_program_id)

    for airport_pair in airport_pairs:
        rows = []
        for fare_mapping in fare_mappings:
            flights = fetch_flight_data(airport_pair[1], airport_pair[2], today, fare_mapping[0])
            for flight in flights:
                rows.append(create_reward_flight_row(flight, fare_mapping[1], qantas_reward_program_id, airport_pair[0]))
        if len(rows) > 0:
            upload_to_db(rows)



if __name__ == "__main__":
    main()
