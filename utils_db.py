import requests
import random

BASE_URL = "https://randomuser.me/api/?nat=IN"
PARTIES = [
    "Socialist_Part",
    "Pakoda_Party",
    "Jumla_Party"
]

def generate_candidate(candidate_id, num_candidates):
    try:
        response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_id % 2 == 1 else "male"))
        response.raise_for_status()  # Raise an error for bad responses (e.g., status code 4xx or 5xx)
        sample_candidates_data = response.json()['results'][0]
        
        party_index = candidate_id % num_candidates  # Ensure index is within range of PARTIES list
        party_affiliation = PARTIES[party_index]
        
        return {
            'candidate_id': sample_candidates_data['login']['uuid'],
            'candidate_name': f"{sample_candidates_data['name']['first']} {sample_candidates_data['name']['last']}",
            'party_affiliation': party_affiliation,
            'biography': "A brief biography of a candidate",
            'campaign_platform': "Key campaign promises or jumlas",
            'photo_url': sample_candidates_data['picture']['large']
        }
    except requests.RequestException as e:
        print(f"Error fetching candidate data: {e}")
        return None


def generate_voter_data():
    try:
        response = requests.get(BASE_URL)
        if response.status_code == 200:
            sample_voter_data = response.json()['results'][0]
            return {
                'voter_id': sample_voter_data['login']['uuid'],
                'voter_name': f"{sample_voter_data['name']['first']} {sample_voter_data['name']['last']}",
                'date_of_birth': sample_voter_data['dob']['date'],
                'gender': sample_voter_data['gender'],
                'nationality': sample_voter_data['nat'],
                'registration_number': sample_voter_data['registered']['number'],
                'address_street': sample_voter_data['location']['street']['name'],
                'address_city': sample_voter_data['location']['city'],
                'address_state': sample_voter_data['location']['state'],
                'address_country': sample_voter_data['location']['country'],
                'address_postcode': sample_voter_data['location']['postcode'],
                'email': sample_voter_data['email'],
                'phone_number': sample_voter_data['phone'],
                'cell_number': sample_voter_data['cell'],
                'picture': sample_voter_data['picture']['large'],
                'registered_age': sample_voter_data['registered']['age']
            }
        else:
            print("Error fetching voter data")
            return None
    except Exception as e:
        print(f"An error occurred while generating voter data: {e}")
        return None
    
    
def insert_voters(conn, curr, voter_data):
    try:
        with conn:
            curr.execute("""
                         INSERT INTO voters(
                             voter_id,
                             voter_name,
                             date_of_birth,
                             gender,
                             nationality,
                             registration_number,
                             address_street,
                             address_city,
                             address_state,
                             address_country,
                             address_postcode,
                             email,
                             phone_number,
                             cell_number,
                             picture,
                             registered_age
                         )
                         VALUES(
                             %(voter_id)s,
                             %(voter_name)s,
                             %(date_of_birth)s,
                             %(gender)s,
                             %(nationality)s,
                             %(registration_number)s,
                             %(address_street)s,
                             %(address_city)s,
                             %(address_state)s,
                             %(address_country)s,
                             %(address_postcode)s,
                             %(email)s,
                             %(phone_number)s,
                             %(cell_number)s,
                             %(picture)s,
                             %(registered_age)s
                         )
                         """, voter_data)
        print("Voter data inserted successfully!")
    except Exception as e:
        conn.rollback()
        print(f"Error occurred while inserting voter data: {e}")