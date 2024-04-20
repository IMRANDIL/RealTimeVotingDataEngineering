import requests
import random

BASE_URL = "https://randomuser.me/api/?nat=IN"
PARTIES = [
    "Socialist_Part",
    "Pakoda_Party",
    "Jumla_Party"
]

random.seed(21)  # Setting random seed for reproducibility

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
