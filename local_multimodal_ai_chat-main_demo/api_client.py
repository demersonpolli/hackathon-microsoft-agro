import requests

API_HOST = "https://localhost:7044"

def health_check():
    response = requests.get(f"{API_HOST}/health", verify=False)
    response.raise_for_status()
    return response.json()

def classify_image(url):
    try:
        response = requests.post(f"{API_HOST}/classify_pest?url={url}", verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {f"Classify Image Error: {e}"}

def classify_pest_file(file_path): 
    try:
        with open(file_path, 'rb') as file:
            header={"Content-Type": "multipart/form-data"}
            response = requests.post(f"{API_HOST}/classify_pest_file",headers=header, files={'file': file}, verify=False)
            response.raise_for_status()
            return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": f"Failed to classify pest file: {e}"}

def get_recommendation(pest):
    response = requests.get(f"{API_HOST}/control_insect_suggestion", params={"pest": pest}, verify=False)
    response.raise_for_status()
    return response.json()

def get_questions(question):
    response = requests.get(f"{API_HOST}/question", params={"question": question}, verify=False)
    response.raise_for_status()
    return response.json()

def get_registered_products(pest):
    response = requests.get(f"{API_HOST}/get_registered_products", params={"pest": pest}, verify=False)
    response.raise_for_status()
    return response.json()