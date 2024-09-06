import requests
import datetime

def fetch_data(api_url, access_token, limit=25):
    headers = {
        'access_token': access_token
    }

    all_data = []
    total_documents = 0
    offset = 0
    while True:
        paginated_url = f"{api_url}&limit={limit}&offset={offset}"
        print(f"Fetching data with URL: {paginated_url}")  # Debugging line
        response = requests.get(paginated_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if not items:
                break
            total_documents += len(items)
            for item in items:
                emission_date = datetime.datetime.fromtimestamp(item['emissionDate'])
                if emission_date.year == 2024 and emission_date.month == 5:
                    filtered_item = {
                        "emissionDate": emission_date,
                        "number": item.get("number"),
                        "codeSii": item.get("codeSii"),
                        "clientCode": item.get("clientCode"),
                        "clientActivity": item.get("clientActivity"),
                        "netAmount": item.get("netAmount", 0.0),
                        "iva": item.get("iva", 0.0),
                        "ivaAmount": item.get("ivaAmount", 0.0),
                        "totalAmount": item.get("totalAmount", 0.0),
                        "bookType": item.get("bookType"),
                        "urlPdf": item.get("urlPdf"),
                        "urlXml": item.get("urlXml")
                    }
                    all_data.append(filtered_item)
            offset += limit
        else:
            print(f"Error al obtener datos en offset {offset}: {response.status_code}")
            break
    
    return all_data, total_documents

if __name__ == "__main__":
    api_url = "https://api.bsale.cl/v1/documents.json?emissiondaterange=[1714521600,1717113599]"
    access_token = "e57d148002d91e58f152bece58d810e50bc84286"

    data, total_documents = fetch_data(api_url, access_token)
    print(f"Total documentos recuperados: {total_documents}")
    for doc in data:
        print(doc)
