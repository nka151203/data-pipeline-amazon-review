import requests
import numpy as np

user_permission = False

def get_text_embedding(text, is_download =  False):
    if is_download == False:
        if user_permission:
            url = "https://api.deepinfra.com/v1/inference/sentence-transformers/all-MiniLM-L6-v2"
            headers = {
                "Authorization": "Bearer 88YOid3vTGjhqiGZx4mmXXB7l8LM1huc",
                "Content-Type": "application/json"
            }
            data = {
                "inputs": [str(text)]
            }

            response = requests.post(url, json=data, headers=headers)
            if response.status_code == 200:
                return response.json()["embeddings"][0]
            else:
                print("Request failed:", response.status_code, response.text)
                return []
        else:
            """
            To save on API costs or save inference time of model, please use fake embeddings
            if you're only testing the pipeline.
            """
            return np.random.rand(1,384).tolist()
    else:
        #If you downloaded local sentence_transformers library
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer('all-MiniLM-L6-v2')
        embeddings = model.encode([str(text)])
        return embeddings.tolist()
    
    
a = get_text_embedding("Hello, I love Ngoc Linh")

