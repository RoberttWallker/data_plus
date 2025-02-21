import requests

#token =  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjbGllbnRlX2lkIjoxLCJsaWNlbmNhX2lkIjoibGljZW5jYTEyMyIsInN0YXR1cyI6ImF0aXZhIiwiZXhwaXJhY2FvIjoiMjAyNS0xMi0zMVQwMDowMDowMCswMDowMCJ9.oHYfdTcSQtkuA1TLuUH5NEmRypWDNAZt2KzfP1Ji6fs"

def validar_licenca(token):
    url = "http://172.23.113.124:8000/api/validar_licenca/"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        response = response.json()
        dados = response.get('dados')
        return dados.get('status', 'desconhecido')
    except requests.RequestException as e:
        print(f"Erro de requisição: {e}")
        return None