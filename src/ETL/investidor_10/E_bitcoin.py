import requests 
import json 
from datetime import date

def bitcoin():
    url = 'https://investidor10.com.br/api/criptomoedas/cotacoes/1/30/dollar'
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://investidor10.com.br/',
    'Connection': 'keep-alive',
    }
    response = requests.get(url, headers=headers)
    data = response.json() 
    extraction_date = date.today().strftime("%Y-%m-%d")
    with open(f'data/raw/raw_bitcoin/bitcoin_{extraction_date}.json','w') as f:
        json.dump(data,f,indent=4)
    
    

if __name__ == '__main__':
    bitcoin()
    
    
    # curl 'https://investidor10.com.br/api/criptomoedas/cotacoes/1/3650/dollar' --compressed -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0' -H 'Accept: */*' -H 'Accept-Language: en-US,en;q=0.5' -H 'Accept-Encoding: gzip, deflate, br, zstd' -H 'Referer: https://investidor10.com.br/criptomoedas/bitcoin/' -H 'X-CSRF-TOKEN: x03bz4QgJt6fqRH9uw6rBkWki2zaLfw8YeBhGKdM' -H 'X-Requested-With: XMLHttpRequest' -H 'Alt-Used: investidor10.com.br' -H 'Connection: keep-alive' -H 'Cookie: XSRF-TOKEN=eyJpdiI6ImpvZTFxd043TjYxSm1pOC9KQklIcFE9PSIsInZhbHVlIjoiek9QQlZrNWs4Z2xHVHl0TWViK2dVRThPRzJxcWZWRk1QV1BBcmt2cjhnRCtlOFVYZlUrT0QxdXArNDJkVUYwMkF0RlNFT1hSbU1tM1crVlduTzgvS01nMWk2WDZhVERBa0FxNEhBbG1HR2RvclpUd1JKTC95ZHpwT21CQWFZVVQiLCJtYWMiOiJiZDA2MGQ1NWY2Mzg4ZTUzMWE5ZGYwZDUwOGQ4M2VkYTZiMjAxZGM0MDI3MzAzMmU3YmQzYjU4NGY2NTU0MTM1IiwidGFnIjoiIn0%3D; laravel_session=eyJpdiI6IkVnN3pSbnhUV3Q1OFF4VzFZUDk1bmc9PSIsInZhbHVlIjoiVDBIZTh6ZmMyRzBuWjJxa2crVGtvVkgrWko4VjA1Z2o2ZE1sU2hsN2pScGpBeHBNTGJHajdpUXhIRFhQZ0JxcHJqbTA1V04zRzNFVFRhUGxBek9tditCVDF3blBCYkV5V2sxMjRJcjcvRFJ3WG95RGgrL1liMFBOZzhnL1JORVoiLCJtYWMiOiJmYWZjMGQ3MzYwMDNhZjgxZTIzNzI2MDFjZmQ5MDlkMGUxNTEzZjdmNmNmM2MzYjA3NmQ5NGMzODIyMWEzMGU4IiwidGFnIjoiIn0%3D; hash-ads_popup=eyJpdiI6IkNndWo3UmdxUWZvUnhPN0toM085WXc9PSIsInZhbHVlIjoiOGVmL1lOZjV6UTlxTzBRdFR0bE9OdUcrY1JQaTBKMFRsZUJnUWNZQkJZaGlWQ3JLaW9qanNqY2E0bjJ2QUxaOWFRRU1hdHQ4UzE0ZndkN0hFUUVpUmtnTm5sWjdyaFUvT1VYcjZOeFlCeVE9IiwibWFjIjoiZmMyMDA5OTA0NWM0NzZjMmIxOWUyMjM3MTUyOWVkYzkzZjM2OTU5MTlmMmUwZjU5YmM0MTdiM2Q2YzVjOGUzMSIsInRhZyI6IiJ9; buy_and_hold=5; g_state={"i_p":1742248522675,"i_l":2}; remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d=eyJpdiI6IlZzclZiQ2p0ZHl4djdvd1ZBN0lLT1E9PSIsInZhbHVlIjoiUitaY2tjZEN0b1VMZ1ZGMTMzamg3ZEtlOHFKQTVweTN2RW9hY2RXVEZERjlZUVdTckh6VnQ3VXNucU0xQytMMlUxUlZQbVk1V2ZjT0YyRFFMNFpvNWJ6b1l5cTdLbGVZdGtjN2lVbmFVR2dxK1daTjZNNW1vM1d0MzdON2VOWjZCamZ1enZhRVM5d1hMaURhWVVWNXZ3PT0iLCJtYWMiOiIxZDZkNzVjNWQ4MmQ4Nzc5YmQxMDUxNGY4OTU3MmUyMjJlZWI2NDVjMTExNDI5NjRjMDNiZjY0OTBjN2NkNjg0IiwidGFnIjoiIn0%3D; user_phone=1; cookie_contador_whole-site=eyJpdiI6ImVYZjZKU2ZQOUMwalJ2K1kzNkx4aHc9PSIsInZhbHVlIjoiVm5uWWp0YXNiWFRaSThQVWZwcWhTM2JzSVc4ZlZUVldSdlNYZHg5ZUFFS2pxSEZUbXRjY1VOUURIUlRTdEkvQ1FQSG9sajJyck93T3ZmRlpUWk1CUWc9PSIsIm1hYyI6ImYwOGY5ZWY3NTRkODYzOTQ5NWRhNjE4ODVmNTZjYjMwNTJjNDliZTU0ZjhiMWYyOTE5MTdiYWY3YjlhOTRiNjIiLCJ0YWciOiIifQ%3D%3D' -H 'Sec-Fetch-Dest: empty' -H 'Sec-Fetch-Mode: cors' -H 'Sec-Fetch-Site: same-origin'