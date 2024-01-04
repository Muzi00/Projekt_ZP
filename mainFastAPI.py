from fastapi import FastAPI
import uvicorn
import requests
from bs4 import BeautifulSoup
import logging

app = FastAPI()

logging.basicConfig(level=logging.DEBUG)

@app.get("/")
async def read_root():
    return {"message": "Witaj, to jest ścieżka główna!"}

@app.get("/{asin}")
async def get_data(asin: str):
    try:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        results = []
#        for woj in ["mazowieckie","slaskie","dolnoslaskie","kujawsko--pomorskie",
#"lodzkie","lubelskie","lubuskie","malopolskie","opolskie","podkarpackie",
#"podlaskie","pomorskie","swietokrzyskie","warminsko--mazurskie","wielkopolskie",
#"zachodniopomorskie"]:
        for woj in ["mazowieckie"]:
            resp = session.get(f"https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/"+woj+"/{asin}")

            if resp.status_code != 200:
                logging.error(f"Bad status code: {resp.status_code}")
                return {"error": f"Bad status code {resp.status_code}"}

            soup = BeautifulSoup(resp.text, "html.parser")

            price_elements = soup.select("[class='e1jyrtvq0 css-1tjkj49 ei6hyam0']")

            #results = []

            for price_element in price_elements:
                cena = price_element.text.replace("zł","!").replace("/m²","").replace(" pokoje","!").replace(" pokój","!").replace(" pokojów","!").replace("m²","!").split("!")
                miasto = asin

                data = {
                    "województwo": woj,
                    "miasto": miasto,
                    "cena": cena[0]+"zł",
                    "cena /m2": cena[1]+"zł/m²",
                    "pokoje": cena[2],
                    "metraż": cena[3]+"m²",
                }




                results.append(data)


        return {"results": results}

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        return {"error": f"Exception occurred: {str(e)}"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8005, log_level="info")
