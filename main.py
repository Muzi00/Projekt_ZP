from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from web_scraping import get_listing_links_async, DiskCache
from tqdm import tqdm
import uvicorn
from datetime import datetime

# Create FastAPI instance
app = FastAPI()

last_retrieval_time = None
current_retrieval_time = None


@app.get("/scrape")
async def scrape_otodom(background_tasks: BackgroundTasks):
    global last_retrieval_time, current_retrieval_time

    last_retrieval_time = current_retrieval_time

    # Set your desired start URL and cache expiration time
    start_url = 'https://www.otodom.pl/pl/wyniki/wynajem/dom/cala-polska'
    cache_expiration_time = 2592000  # 30 days in seconds

    # Add the web scraping task to background tasks
    background_tasks.add_task(
        get_listing_links_async,
        start_url,
        DiskCache(expiration_time=cache_expiration_time),
        tqdm(total=1, desc="Downloading the number of pages", position=0)
    )

    current_retrieval_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    response_content = {
        "message": "Web scraping started",
        "last_retrieval_time": last_retrieval_time,
        "current_retrieval_time": current_retrieval_time
    }

    return JSONResponse(content=response_content)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8005, reload=True)
