from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

import requests, os
import pandas as pd
from typing import List, Dict, Any
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

if not API_KEY:
    raise RuntimeError("Set YOUTUBE_API_KEY in .env")

if not MONGO_URI:
    raise RuntimeError("Set MONGO_URI in .env")

client = MongoClient(MONGO_URI)
db = client["video_dashboard"]
manual_collection = db["manual_data"]

app = FastAPI(title="Multi-Source Video Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

# ---------------------------
# Helper to fetch JSON safely
# ---------------------------
def yt(url: str) -> Dict[str, Any]:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        r.encoding = "utf-8"
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def fix(s: str) -> str:
    if not isinstance(s, str):
        return s
    try:
        s.encode("utf-8").decode("utf-8")
        return s
    except:
        try:
            return s.encode("latin1").decode("utf-8")
        except:
            return s

# ---------------------------
# Upload Excel data to Mongo
# ---------------------------
@app.post("/upload-excel")
def upload_excel(file_path: str):
    try:
        df = pd.read_excel(file_path)
        df.columns = df.columns.str.strip().str.lower()
        df = df.fillna("")
        # Save to MongoDB
        records = df.to_dict(orient="records")
        if records:
            manual_collection.delete_many({})  # clear old
            manual_collection.insert_many(records)
        return {"status": "ok", "count": len(records)}
    except Exception as e:
        raise HTTPException(500, str(e))

# ---------------------------
# Fetch YouTube videos
# ---------------------------
@app.get("/search-videos")
def search_videos(query: str, start: str, end: str, max_results: int = 50):
    if query.startswith("#"):
        query = query[1:]
    video_ids, next_page, total = [], "", 0

    while True:
        url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&type=video&maxResults=50&q={query}"
            f"&publishedAfter={start}T00:00:00Z"
            f"&publishedBefore={end}T23:59:59Z"
            f"&pageToken={next_page}&key={API_KEY}"
        )
        data = yt(url)
        if data.get("error"):
            raise HTTPException(502, data["error"])
        for item in data.get("items", []):
            video_ids.append(item["id"]["videoId"])
            total += 1
            if total >= max_results:
                break
        if total >= max_results or not data.get("nextPageToken"):
            break
        next_page = data.get("nextPageToken", "")

    stats = []
    for i in range(0, len(video_ids), 50):
        batch = ",".join(video_ids[i:i+50])
        info = yt(
            f"https://www.googleapis.com/youtube/v3/videos"
            f"?part=snippet,statistics&id={batch}&key={API_KEY}"
        )
        if info.get("error"):
            continue
        for v in info.get("items", []):
            sn = v["snippet"]
            st = v["statistics"]
            stats.append({
                "video_id": v["id"],
                "title": fix(sn["title"]),
                "channel": fix(sn["channelTitle"]),
                "url": f"https://youtu.be/{v['id']}",
                "published": sn["publishedAt"][:10],
                "views": int(st.get("viewCount", 0)),
                "likes": int(st.get("likeCount", 0)),
                "comments": int(st.get("commentCount", 0)),
                "platform": "YouTube",
                "keywords": query.lower()
            })
    return {"videos": stats, "total": len(stats)}

# ---------------------------
# Combined endpoint
# ---------------------------
@app.get("/combined-videos")
def combined_videos(query: str, start: str, end: str, max_results: int = 50, source: str = "all"):
    yt_videos = []
    if source in ["all", "youtube"]:
        yt_videos = search_videos(query, start, end, max_results)["videos"]

    # Manual videos from Mongo
    manual_videos = list(manual_collection.find({}))
    query_lower = query.lower()
    filtered_manual = []

    for v in manual_videos:
        pub_date = str(v.get("published", ""))
        if not (start <= pub_date <= end):
            continue
        keywords = str(v.get("keywords", "")).lower().split(",")
        if any(query_lower in k.strip() for k in keywords):
            filtered_manual.append(v)
        # Fill channel if empty
        if not v.get("channel"):
            v["channel"] = v.get("id", "")

    combined = yt_videos + filtered_manual
    combined.sort(key=lambda x: x.get("published", ""), reverse=False)

    return JSONResponse(content={"videos": combined, "total": len(combined)}, media_type="application/json; charset=utf-8")

# ---------------------------
# Serve frontend
# ---------------------------
@app.get("/", include_in_schema=False)
def root():
    return FileResponse("static/index.html")
