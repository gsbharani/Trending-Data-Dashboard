from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import requests
import os
import pandas as pd
from typing import Dict, Any
from dotenv import load_dotenv
from pymongo import MongoClient
from io import BytesIO

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

app = FastAPI(title="Multi-Source Trending Data Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------------------------
# Helper functions
# ---------------------------
def yt(url: str) -> Dict[str, Any]:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}


def fix(s: str) -> str:
    if not isinstance(s, str):
        return str(s)
    try:
        return s.encode("latin1").decode("utf-8")
    except:
        return s.replace("\x00", "")  # remove null bytes if any


# ---------------------------
# Upload Excel (secure file upload)
# ---------------------------
@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        raise HTTPException(400, "Only Excel files (.xlsx, .xls) are allowed")

    try:
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        df.columns = df.columns.str.strip().str.lower()
        df = df.fillna("")

        records = df.to_dict(orient="records")
        manual_collection.delete_many({})  # Clear previous manual data
        if records:
            manual_collection.insert_many(records)

        return {"status": "ok", "count": len(records)}
    except Exception as e:
        raise HTTPException(500, f"Error processing file: {str(e)}")


# ---------------------------
# Fetch YouTube videos
# ---------------------------
@app.get("/search-videos")
def search_videos(query: str, start: str, end: str, max_results: int = 50):
    query = query.lstrip("#").strip()
    if not query:
        raise HTTPException(400, "Query cannot be empty")

    video_ids = []
    next_page = ""

    while len(video_ids) < max_results:
        url = (
            f"https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&type=video&maxResults=50&q={query}"
            f"&publishedAfter={start}T00:00:00Z"
            f"&publishedBefore={end}T23:59:59Z"
            f"&pageToken={next_page}&key={API_KEY}"
        )
        data = yt(url)
        if data.get("error"):
            raise HTTPException(502, f"YouTube API error: {data['error']}")

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            video_ids.append(item["id"]["videoId"])
            if len(video_ids) >= max_results:
                break

        if not data.get("nextPageToken"):
            break
        next_page = data["nextPageToken"]

    # Fetch stats in batches
    stats = []
    for i in range(0, len(video_ids), 50):
        batch = ",".join(video_ids[i:i + 50])
        info_url = (
            f"https://www.googleapis.com/youtube/v3/videos"
            f"?part=snippet,statistics&id={batch}&key={API_KEY}"
        )
        info = yt(info_url)
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
                "views": int(st.get("viewCount", 0) or 0),
                "likes": int(st.get("likeCount", 0) or 0),
                "comments": int(st.get("commentCount", 0) or 0),
                "platform": "YouTube",
                "keywords": query.lower()
            })

    return {"videos": stats, "total": len(stats)}


# ---------------------------
# Combined endpoint (fixed source filtering)
# ---------------------------
@app.get("/combined-videos")
def combined_videos(query: str, start: str, end: str, max_results: int = 50, source: str = "all"):
    try:
        yt_videos = []
        if source in ["all", "youtube"]:
            result = search_videos(query, start, end, max_results)
            yt_videos = result.get("videos", [])

        # Fetch and filter manual data
        manual_videos_raw = list(manual_collection.find({}))
        query_lower = query.lower().strip()
        filtered_manual = []

        for v in manual_videos_raw:
            pub_date = str(v.get("published", "") or "").strip()
            if not pub_date or not (start <= pub_date <= end):
                continue

            keywords_str = str(v.get("keywords", "") or "").lower()
            if query_lower and query_lower not in keywords_str:
                continue

            # Ensure required fields
            v.setdefault("title", "Untitled")
            v.setdefault("channel", v.get("id", "Unknown"))
            v.setdefault("url", "#")
            v.setdefault("views", 0)
            v.setdefault("likes", 0)
            v.setdefault("comments", 0)
            v.setdefault("platform", "Manual")
            v["published"] = pub_date

            filtered_manual.append(v)

        # Combine based on source
        if source == "youtube":
            combined = yt_videos
        elif source == "manual":
            combined = filtered_manual
        else:
            combined = yt_videos + filtered_manual

        # Sort newest first
        combined.sort(key=lambda x: x.get("published", "0000-00-00"), reverse=True)

        return {"videos": combined, "total": len(combined)}
    except Exception as e:
        return JSONResponse(
            content={"error": str(e), "videos": [], "total": 0},
            status_code=500
        )


# ---------------------------
# Serve frontend
# ---------------------------
@app.get("/", include_in_schema=False)
def root():
    return FileResponse("static/index.html")
