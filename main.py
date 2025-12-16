from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv
from io import BytesIO
from datetime import datetime
import pandas as pd
import requests
import os

# ------------------ CONFIG ------------------
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise RuntimeError("Set YOUTUBE_API_KEY in environment variables")

# ------------------ DATABASE (SQLite) ------------------
engine = create_engine(
    "sqlite:///videos.db", connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class ManualVideo(Base):
    __tablename__ = "manual_videos"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    channel = Column(String)
    published = Column(String)
    views = Column(Integer)
    likes = Column(Integer)
    comments = Column(Integer)
    url = Column(String)
    keywords = Column(String)

Base.metadata.create_all(bind=engine)

# ------------------ APP ------------------
app = FastAPI(title="Multi-Source Video Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", include_in_schema=False)
def home():
    return FileResponse("static/index.html")

# ------------------ EXCEL UPLOAD ------------------
@app.post("/upload-excel")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Only Excel files allowed")

    df = pd.read_excel(BytesIO(await file.read())).fillna("")
    session = SessionLocal()
    session.query(ManualVideo).delete()

    for _, r in df.iterrows():
        pub = r.get("published", "")
        if isinstance(pub, (pd.Timestamp, datetime)):
            pub = pub.strftime("%Y-%m-%d")
        else:
            pub = str(pub)[:10]

        session.add(ManualVideo(
            title=str(r.get("title", "")),
            channel=str(r.get("channel", "")),
            published=pub,
            views=int(r.get("views", 0) or 0),
            likes=int(r.get("likes", 0) or 0),
            comments=int(r.get("comments", 0) or 0),
            url=str(r.get("url", "")),
            keywords=str(r.get("keywords", "")).lower()
        ))

    session.commit()
    session.close()
    return {"status": "ok", "count": len(df)}

# ------------------ YOUTUBE HELPERS ------------------
def yt(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

# ------------------ COMBINED API ------------------
@app.get("/combined-videos")
def combined_videos(query: str, start: str, end: str, source: str = "all"):
    videos = []
    q = query.lstrip("#").lower().strip()

    # ---------- YouTube ----------
    if source in ("all", "youtube"):
        search_url = (
            "https://www.googleapis.com/youtube/v3/search"
            f"?part=snippet&type=video&maxResults=25&q={q}"
            f"&publishedAfter={start}T00:00:00Z"
            f"&publishedBefore={end}T23:59:59Z"
            f"&key={YOUTUBE_API_KEY}"
        )
        data = yt(search_url)
        ids = [i["id"]["videoId"] for i in data.get("items", [])]

        if ids:
            info = yt(
                "https://www.googleapis.com/youtube/v3/videos"
                f"?part=snippet,statistics&id={','.join(ids)}&key={YOUTUBE_API_KEY}"
            )
            for v in info.get("items", []):
                s = v["snippet"]
                st = v.get("statistics", {})
                videos.append({
                    "title": s["title"],
                    "channel": s["channelTitle"],
                    "published": s["publishedAt"][:10],
                    "views": int(st.get("viewCount", 0)),
                    "likes": int(st.get("likeCount", 0)),
                    "comments": int(st.get("commentCount", 0)),
                    "url": f"https://youtu.be/{v['id']}",
                    "platform": "YouTube"
                })

    # ---------- Manual (Excel) ----------
    if source in ("all", "manual"):
        session = SessionLocal()
        rows = session.query(ManualVideo).all()
        session.close()

        for r in rows:
            date_ok = not r.published or start <= r.published <= end
            keyword_ok = not q or q in (r.keywords or "")

            if date_ok and keyword_ok:
                videos.append({
                    "title": r.title,
                    "channel": r.channel,
                    "published": r.published,
                    "views": r.views,
                    "likes": r.likes,
                    "comments": r.comments,
                    "url": r.url,
                    "platform": "Manual"
                })

    return {"videos": videos, "total": len(videos)}
