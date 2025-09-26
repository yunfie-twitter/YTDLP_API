
"""
YouTube動画ダウンロードAPI - メインサーバー
必須ライブラリ: yt-dlp, FastAPI, ffmpeg, sqlite3, logging
"""

import asyncio
import json
import secrets
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging as logger  # loggerモジュールとして使用

# FastAPI関連
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request, Depends
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

# 非同期処理関連
import aiosqlite
import aiofiles
import yt_dlp
import ffmpeg

# 設定とユーティリティ
from pathlib import Path
import tempfile
import os
import subprocess
import time

# === 設定 ===
class Config:
    DATABASE_PATH = "youtube_downloader.db"
    DOWNLOAD_DIR = Path("downloads")
    TEMP_DIR = Path("temp")
    LOG_FILE = "youtube_downloader.log"
    MAX_CONCURRENT_DOWNLOADS = 5
    TEMP_URL_EXPIRE_HOURS = 1
    CLEANUP_INTERVAL_MINUTES = 30

# ディレクトリ作成
Config.DOWNLOAD_DIR.mkdir(exist_ok=True)
Config.TEMP_DIR.mkdir(exist_ok=True)

# === ロガー設定 ===
def setup_logger():
    """loggerモジュールを使用した詳細ログ設定"""
    # ログフォーマット
    formatter = logger.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )

    # ファイルハンドラー
    file_handler = logger.FileHandler(Config.LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logger.DEBUG)
    file_handler.setFormatter(formatter)

    # コンソールハンドラー
    console_handler = logger.StreamHandler()
    console_handler.setLevel(logger.INFO)
    console_handler.setFormatter(formatter)

    # メインロガー
    main_logger = logger.getLogger('youtube_downloader')
    main_logger.setLevel(logger.DEBUG)
    main_logger.addHandler(file_handler)
    main_logger.addHandler(console_handler)

    return main_logger

# グローバルロガー
app_logger = setup_logger()

# === データモデル ===
class DownloadRequest(BaseModel):
    url: str = Field(..., description="YouTube動画URL")
    format_type: str = Field(..., description="フォーマット種別 (audio/video)")
    quality: Optional[str] = Field(None, description="画質・音質指定")
    video_codec: Optional[str] = Field(None, description="映像コーデック")
    audio_codec: Optional[str] = Field(None, description="音声コーデック")
    video_bitrate: Optional[str] = Field(None, description="映像ビットレート")
    audio_bitrate: Optional[str] = Field(None, description="音声ビットレート")
    embed_thumbnail: bool = Field(False, description="サムネイル埋め込み")
    priority_ticket: Optional[str] = Field(None, description="優先チケット")

class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: float
    message: Optional[str] = None
    download_url: Optional[str] = None
    file_size: Optional[int] = None
    estimated_time: Optional[int] = None

# === 優先度付きキュー管理 ===
class PriorityQueueManager:
    def __init__(self):
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.running_jobs: Dict[str, asyncio.Task] = {}
        self.job_priorities: Dict[str, int] = {}

    async def add_job(self, job_id: str, job_data: Dict, has_priority: bool = False):
        """ジョブをキューに追加"""
        if has_priority:
            priority = 0  # 最高優先度
            app_logger.info(f"優先チケットジョブを追加: {job_id}")
        else:
            # 動画時間とコーデック重さで自動ソート
            priority = await self._calculate_priority(job_data)
            app_logger.info(f"通常ジョブを追加: {job_id}, 優先度: {priority}")

        self.job_priorities[job_id] = priority
        await self.queue.put((priority, job_id, job_data))

    async def _calculate_priority(self, job_data: Dict) -> int:
        """動画時間とコーデックの重さで優先度計算"""
        try:
            # yt-dlpで動画情報取得
            ydl_opts = {'quiet': True, 'no_warnings': True}
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(job_data['url'], download=False)
                duration = info.get('duration', 300)  # デフォルト5分

            # コーデック重さ判定
            codec_weight = self._get_codec_weight(
                job_data.get('video_codec', 'h264'),
                job_data.get('audio_codec', 'aac')
            )

            # 優先度 = 動画秒数 + コーデック重さ（小さいほど優先）
            priority = int(duration) + codec_weight
            return priority

        except Exception as e:
            app_logger.error(f"優先度計算エラー: {e}")
            return 1000  # エラー時は低優先度

    def _get_codec_weight(self, video_codec: str, audio_codec: str) -> int:
        """コーデック重さ計算"""
        video_weights = {
            'h264': 10, 'avc1': 10,
            'vp9': 50,
            'av01': 100,  # AV1は重い
            'hevc': 30, 'h265': 30
        }
        audio_weights = {
            'aac': 5, 'm4a': 5,
            'mp3': 3,
            'opus': 8,
            'flac': 15,
            'wav': 20
        }

        v_weight = video_weights.get(video_codec.lower(), 20)
        a_weight = audio_weights.get(audio_codec.lower(), 10)

        return v_weight + a_weight

# === データベース管理 ===
class DatabaseManager:
    def __init__(self):
        self.db_path = Config.DATABASE_PATH

    async def initialize(self):
        """データベース初期化"""
        async with aiosqlite.connect(self.db_path) as db:
            # ジョブテーブル
            await db.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT UNIQUE NOT NULL,
                    ip_address TEXT NOT NULL,
                    url TEXT NOT NULL,
                    format_type TEXT NOT NULL,
                    video_quality TEXT,
                    audio_quality TEXT,
                    video_codec TEXT,
                    audio_codec TEXT,
                    has_priority_ticket BOOLEAN DEFAULT 0,
                    priority_ticket TEXT,
                    status TEXT DEFAULT 'queued',
                    progress REAL DEFAULT 0,
                    file_path TEXT,
                    file_size INTEGER,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """)

            # 一時URLテーブル
            await db.execute("""
                CREATE TABLE IF NOT EXISTS temp_urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    temp_token TEXT UNIQUE NOT NULL,
                    job_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    downloaded_count INTEGER DEFAULT 0,
                    FOREIGN KEY (job_id) REFERENCES jobs (job_id)
                )
            """)

            # 優先チケットテーブル
            await db.execute("""
                CREATE TABLE IF NOT EXISTS priority_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT UNIQUE NOT NULL,
                    is_valid BOOLEAN DEFAULT 1,
                    used_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TIMESTAMP
                )
            """)

            await db.commit()
            app_logger.info("データベース初期化完了")

    async def create_job(self, job_data: Dict) -> str:
        """ジョブ作成"""
        job_id = secrets.token_urlsafe(16)

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO jobs (
                    job_id, ip_address, url, format_type, video_quality,
                    audio_quality, video_codec, audio_codec, has_priority_ticket,
                    priority_ticket
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_id, job_data['ip_address'], job_data['url'],
                job_data['format_type'], job_data.get('video_quality'),
                job_data.get('audio_quality'), job_data.get('video_codec'),
                job_data.get('audio_codec'), job_data.get('has_priority_ticket', False),
                job_data.get('priority_ticket')
            ))
            await db.commit()

        app_logger.info(f"ジョブ作成完了: {job_id}")
        return job_id

    async def update_job_status(self, job_id: str, status: str, progress: float = None, 
                              file_path: str = None, file_size: int = None, 
                              error_message: str = None):
        """ジョブステータス更新"""
        async with aiosqlite.connect(self.db_path) as db:
            update_fields = ["status = ?"]
            values = [status]

            if progress is not None:
                update_fields.append("progress = ?")
                values.append(progress)
            if file_path:
                update_fields.append("file_path = ?")
                values.append(file_path)
            if file_size:
                update_fields.append("file_size = ?")
                values.append(file_size)
            if error_message:
                update_fields.append("error_message = ?")
                values.append(error_message)

            if status == 'processing':
                update_fields.append("started_at = ?")
                values.append(datetime.now().isoformat())
            elif status == 'completed':
                update_fields.append("completed_at = ?")
                values.append(datetime.now().isoformat())

            values.append(job_id)

            query = f"UPDATE jobs SET {', '.join(update_fields)} WHERE job_id = ?"
            await db.execute(query, values)
            await db.commit()

        app_logger.info(f"ジョブステータス更新: {job_id} -> {status}")

# グローバルインスタンス
queue_manager = PriorityQueueManager()
db_manager = DatabaseManager()

print("メインAPIコード（前半）を作成しました")

# === ダウンロード処理クラス ===
class DownloadProcessor:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_DOWNLOADS)

    async def process_download(self, job_id: str, job_data: Dict):
        """非同期ダウンロード処理"""
        async with self.semaphore:
            try:
                app_logger.info(f"ダウンロード開始: {job_id}")
                await db_manager.update_job_status(job_id, 'processing', progress=0)

                # yt-dlp設定構築
                ydl_opts = await self._build_ydl_options(job_data)

                # プログレス追跡用のhook
                def progress_hook(d):
                    if d['status'] == 'downloading':
                        percentage = d.get('_percent_str', '0%').replace('%', '')
                        try:
                            progress = float(percentage)
                            asyncio.create_task(
                                db_manager.update_job_status(job_id, 'processing', progress=progress)
                            )
                        except:
                            pass

                ydl_opts['progress_hooks'] = [progress_hook]

                # ダウンロード実行
                output_path = await self._download_with_ytdlp(job_data['url'], ydl_opts, job_id)

                # サムネイル埋め込み処理
                if job_data.get('embed_thumbnail') and job_data['format_type'] == 'audio':
                    output_path = await self._embed_thumbnail(output_path, job_data['url'])

                # ファイル情報取得
                file_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0

                # 完了状態更新
                await db_manager.update_job_status(
                    job_id, 'completed', progress=100, 
                    file_path=str(output_path), file_size=file_size
                )

                # 一時URL生成
                await self._create_temp_url(job_id, output_path)

                app_logger.info(f"ダウンロード完了: {job_id}, ファイル: {output_path}")

            except Exception as e:
                error_msg = str(e)
                app_logger.error(f"ダウンロードエラー: {job_id}, エラー: {error_msg}")
                await db_manager.update_job_status(job_id, 'failed', error_message=error_msg)

    async def _build_ydl_options(self, job_data: Dict) -> Dict:
        """yt-dlp オプション構築"""
        # 基本設定
        ydl_opts = {
            'outtmpl': str(Config.DOWNLOAD_DIR / '%(title)s.%(ext)s'),
            'quiet': False,
            'no_warnings': False
        }

        # フォーマット設定
        if job_data['format_type'] == 'audio':
            # 音声フォーマット
            format_selector = self._build_audio_format(job_data)
            ydl_opts.update({
                'format': format_selector,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': job_data.get('audio_codec', 'mp3'),
                    'preferredquality': job_data.get('audio_bitrate', '192')
                }]
            })
        else:
            # 動画フォーマット
            format_selector = self._build_video_format(job_data)
            ydl_opts['format'] = format_selector

        return ydl_opts

    def _build_audio_format(self, job_data: Dict) -> str:
        """音声フォーマット選択文字列構築"""
        quality = job_data.get('quality', 'best')
        codec = job_data.get('audio_codec', 'aac')

        if quality == 'best':
            return 'bestaudio/best'
        else:
            # 特定品質指定
            return f'bestaudio[acodec*={codec}]/bestaudio/best'

    def _build_video_format(self, job_data: Dict) -> str:
        """動画フォーマット選択文字列構築"""
        quality = job_data.get('quality', '1080p')
        video_codec = job_data.get('video_codec', 'h264')
        audio_codec = job_data.get('audio_codec', 'aac')

        # 4K制限チェック
        has_priority = job_data.get('has_priority_ticket', False)

        format_parts = []

        # 画質フィルタ
        if quality == '4K':
            if has_priority:
                format_parts.append('[height<=2160]')
            else:
                # 通常ユーザーは4KはWebM限定
                format_parts.append('[height<=2160][ext=webm]')
        elif quality.endswith('p'):
            height = quality[:-1]
            format_parts.append(f'[height<={height}]')

        # コーデックフィルタ
        if video_codec:
            format_parts.append(f'[vcodec*={video_codec}]')
        if audio_codec:
            format_parts.append(f'[acodec*={audio_codec}]')

        # フォーマット選択文字列構築
        filter_str = ''.join(format_parts)

        if quality == '4K' and not has_priority:
            # 4K WebM専用
            return f'bestvideo{filter_str}+bestaudio/best{filter_str}'
        else:
            return f'bestvideo{filter_str}+bestaudio[acodec*={audio_codec}]/best'

    async def _download_with_ytdlp(self, url: str, ydl_opts: Dict, job_id: str) -> str:
        """yt-dlpでダウンロード実行"""
        try:
            # 非同期実行のためにexecutorを使用
            loop = asyncio.get_event_loop()

            def sync_download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    filename = ydl.prepare_filename(info)
                    return filename

            output_path = await loop.run_in_executor(None, sync_download)
            return output_path

        except Exception as e:
            raise Exception(f"yt-dlpダウンロードエラー: {str(e)}")

    async def _embed_thumbnail(self, audio_path: str, video_url: str) -> str:
        """音声ファイルにサムネイル埋め込み"""
        try:
            app_logger.info(f"サムネイル埋め込み開始: {audio_path}")

            # サムネイル画像ダウンロード
            ydl_opts = {
                'skip_download': True,
                'writeinfojson': True,
                'writethumbnail': True,
                'outtmpl': str(Config.TEMP_DIR / 'thumb_%(id)s.%(ext)s')
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                thumbnail_url = info.get('thumbnail')

            if not thumbnail_url:
                app_logger.warning("サムネイルURL取得失敗")
                return audio_path

            # FFmpegでサムネイル埋め込み
            output_path = audio_path.replace('.', '_with_thumb.')

            # 非同期実行
            def embed_sync():
                subprocess.run([
                    'ffmpeg', '-i', audio_path, '-i', thumbnail_url,
                    '-c', 'copy', '-map', '0', '-map', '1',
                    '-disposition:v:0', 'attached_pic',
                    '-y', output_path
                ], check=True, capture_output=True)

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, embed_sync)

            # 元ファイル削除
            os.remove(audio_path)

            app_logger.info(f"サムネイル埋め込み完了: {output_path}")
            return output_path

        except Exception as e:
            app_logger.error(f"サムネイル埋め込みエラー: {str(e)}")
            return audio_path  # エラー時は元ファイルを返す

    async def _create_temp_url(self, job_id: str, file_path: str):
        """一時URLトークン生成"""
        temp_token = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(hours=Config.TEMP_URL_EXPIRE_HOURS)

        async with aiosqlite.connect(db_manager.db_path) as db:
            await db.execute("""
                INSERT INTO temp_urls (temp_token, job_id, file_path, expires_at)
                VALUES (?, ?, ?, ?)
            """, (temp_token, job_id, file_path, expires_at.isoformat()))
            await db.commit()

        app_logger.info(f"一時URL生成: {temp_token}, 期限: {expires_at}")

# グローバルダウンロード処理インスタンス
download_processor = DownloadProcessor()

print("ダウンロード処理クラスを作成しました")


# === FastAPIアプリケーション ===
app = FastAPI(
    title="YouTube動画ダウンロードAPI",
    description="yt-dlp, FastAPI, SQLite3を使用した高機能動画ダウンロードAPI",
    version="1.0.0"
)

# === ユーティリティ関数 ===
async def get_client_ip(request: Request) -> str:
    """クライアントIP取得"""
    forwarded_ip = request.headers.get("X-Forwarded-For")
    if forwarded_ip:
        return forwarded_ip.split(",")[0].strip()
    return request.client.host

async def validate_priority_ticket(ticket: str) -> bool:
    """優先チケット検証"""
    if not ticket or len(ticket) != 12:
        return False

    async with aiosqlite.connect(db_manager.db_path) as db:
        async with db.execute(
            "SELECT is_valid FROM priority_tickets WHERE ticket_id = ?",
            (ticket,)
        ) as cursor:
            result = await cursor.fetchone()
            return result and result[0]

# === APIエンドポイント ===

@app.on_event("startup")
async def startup_event():
    """起動時処理"""
    app_logger.info("YouTube動画ダウンロードAPI起動開始")
    await db_manager.initialize()

    # クリーンアップサービス開始
    asyncio.create_task(cleanup_service())

    # ワーカープロセス開始
    asyncio.create_task(download_worker())

    app_logger.info("API起動完了")

@app.post("/download", response_model=Dict[str, str])
async def create_download(
    request: DownloadRequest, 
    client_request: Request,
    background_tasks: BackgroundTasks
):
    """ダウンロードリクエスト作成"""
    try:
        client_ip = await get_client_ip(client_request)
        app_logger.info(f"ダウンロード要求: IP={client_ip}, URL={request.url}")

        # 優先チケット検証
        has_priority = False
        if request.priority_ticket:
            has_priority = await validate_priority_ticket(request.priority_ticket)
            if has_priority:
                app_logger.info(f"優先チケット有効: {request.priority_ticket}")
            else:
                app_logger.warning(f"無効な優先チケット: {request.priority_ticket}")

        # ジョブデータ準備
        job_data = {
            'ip_address': client_ip,
            'url': request.url,
            'format_type': request.format_type,
            'video_quality': request.quality,
            'audio_quality': request.quality,
            'video_codec': request.video_codec,
            'audio_codec': request.audio_codec,
            'video_bitrate': request.video_bitrate,
            'audio_bitrate': request.audio_bitrate,
            'embed_thumbnail': request.embed_thumbnail,
            'has_priority_ticket': has_priority,
            'priority_ticket': request.priority_ticket if has_priority else None
        }

        # ジョブ作成
        job_id = await db_manager.create_job(job_data)

        # キューに追加
        await queue_manager.add_job(job_id, job_data, has_priority)

        return {
            "job_id": job_id,
            "status": "queued",
            "message": "ダウンロードジョブをキューに追加しました"
        }

    except Exception as e:
        app_logger.error(f"ダウンロード要求処理エラー: {str(e)}")
        raise HTTPException(status_code=500, detail=f"処理エラー: {str(e)}")

@app.get("/status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    """ジョブステータス取得"""
    try:
        async with aiosqlite.connect(db_manager.db_path) as db:
            async with db.execute("""
                SELECT j.status, j.progress, j.error_message, j.file_size,
                       t.temp_token
                FROM jobs j
                LEFT JOIN temp_urls t ON j.job_id = t.job_id
                WHERE j.job_id = ?
            """, (job_id,)) as cursor:
                result = await cursor.fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="ジョブが見つかりません")

        status, progress, error_message, file_size, temp_token = result

        job_status = JobStatus(
            job_id=job_id,
            status=status,
            progress=progress or 0,
            message=error_message,
            file_size=file_size
        )

        # 完了時はダウンロードURL追加
        if status == 'completed' and temp_token:
            job_status.download_url = f"/download/{temp_token}"

        return job_status

    except HTTPException:
        raise
    except Exception as e:
        app_logger.error(f"ステータス取得エラー: {str(e)}")
        raise HTTPException(status_code=500, detail="ステータス取得エラー")

@app.get("/download/{temp_token}")
async def download_file(temp_token: str, background_tasks: BackgroundTasks):
    """一時URLでファイルダウンロード"""
    try:
        async with aiosqlite.connect(db_manager.db_path) as db:
            async with db.execute("""
                SELECT file_path, expires_at, downloaded_count
                FROM temp_urls 
                WHERE temp_token = ?
            """, (temp_token,)) as cursor:
                result = await cursor.fetchone()

        if not result:
            app_logger.warning(f"無効なダウンロードトークン: {temp_token}")
            raise HTTPException(status_code=404, detail="ダウンロードリンクが見つかりません")

        file_path, expires_at_str, downloaded_count = result
        expires_at = datetime.fromisoformat(expires_at_str)

        # 期限チェック
        if datetime.now() > expires_at:
            app_logger.info(f"期限切れトークン削除: {temp_token}")
            # 期限切れファイルとレコード削除
            background_tasks.add_task(cleanup_expired_file, temp_token, file_path)
            raise HTTPException(status_code=410, detail="ダウンロードリンクの有効期限が切れています")

        # ファイル存在チェック
        if not os.path.exists(file_path):
            app_logger.error(f"ファイルが見つかりません: {file_path}")
            raise HTTPException(status_code=404, detail="ファイルが見つかりません")

        # ダウンロード回数更新
        async with aiosqlite.connect(db_manager.db_path) as db:
            await db.execute(
                "UPDATE temp_urls SET downloaded_count = downloaded_count + 1 WHERE temp_token = ?",
                (temp_token,)
            )
            await db.commit()

        app_logger.info(f"ファイルダウンロード: {temp_token}, パス: {file_path}")

        # ファイル名取得
        filename = os.path.basename(file_path)

        return FileResponse(
            path=file_path,
            filename=filename,
            media_type='application/octet-stream'
        )

    except HTTPException:
        raise
    except Exception as e:
        app_logger.error(f"ファイルダウンロードエラー: {str(e)}")
        raise HTTPException(status_code=500, detail="ダウンロードエラー")

@app.get("/formats")
async def get_supported_formats():
    """対応フォーマット一覧"""
    return {
        "audio_formats": ["MP3", "M4A", "Opus", "WAV"],
        "video_formats": ["MP4", "WebM"],
        "video_qualities": ["360p", "480p", "540p", "720p", "1080p", "4K"],
        "video_codecs": ["H.264", "VP9", "AV1", "HEVC"],
        "audio_codecs": ["AAC", "MP3", "Opus", "FLAC"]
    }

@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    try:
        # DB接続確認
        async with aiosqlite.connect(db_manager.db_path) as db:
            await db.execute("SELECT 1")

        # キュー状況
        queue_size = queue_manager.queue.qsize()
        running_jobs = len(queue_manager.running_jobs)

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "queue_size": queue_size,
            "running_jobs": running_jobs,
            "max_concurrent": Config.MAX_CONCURRENT_DOWNLOADS
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

print("FastAPIエンドポイントを作成しました")


# === バックグラウンドサービス ===

async def download_worker():
    """ダウンロードワーカー（非同期処理）"""
    app_logger.info("ダウンロードワーカー開始")

    while True:
        try:
            # キューからジョブ取得（優先度順）
            priority, job_id, job_data = await queue_manager.queue.get()

            app_logger.info(f"ジョブ処理開始: {job_id}, 優先度: {priority}")

            # ジョブをrunning_jobsに追加
            task = asyncio.create_task(
                download_processor.process_download(job_id, job_data)
            )
            queue_manager.running_jobs[job_id] = task

            # ジョブ完了まで待機
            try:
                await task
            except Exception as e:
                app_logger.error(f"ジョブ処理エラー: {job_id}, エラー: {str(e)}")
            finally:
                # running_jobsから削除
                if job_id in queue_manager.running_jobs:
                    del queue_manager.running_jobs[job_id]

                # キューからタスク完了通知
                queue_manager.queue.task_done()

        except Exception as e:
            app_logger.error(f"ワーカーエラー: {str(e)}")
            await asyncio.sleep(5)  # エラー時は5秒待機

async def cleanup_service():
    """期限切れファイル自動削除サービス"""
    app_logger.info("クリーンアップサービス開始")

    while True:
        try:
            now = datetime.now()
            app_logger.info("期限切れファイルクリーンアップ実行")

            async with aiosqlite.connect(db_manager.db_path) as db:
                # 期限切れレコード取得
                async with db.execute("""
                    SELECT temp_token, file_path FROM temp_urls 
                    WHERE expires_at < ?
                """, (now.isoformat(),)) as cursor:
                    expired_records = await cursor.fetchall()

                cleaned_count = 0
                for temp_token, file_path in expired_records:
                    try:
                        # ファイル削除
                        if os.path.exists(file_path):
                            os.remove(file_path)
                            app_logger.info(f"期限切れファイル削除: {file_path}")

                        # DB レコード削除
                        await db.execute(
                            "DELETE FROM temp_urls WHERE temp_token = ?",
                            (temp_token,)
                        )
                        cleaned_count += 1

                    except Exception as e:
                        app_logger.error(f"ファイル削除エラー: {file_path}, エラー: {str(e)}")

                if cleaned_count > 0:
                    await db.commit()
                    app_logger.info(f"クリーンアップ完了: {cleaned_count}件削除")
                else:
                    app_logger.info("期限切れファイルなし")

        except Exception as e:
            app_logger.error(f"クリーンアップエラー: {str(e)}")

        # 次回実行まで待機
        await asyncio.sleep(Config.CLEANUP_INTERVAL_MINUTES * 60)

async def cleanup_expired_file(temp_token: str, file_path: str):
    """個別ファイル削除（BackgroundTask用）"""
    try:
        # ファイル削除
        if os.path.exists(file_path):
            os.remove(file_path)
            app_logger.info(f"期限切れファイル削除: {file_path}")

        # DBレコード削除
        async with aiosqlite.connect(db_manager.db_path) as db:
            await db.execute(
                "DELETE FROM temp_urls WHERE temp_token = ?",
                (temp_token,)
            )
            await db.commit()

    except Exception as e:
        app_logger.error(f"期限切れファイル削除エラー: {file_path}, エラー: {str(e)}")

# === メイン実行部分 ===
if __name__ == "__main__":
    import uvicorn

    app_logger.info("=== YouTube動画ダウンロードAPI 起動 ===")
    app_logger.info(f"ダウンロード先: {Config.DOWNLOAD_DIR}")
    app_logger.info(f"一時ファイル: {Config.TEMP_DIR}")
    app_logger.info(f"データベース: {Config.DATABASE_PATH}")
    app_logger.info(f"最大同時ダウンロード数: {Config.MAX_CONCURRENT_DOWNLOADS}")

    # サーバー起動
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
