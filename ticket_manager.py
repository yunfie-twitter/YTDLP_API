#!/usr/bin/env python3
"""
優先チケット生成・管理スクリプト
"""

import asyncio
import secrets
import aiosqlite
from datetime import datetime

async def generate_priority_tickets(count: int = 10):
    """優先チケット生成"""
    print(f"優先チケット {count}枚 を生成します...")

    async with aiosqlite.connect("youtube_downloader.db") as db:
        tickets = []
        for _ in range(count):
            ticket_id = secrets.token_urlsafe(9)[:12].upper()  # 12文字

            await db.execute("""
                INSERT INTO priority_tickets (ticket_id, is_valid)
                VALUES (?, ?)
            """, (ticket_id, True))

            tickets.append(ticket_id)

        await db.commit()

    print("\n=== 生成された優先チケット ===")
    for i, ticket in enumerate(tickets, 1):
        print(f"{i:2d}: {ticket}")

    print(f"\n{count}枚の優先チケットを生成しました!")

async def list_tickets():
    """チケット一覧表示"""
    async with aiosqlite.connect("youtube_downloader.db") as db:
        async with db.execute("""
            SELECT ticket_id, is_valid, used_count, created_at, last_used_at
            FROM priority_tickets
            ORDER BY created_at DESC
        """) as cursor:
            tickets = await cursor.fetchall()

    if not tickets:
        print("登録された優先チケットはありません。")
        return

    print("\n=== 優先チケット一覧 ===")
    print("チケットID    | 有効 | 使用回数 | 作成日時        | 最終使用日時")
    print("-" * 70)

    for ticket_id, is_valid, used_count, created_at, last_used_at in tickets:
        status = "有効" if is_valid else "無効"
        last_used = last_used_at[:19] if last_used_at else "未使用"
        print(f"{ticket_id} | {status:4s} | {used_count:6d} | {created_at[:19]} | {last_used}")

async def main():
    import sys

    if len(sys.argv) < 2:
        print("使用方法:")
        print("  python ticket_manager.py generate [数量]  # チケット生成")
        print("  python ticket_manager.py list             # チケット一覧")
        return

    command = sys.argv[1]

    if command == "generate":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        await generate_priority_tickets(count)
    elif command == "list":
        await list_tickets()
    else:
        print(f"不明なコマンド: {command}")

if __name__ == "__main__":
    asyncio.run(main())
