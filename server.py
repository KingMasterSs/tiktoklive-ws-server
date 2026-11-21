import asyncio
import json
import os
import websockets
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent, RoomUserSeqEvent


async def send_json(ws, payload: dict):
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


async def listen_tiktok_room(ws, username: str):
    client = TikTokLiveClient(unique_id=username)
    comment_count = 0
    viewer_count: int | None = None

    async def push_stats():
        await send_json(ws, {
            "type": "stats",
            "username": username,
            "commentCount": comment_count,
            "viewerCount": viewer_count
        })

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} yayınına bağlandı, yorumlar dinleniyor..."
        })
        await push_stats()

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} yayın bağlantısı kapandı."
        })

    @client.on(RoomUserSeqEvent)
    async def on_room_stats(event: RoomUserSeqEvent):
        nonlocal viewer_count
        count = getattr(event, "viewer_count", None)
        if isinstance(count, int):
            viewer_count = count
            await push_stats()

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        nonlocal comment_count
        comment_text = str(getattr(event, "comment", "") or "").strip()
        if not comment_text:
            return

        user_name = (
            getattr(event.user, "unique_id", None)
            or getattr(event.user, "nickname", None)
            or "bilinmeyen"
        )
        comment_count += 1

        await send_json(ws, {
            "type": "chat",
            "user": str(user_name),
            "comment": comment_text
        })
        await push_stats()

    try:
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} canlı yayınına bağlanılıyor..."
        })
        await client.start()
    except asyncio.CancelledError:
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} dinlemesi durduruldu."
        })
        raise
    except Exception as e:
        await send_json(ws, {
            "type": "error",
            "message": f"TikTok bağlantısı başarısız: {str(e)}"
        })
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def ws_handler(websocket):
    current_task: asyncio.Task | None = None

    async def cancel_current_task():
        nonlocal current_task
        if current_task and not current_task.done():
            current_task.cancel()
            try:
                await current_task
            except asyncio.CancelledError:
                pass
        current_task = None

    try:
        async for raw_message in websocket:
            try:
                data = json.loads(raw_message)
            except Exception:
                await send_json(websocket, {
                    "type": "error",
                    "message": "Geçersiz JSON mesajı."
                })
                continue

            action = data.get("action")

            if action == "join":
                username = str(data.get("username", "")).strip().lstrip("@")
                if not username:
                    await send_json(websocket, {
                        "type": "error",
                        "message": "Kullanıcı adı boş olamaz."
                    })
                    continue

                await cancel_current_task()
                current_task = asyncio.create_task(listen_tiktok_room(websocket, username))

            elif action == "stop":
                await cancel_current_task()
                await send_json(websocket, {
                    "type": "status",
                    "message": "Dinleme durduruldu."
                })

            else:
                await send_json(websocket, {
                    "type": "error",
                    "message": f"Bilinmeyen işlem: {action}"
                })
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await cancel_current_task()


async def main():
    port = int(os.environ.get("PORT", "10000"))
    async with websockets.serve(ws_handler, "0.0.0.0", port):
        print(f"TikTokLive WebSocket sunucusu {port} portunda çalışıyor...")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
    
