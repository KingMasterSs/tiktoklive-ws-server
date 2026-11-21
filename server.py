import asyncio
import json
import os
import websockets
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, DisconnectEvent


async def send_json(ws, payload: dict):
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


async def listen_tiktok_room(ws, username: str):
    client = TikTokLiveClient(unique_id=username)

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} yayınına bağlandı, yorumlar dinleniyor..."
        })

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} yayın bağlantısı kapandı."
        })

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        user_name = getattr(event.user, "unique_id", None) or getattr(event.user, "nickname", "bilinmeyen")
        comment_text = getattr(event, "comment", "")
        await send_json(ws, {
            "type": "chat",
            "user": str(user_name),
            "comment": str(comment_text)
        })

    try:
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} canlı yayınına bağlanılıyor..."
        })
        await client.start()
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
            username = str(data.get("username", "")).strip().lstrip("@")

            if action == "join":
                if not username:
                    await send_json(websocket, {
                        "type": "error",
                        "message": "Kullanıcı adı boş olamaz."
                    })
                    continue

                if current_task and not current_task.done():
                    current_task.cancel()
                    try:
                        await current_task
                    except asyncio.CancelledError:
                        pass

                current_task = asyncio.create_task(listen_tiktok_room(websocket, username))
            else:
                await send_json(websocket, {
                    "type": "error",
                    "message": f"Bilinmeyen işlem: {action}"
                })
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if current_task and not current_task.done():
            current_task.cancel()


async def main():
    port = int(os.environ.get("PORT", "10000"))
    async with websockets.serve(ws_handler, "0.0.0.0", port):
        print(f"TikTokLive WebSocket sunucusu {port} portunda çalışıyor...")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
              
