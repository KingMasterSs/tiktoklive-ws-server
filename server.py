import asyncio
import json
import os
import time
import websockets
from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    CommentEvent,
    ConnectEvent,
    DisconnectEvent,
    GiftEvent,
    JoinEvent,
    LikeEvent,
    RoomUserSeqEvent,
)


def extract_username(user_obj) -> str:
    if not user_obj:
        return "bilinmeyen"
    return (
        getattr(user_obj, "unique_id", None)
        or getattr(user_obj, "nickname", None)
        or "bilinmeyen"
    )


def _find_int_field(obj, names: list[str]) -> int | None:
    if obj is None:
        return None
    for name in names:
        if isinstance(obj, dict) and name in obj:
            val = obj[name]
            if isinstance(val, int) and val >= 0:
                return val
        else:
            val = getattr(obj, name, None)
            if isinstance(val, int) and val >= 0:
                return val
    return None


def extract_viewer_counts_from_room_event(event: RoomUserSeqEvent) -> tuple[int | None, int | None]:
    live_names = [
        "viewer_count",
        "viewerCount",
        "online_user_count",
        "onlineUserCount",
        "current_viewer_count",
    ]
    total_names = [
        "total_user",
        "totalUser",
        "total_viewer_count",
        "totalViewerCount",
    ]

    live_val = _find_int_field(event, live_names)
    total_val = _find_int_field(event, total_names)

    # Bazı sürümlerde alanlar iç içe bir alt nesne altında bulunabilir
    for attr in ("data", "room_user_seq", "payload", "raw"):
        nested = getattr(event, attr, None)
        if nested is not None:
            if live_val is None:
                live_val = _find_int_field(nested, live_names)
            if total_val is None:
                total_val = _find_int_field(nested, total_names)

    if live_val is None and total_val is None:
        try:
            print("RoomUserSeqEvent debug:", getattr(event, "__dict__", str(event)), flush=True)
        except Exception:
            pass

    return live_val, total_val


def extract_live_from_client(client: TikTokLiveClient) -> tuple[int | None, int | None]:
    room_info = getattr(client, "room_info", None)
    live_val = None
    total_val = None

    if isinstance(room_info, dict):
        live_val = _find_int_field(room_info, ["user_count", "viewer_count", "viewerCount"])
        total_val = _find_int_field(room_info, ["total_user", "totalUser"])

        data_obj = room_info.get("data")
        if isinstance(data_obj, dict):
            if live_val is None:
                live_val = _find_int_field(data_obj, ["user_count", "viewer_count", "viewerCount"])
            if total_val is None:
                total_val = _find_int_field(data_obj, ["total_user", "totalUser"])

    return live_val, total_val


async def send_json(ws, payload: dict):
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


async def listen_tiktok_room(ws, username: str):
    client = TikTokLiveClient(unique_id=username)

    comment_count = 0
    gift_count = 0
    like_count = 0
    join_count = 0
    viewer_count: int | None = None
    total_room_viewers: int | None = None
    recent_signatures: dict[str, float] = {}

    def is_duplicate(signature: str, window_sec: float = 2.0) -> bool:
        now = time.monotonic()
        expired = [k for k, ts in recent_signatures.items() if now - ts > window_sec]
        for k in expired:
            del recent_signatures[k]
        if signature in recent_signatures:
            return True
        recent_signatures[signature] = now
        return False

    async def push_stats():
        nonlocal viewer_count, total_room_viewers
        c_live, c_total = extract_live_from_client(client)
        if c_live is not None:
            viewer_count = c_live
        if c_total is not None and total_room_viewers is None:
            total_room_viewers = c_total

        await send_json(ws, {
            "type": "stats",
            "username": username,
            "commentCount": comment_count,
            "giftCount": gift_count,
            "likeCount": like_count,
            "joinCount": join_count,
            "viewerCount": viewer_count,
            "totalViewers": total_room_viewers,
        })

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} yayınına bağlandı • Canlı akış aktif",
        })
        await push_stats()

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} yayın bağlantısı sonlandı",
        })

    @client.on(RoomUserSeqEvent)
    async def on_room_stats(event: RoomUserSeqEvent):
        nonlocal viewer_count, total_room_viewers
        live_val, total_val = extract_viewer_counts_from_room_event(event)
        updated = False
        if live_val is not None:
            viewer_count = live_val
            updated = True
        if total_val is not None:
            total_room_viewers = total_val
            updated = True
        if updated:
            await push_stats()

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        nonlocal comment_count
        comment_text = str(getattr(event, "comment", "") or "").strip()
        if not comment_text:
            return

        user_name = str(extract_username(getattr(event, "user", None)))
        sig = f"chat:{user_name}:{comment_text}"
        if is_duplicate(sig, window_sec=2.5):
            return

        comment_count += 1
        await send_json(ws, {
            "type": "chat",
            "user": user_name,
            "comment": comment_text,
        })
        await push_stats()

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        nonlocal gift_count
        repeat_end = getattr(event, "repeat_end", None)
        streakable = bool(getattr(getattr(event, "gift", None), "streakable", False))
        if streakable and repeat_end is False:
            return

        user_name = str(extract_username(getattr(event, "user", None)))
        repeat_count = getattr(event, "repeat_count", None)
        count = repeat_count if isinstance(repeat_count, int) and repeat_count > 0 else 1

        gift_obj = getattr(event, "gift", None)
        gift_name = str(
            getattr(gift_obj, "name", None)
            or getattr(event, "gift_name", None)
            or "Hediye"
        )
        diamonds = getattr(gift_obj, "diamond_count", None)
        diamond_val = diamonds if isinstance(diamonds, int) and diamonds > 0 else None

        sig = f"gift:{user_name}:{gift_name}:{count}"
        if is_duplicate(sig, window_sec=2.0):
            return

        gift_count += count
        await send_json(ws, {
            "type": "gift",
            "user": user_name,
            "giftName": gift_name,
            "count": count,
            "diamonds": diamond_val,
        })
        await push_stats()

    @client.on(LikeEvent)
    async def on_like(event: LikeEvent):
        nonlocal like_count
        user_name = str(extract_username(getattr(event, "user", None)))
        raw_count = getattr(event, "count", None)
        added = raw_count if isinstance(raw_count, int) and raw_count > 0 else 1

        like_count += added
        total_room_likes = getattr(event, "total_like_count", None)

        await send_json(ws, {
            "type": "like",
            "user": user_name,
            "count": added,
            "totalLikes": total_room_likes if isinstance(total_room_likes, int) else like_count,
        })
        await push_stats()

    @client.on(JoinEvent)
    async def on_join(event: JoinEvent):
        nonlocal join_count
        user_name = str(extract_username(getattr(event, "user", None)))
        sig = f"join:{user_name}"
        if is_duplicate(sig, window_sec=5.0):
            return

        join_count += 1
        await send_json(ws, {
            "type": "join",
            "user": user_name,
        })
        await push_stats()

    try:
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} yayınına bağlanılıyor...",
        })
        await client.start()
    except asyncio.CancelledError:
        await send_json(ws, {
            "type": "status",
            "message": f"@{username} dinlemesi durduruldu",
        })
        raise
    except Exception as e:
        await send_json(ws, {
            "type": "error",
            "message": f"TikTok bağlantısı başarısız: {str(e)}",
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
                    "message": "Geçersiz JSON mesajı.",
                })
                continue

            action = data.get("action")

            if action == "join":
                username = str(data.get("username", "")).strip().lstrip("@")
                if not username:
                    await send_json(websocket, {
                        "type": "error",
                        "message": "Kullanıcı adı boş olamaz.",
                    })
                    continue

                await cancel_current_task()
                current_task = asyncio.create_task(listen_tiktok_room(websocket, username))

            elif action == "stop":
                await cancel_current_task()
                await send_json(websocket, {
                    "type": "status",
                    "message": "Dinleme durduruldu",
                })

            else:
                await send_json(websocket, {
                    "type": "error",
                    "message": f"Bilinmeyen işlem: {action}",
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
