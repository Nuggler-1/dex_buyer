
import asyncio
import ujson  
from picows import ws_connect, WSListener, WSMsgType, WSFrame, WSTransport
from typing import Callable
import traceback
from loguru import logger

class WebSocketClient:
    
    def __init__(self, uri: str, on_message_callback_handler:Callable):
        self.uri = uri
        self.on_message_callback_handler = on_message_callback_handler
        self._connection = None
        
    async def listen(self):
        callback = self.on_message_callback_handler

        #слушатель
        class ClientListener(WSListener):
            def on_ws_connected(self, transport: WSTransport):
                logger.info("[WS_CLIENT] WebSocket connected!")
                
            def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
                if frame.msg_type == WSMsgType.TEXT:
                    try:
                        data = ujson.loads(frame.get_payload_as_ascii_text())
                        asyncio.create_task(callback(data))
                    except Exception as e:
                        logger.error(f"[WS_CLIENT] Error processing frame: {e}")
                        traceback.print_exc()
                elif frame.msg_type == WSMsgType.CLOSE:
                    logger.info("[WS_CLIENT] Received CLOSE frame")
                else:
                    logger.warning(f"[WS_CLIENT] Received frame type: {frame.msg_type}")
        
        #коннектим, picows сама обрабатывает коллбеки на каждом новом сообщении
        transport, client = await ws_connect(ClientListener, self.uri)
        await transport.wait_disconnected() #слушаем