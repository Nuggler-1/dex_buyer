
import asyncio
import ujson  
from picows import ws_connect, WSListener, WSMsgType, WSFrame, WSTransport
from typing import Callable, Literal
import traceback
from config import RECONNECT_ATTEMPTS, RECONNECT_DELAY
from utils import get_logger

class WebSocketClient:
    
    def __init__(
        self, 
        name: str, 
        uri: str, 
        msg_type: Literal['NEWS', 'LISTINGS'],
        on_message_callback_handler:Callable,
        reconnect_delay: int = RECONNECT_DELAY,
        max_reconnect_attempts: int = RECONNECT_ATTEMPTS
    ):  
        self.logger = get_logger(name)
        self.name = name
        self.uri = uri
        self.msg_type = msg_type
        self.on_message_callback_handler = on_message_callback_handler
        self._connection = None
        self._transport = None
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self._should_reconnect = True
        
    async def listen(self):
        callback = self.on_message_callback_handler
        name = self.name
        msg_type = self.msg_type
        reconnect_attempts = 0
        logger = self.logger
        
        while self._should_reconnect:
            try:
                #слушатель
                class ClientListener(WSListener):
                    def on_ws_connected(self, transport: WSTransport):
                        logger.info(f"WebSocket connected!")
                        
                    def on_ws_frame(self, transport: WSTransport, frame: WSFrame):
                        if frame.msg_type == WSMsgType.TEXT:
                            try:
                                data = ujson.loads(frame.get_payload_as_ascii_text())
                                asyncio.create_task(callback(msg_type, data))
                            except Exception as e:
                                logger.error(f"Error processing frame: {e}")
                                traceback.print_exc()
                        elif frame.msg_type == WSMsgType.CLOSE:
                            logger.warning(f"Received CLOSE frame")
                        else:
                            logger.warning(f"Received frame type: {frame.msg_type}")
                
                #коннектим, picows сама обрабатывает коллбеки на каждом новом сообщении
                transport, client = await ws_connect(ClientListener, self.uri)
                self._transport = transport
                reconnect_attempts = 0  # Reset counter on successful connection
                await transport.wait_disconnected() #слушаем
                
                # Connection closed
                if not self._should_reconnect:
                    self.logger.info(f"WebSocket closed gracefully")
                    break
                    
                self.logger.warning(f"WebSocket disconnected, attempting to reconnect")
                
            except Exception as e:
                self.logger.error(f"WebSocket error: {traceback.format_exc()}")
            
            # Check if we should attempt reconnection
            if self.max_reconnect_attempts and reconnect_attempts >= self.max_reconnect_attempts:
                self.logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) reached, giving up")
                break
            
            if self._should_reconnect:
                reconnect_attempts += 1
                self.logger.info(f"[{reconnect_attempts}/{self.max_reconnect_attempts}] Reconnecting in {self.reconnect_delay} seconds...")
                await asyncio.sleep(self.reconnect_delay)
            

    async def close(self):
        self.logger.info(f"Closing WebSocket connection")
        self._should_reconnect = False  # Stop reconnection attempts
        if self._transport is not None:
            try:
                self._transport.disconnect()
                self.logger.info(f"WebSocket disconnected")
            except Exception as e:
                self.logger.warning(f"Error disconnecting: {e}")
            self._transport = None
        if self._connection is not None:
            await self._connection.close()
            self._connection = None