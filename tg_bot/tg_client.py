"""
Telegram notification client using aiogram
Sends messages to Telegram if API credentials are configured, otherwise skips silently
"""
from aiogram import Bot
from loguru import logger
from typing import Optional
from config import TG_BOT_TOKEN, TG_CHAT_ID
import asyncio
from datetime import datetime
import re


def escape_markdown(text: str) -> str:
    """Escape special characters for Telegram Markdown"""
    # Characters that need to be escaped in Markdown
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))


class TelegramClient:

    def __init__(
        self, 
        bot_token: Optional[str] = TG_BOT_TOKEN, 
        chat_id: Optional[str] = TG_CHAT_ID
    ):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = bool(bot_token and chat_id)
        self._status_message_id = None
        self._status_monitor_task = None
        
        if self.enabled:
            self.bot = Bot(token=self.bot_token)
            logger.info("[TG_CLIENT] Telegram notifications enabled")
        else:
            self.bot = None
            logger.info("[TG_CLIENT] Telegram notifications disabled (no API key configured)")
    
    async def close(self):
        if self._status_monitor_task:
            self._status_monitor_task.cancel()
            try:
                await self._status_monitor_task
            except asyncio.CancelledError:
                pass
        
        if self.enabled and self.bot:
            await self.bot.session.close()
            logger.debug("[TG_CLIENT] Bot session closed")
    
    async def send_message(
        self, 
        message: str, 
        parse_mode: str = "Markdown",
        disable_notification: bool = False
    ) -> bool:
        """
        Send a text message to Telegram
        
        Args:
            message: Message text to send
            parse_mode: Parse mode (HTML, Markdown, or None)
            disable_notification: Send silently without notification
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=parse_mode,
                disable_notification=disable_notification
            )
            logger.debug("[TG_CLIENT] Message sent successfully")
            return True
                        
        except Exception as e:
            logger.error(f"[TG_CLIENT] Error sending message: {e}")
            return False
    
    async def send_trade_alert(
        self,
        chain: str,
        token_address: str,
        token_name: str = "TOKEN",
        tx_hash: Optional[str] = None
    ) -> bool:
        """
        Args:
            chain: Blockchain name (e.g., "ETHEREUM", "BSC")
            token_address: Token contract address
            token_name: Token name
            tx_hash: Transaction hash (optional)
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        # Build message
        message = f"🔔 *BUY Alert*\n\n"
        message += f"*Chain:* {chain}\n\n"
        message += f"*Token:* `{token_address}`\n"
        
        if tx_hash:
            message += f"\n*TX:* `{('0x' + tx_hash) if not (tx_hash.startswith('0x') and chain != 'SOLANA') else tx_hash}`"
        
        return await self.send_message(message)
    
    async def send_error_alert(
        self,
        error_type: str,
        error_message: str,
        context: Optional[str] = None
    ) -> bool:
        """
        Send an error alert
        
        Args:
            error_type: Type of error (e.g., "SWAP_FAILED", "RPC_ERROR")
            error_message: Error message
            context: Additional context (optional)
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        # Escape special characters in error messages
        safe_error_type = escape_markdown(error_type)
        safe_error_message = escape_markdown(error_message)
        
        message = f"⚠️ *Error Alert*\n\n"
        message += f"*Type:* {safe_error_type}\n\n"
        message += f"*Message:* {safe_error_message}\n\n"
        
        if context:
            safe_context = escape_markdown(context)
            message += f"*Context:* {safe_context}\n\n"
        
        return await self.send_message(message)
    
    async def tp_task_message(
        self,
        chain: str,
        token_address: str,
        entry_price: float,
        exit_price: float,
        tp_step: int,
        token_name: str = "TOKEN",
        tx_hash: Optional[str] = None
    ) -> bool:
        """
        Send a take profit alert
        
        Args:
            chain: Blockchain name
            token_address: Token contract address
            entry_price: Entry price
            exit_price: Exit price
            tp_step: TP step
            token_name: Token name
            tx_hash: Transaction hash (optional)
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        if tp_step == 0: 
            emoji = "🔴"
            message = f"{emoji} *STOP LOSS*\n\n"
        else:
            emoji = "🟢"
            message = f"{emoji} *TAKE PROFIT*\n\n"

        message += f"*Chain:* {chain}\n\n"
        message += f"*Token:* `{token_address}`\n\n"
        message += f"*Entry:* ${entry_price:.8f}\n\n"
        message += f"*Exit:* ${exit_price:.8f}\n\n"
        message += f"*TP Step:* {tp_step}\n\n"
        
        if tx_hash:
            message += f"\n*TX:* `{('0x' + tx_hash) if not (tx_hash.startswith('0x') and chain != 'SOLANA') else tx_hash}`"
        
        return await self.send_message(message)
    
    async def start_status_monitor(self, chains: list[str]) -> bool:
        """
        Args:
            chains: List of enabled chains
            
        Returns:
            True if started successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("[TG_CLIENT] Telegram notifications disabled (no API key configured)")
            return False
        
        try:
            # Send initial message
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message = "*Trading Bot Started*\n\n"
            message += f"*Active Chains:*\n\n"
            for chain in chains:
                message += f"  • {chain}\n"
            message += f"\n_Monitoring for trading signals_\n"
            message += f"\n*Last Update:* `{current_time}`"
            
            sent_message = await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown"
            )
            self._status_message_id = sent_message.message_id
            logger.info("[TG_CLIENT] Status monitor message sent")
            
            # Start background task to update message every 20 seconds
            self._status_monitor_task = asyncio.create_task(
                self._update_status_loop(chains)
            )
            
            return True
            
        except Exception as e:
            logger.error(f"[TG_CLIENT] Error starting status monitor: {e}")
            return False
    
    async def _update_status_loop(self, chains: list[str]):
        """
        Background task that updates the status message every 20 seconds
        
        Args:
            chains: List of enabled chains
        """

        while True:
            await asyncio.sleep(20)
            
            if not self._status_message_id:
                break
            
            try:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                message = "*Trading Bot Started*\n\n"
                message += f"*Active Chains:*\n\n"
                for chain in chains:
                    message += f"  • {chain}\n"
                message += f"\n_Monitoring for trading signals_\n"
                message += f"\n*Last Update:* `{current_time}`"
                
                await self.bot.edit_message_text(
                    chat_id=self.chat_id,
                    message_id=self._status_message_id,
                    text=message,
                    parse_mode="Markdown"
                )
                #logger.debug("[TG_CLIENT] Status message updated")
                
            except Exception as e:
                logger.error(f"[TG_CLIENT] Error updating status message: {e}")
                