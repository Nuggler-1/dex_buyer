import asyncio
import base64
import os
import struct
import time
from typing import Optional
from loguru import logger

from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed, Confirmed
from solana.rpc.types import TokenAccountOpts, TxOpts
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.system_program import CreateAccountWithSeedParams, create_account_with_seed
from solders.transaction import VersionedTransaction
from solders.instruction import AccountMeta, Instruction
from spl.token.async_client import AsyncToken
from spl.token.instructions import (
    CloseAccountParams,
    InitializeAccountParams,
    close_account,
    create_associated_token_account,
    get_associated_token_address,
    initialize_account,
)

from .constants import (
    RAYDIUM_AMM_V4,
    TOKEN_PROGRAM_ID,
    WSOL,
    ACCOUNT_LAYOUT_LEN,
    RAY_AUTHORITY_V4,
    OPENBOOK_PROGRAM,
)
from .pool_keys import AmmV4PoolKeys
from .layouts import LIQUIDITY_STATE_LAYOUT_V4, MARKET_STATE_LAYOUT_V3


class RaydiumClient:
    
    def __init__(
        self,
        rpc_url: str,
        private_key_base58: str,
        compute_unit_limit: int = 400_000,
        compute_unit_price: int = 100_000,
    ):
        self.client = AsyncClient(rpc_url)
        self.keypair = Keypair.from_base58_string(private_key_base58)
        self.pubkey = self.keypair.pubkey()
        self.compute_unit_limit = compute_unit_limit
        self.compute_unit_price = compute_unit_price
        self._pool_keys_cache = {}
        
        logger.info(f"[SOLANA | RAYDIUM] Raydium client initialized | Wallet: {str(self.pubkey)[:8]}...")
    
    async def fetch_pool_keys(self, pair_address: str, use_cache: bool = True) -> Optional[AmmV4PoolKeys]:
        if use_cache and pair_address in self._pool_keys_cache:
            return self._pool_keys_cache[pair_address]
        
        def bytes_of(value):
            if not (0 <= value < 2**64):
                raise ValueError("Value must be in the range of a u64 (0 to 2^64 - 1).")
            return struct.pack('<Q', value)
        
        try:
            amm_id = Pubkey.from_string(pair_address)
            amm_data = (await self.client.get_account_info_json_parsed(amm_id, commitment=Processed)).value.data
            amm_data_decoded = LIQUIDITY_STATE_LAYOUT_V4.parse(amm_data)
            market_id = Pubkey.from_bytes(amm_data_decoded.serumMarket)
            market_info = (await self.client.get_account_info_json_parsed(market_id, commitment=Processed)).value.data
            
            market_decoded = MARKET_STATE_LAYOUT_V3.parse(market_info)
            vault_signer_nonce = market_decoded.vault_signer_nonce
            
            pool_keys = AmmV4PoolKeys(
                amm_id=amm_id,
                base_mint=Pubkey.from_bytes(market_decoded.base_mint),
                quote_mint=Pubkey.from_bytes(market_decoded.quote_mint),
                base_decimals=amm_data_decoded.coinDecimals,
                quote_decimals=amm_data_decoded.pcDecimals,
                open_orders=Pubkey.from_bytes(amm_data_decoded.ammOpenOrders),
                target_orders=Pubkey.from_bytes(amm_data_decoded.ammTargetOrders),
                base_vault=Pubkey.from_bytes(amm_data_decoded.poolCoinTokenAccount),
                quote_vault=Pubkey.from_bytes(amm_data_decoded.poolPcTokenAccount),
                market_id=market_id,
                market_authority=Pubkey.create_program_address(
                    seeds=[bytes(market_id), bytes_of(vault_signer_nonce)], 
                    program_id=OPENBOOK_PROGRAM
                ),
                market_base_vault=Pubkey.from_bytes(market_decoded.base_vault),
                market_quote_vault=Pubkey.from_bytes(market_decoded.quote_vault),
                bids=Pubkey.from_bytes(market_decoded.bids),
                asks=Pubkey.from_bytes(market_decoded.asks),
                event_queue=Pubkey.from_bytes(market_decoded.event_queue),
                ray_authority_v4=RAY_AUTHORITY_V4,
                open_book_program=OPENBOOK_PROGRAM,
                token_program_id=TOKEN_PROGRAM_ID
            )
            if use_cache:
                self._pool_keys_cache[pair_address] = pool_keys
            
            return pool_keys
            
        except Exception as e:
            import traceback
            logger.error(traceback.format_exc())
            logger.error(f"[SOLANA | RAYDIUM] Error fetching pool keys: {e}")
            return None
    
    async def get_token_balances(self, mint: Pubkey) -> Optional[float]:
        try:
            response = await self.client.get_token_accounts_by_owner_json_parsed(
                self.pubkey,
                TokenAccountOpts(mint=mint),
                commitment=Processed
            )
            
            if response.value:
                accounts = response.value
                if accounts:
                    token_amount = accounts[0].account.data.parsed['info']['tokenAmount']
                    return token_amount if token_amount is not None else {}
            return {}
            
        except Exception as e:
            logger.error(f"[SOLANA | RAYDIUM] Error getting token balance: {e}")
            return {}
    
    async def get_reserves(self, pool_keys: AmmV4PoolKeys) -> tuple:
        try:
            balances_response = await self.client.get_multiple_accounts_json_parsed(
                [pool_keys.base_vault, pool_keys.quote_vault],
                Processed
            )
            balances = balances_response.value
            
            base_balance = balances[0].data.parsed['info']['tokenAmount']['uiAmount']
            quote_balance = balances[1].data.parsed['info']['tokenAmount']['uiAmount']
            
            if base_balance is None or quote_balance is None:
                logger.error("[SOLANA | RAYDIUM] One of the reserves is None")
                return None, None, None, None
            
            return base_balance, quote_balance, pool_keys.base_decimals, pool_keys.quote_decimals
            
        except Exception as e:
            import traceback
            logger.error(f"[SOLANA | RAYDIUM] Error getting reserves: {e}")
            logger.error(traceback.format_exc())
            return None, None, None, None
    
    def calculate_amount_out(
        self, 
        amount_in: float, 
        reserve_in: float, 
        reserve_out: float, 
        swap_fee: float = 0.0025
    ) -> float:
        effective_amount_in = amount_in * (1 - swap_fee)
        constant_product = reserve_in * reserve_out
        updated_reserve_in = reserve_in + effective_amount_in
        updated_reserve_out = constant_product / updated_reserve_in
        amount_out = reserve_out - updated_reserve_out
        return amount_out
    
    def _make_swap_instruction(
        self,
        amount_in: int,
        minimum_amount_out: int,
        token_account_in: Pubkey,
        token_account_out: Pubkey,
        pool_keys: AmmV4PoolKeys,
    ) -> Instruction:
        keys = [
            AccountMeta(pubkey=TOKEN_PROGRAM_ID, is_signer=False, is_writable=False),
            AccountMeta(pubkey=pool_keys.amm_id, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.ray_authority_v4, is_signer=False, is_writable=False),
            AccountMeta(pubkey=pool_keys.open_orders, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.target_orders, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.base_vault, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.quote_vault, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.open_book_program, is_signer=False, is_writable=False),
            AccountMeta(pubkey=pool_keys.market_id, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.bids, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.asks, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.event_queue, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.market_base_vault, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.market_quote_vault, is_signer=False, is_writable=True),
            AccountMeta(pubkey=pool_keys.market_authority, is_signer=False, is_writable=False),
            AccountMeta(pubkey=token_account_in, is_signer=False, is_writable=True),
            AccountMeta(pubkey=token_account_out, is_signer=False, is_writable=True),
            AccountMeta(pubkey=self.pubkey, is_signer=True, is_writable=False)
        ]
        
        data = bytearray()
        data.extend(struct.pack('<B', 9))
        data.extend(struct.pack('<Q', amount_in))
        data.extend(struct.pack('<Q', minimum_amount_out))
        
        return Instruction(RAYDIUM_AMM_V4, bytes(data), keys)

    async def create_wsol_account_ixs(self, amount_in_raw:int = 0):
        """
        returns wsol_token_account and list of ixs
        """
        seed = base64.urlsafe_b64encode(os.urandom(24)).decode("utf-8")
        wsol_token_account = Pubkey.create_with_seed(self.pubkey, seed, TOKEN_PROGRAM_ID)
        balance_needed = await AsyncToken.get_min_balance_rent_for_exempt_for_account(self.client)

        create_wsol_account_instruction = create_account_with_seed(
            CreateAccountWithSeedParams(
                from_pubkey=self.pubkey,
                to_pubkey=wsol_token_account,
                base=self.pubkey,
                seed=seed,
                lamports=int(balance_needed + amount_in_raw),
                space=ACCOUNT_LAYOUT_LEN,
                owner=TOKEN_PROGRAM_ID,
            )
        )

        init_wsol_account_instruction = initialize_account(
            InitializeAccountParams(
                program_id=TOKEN_PROGRAM_ID,
                account=wsol_token_account,
                mint=WSOL,
                owner=self.pubkey,
            )
        )

        close_wsol_account_instruction = close_account(
            CloseAccountParams(
                program_id=TOKEN_PROGRAM_ID,
                account=wsol_token_account,
                dest=self.pubkey,
                owner=self.pubkey,
            )
        )
        return wsol_token_account, [create_wsol_account_instruction, init_wsol_account_instruction, close_wsol_account_instruction]
    
    async def get_swap_data_and_price(
        self,
        pair_address: str,
        token_in_mint: str,
        token_out_mint: str,
        cached_blockhash: Optional[str] = None
    ) -> Optional[tuple]:
        """
        Получает все данные для свапа и рассчитывает цену токена.
        
        возвращает:
            tuple: (pool_data, price) 
            или None 
        """
        try:
            # Fetch pool keys
            pool_keys = await self.fetch_pool_keys(pair_address)
            if not pool_keys:
                logger.error("[SOLANA | RAYDIUM] Failed to fetch pool keys")
                return None
            
            token_in_mint_pubkey = Pubkey.from_string(token_in_mint)
            token_out_mint_pubkey = Pubkey.from_string(token_out_mint)
            
            # Determine which token is base/quote
            if token_in_mint_pubkey == pool_keys.base_mint:
                input_decimal = pool_keys.base_decimals
                output_decimal = pool_keys.quote_decimals
                is_base_input = True
            elif token_in_mint_pubkey == pool_keys.quote_mint:
                input_decimal = pool_keys.quote_decimals
                output_decimal = pool_keys.base_decimals
                is_base_input = False
            else:
                logger.error("[SOLANA | RAYDIUM] Input token not in pool")
                return None
            
            # Gather reserves and account data in parallel
            reserves_task = self.get_reserves(pool_keys)
            token_in_task = self.client.get_token_accounts_by_owner(
                self.pubkey, 
                TokenAccountOpts(token_in_mint_pubkey), 
                Processed
            )
            token_out_task = self.client.get_token_accounts_by_owner(
                self.pubkey,
                TokenAccountOpts(token_out_mint_pubkey),
                Processed
            )
            
            (base_reserve, quote_reserve, _, _), token_account_in_check, token_account_out_check = await asyncio.gather(
                reserves_task,
                token_in_task,
                token_out_task
            )
            
            blockhash = cached_blockhash if cached_blockhash else await self.client.get_latest_blockhash()
            
            if base_reserve is None:
                return None
            
            # Calculate price: how many output tokens per 1 input token
            reserve_in = base_reserve if is_base_input else quote_reserve
            reserve_out = quote_reserve if is_base_input else base_reserve
            
            price = reserve_out/reserve_in
            
            # Package pool_data for swap method
            pool_data = (
                pool_keys,
                base_reserve, 
                quote_reserve, 
                token_account_in_check, 
                token_account_out_check,
                blockhash,
                is_base_input,
                input_decimal,
                output_decimal
            )
            
            return (pool_data, price)
            
        except Exception as e:
            import traceback
            logger.error(traceback.format_exc())
            logger.error(f"[SOLANA | RAYDIUM] Error getting swap data and price: {str(e)}")
            return None
    
    async def _confirm_tx(self, tx_sig:str):

        try:
            confirmation = await self.client.confirm_transaction(
                tx_sig,
                commitment=Confirmed
            )
            
            if confirmation.value[0].err is None:
                logger.success(f"[SOLANA | RAYDIUM] Transaction confirmed successfully")
                return str(tx_sig)
            else:
                logger.error(f"[SOLANA | RAYDIUM] Transaction failed: {confirmation.value[0].err}")
                return None
                
        except Exception as e:
            logger.error(f"[SOLANA | RAYDIUM] Confirmation error: {e}")
            return None

    #можно попробовать ускорить на 200-250ms если найти сервис, который вместе с адресом пула выдаст всю остальную дату, чтобы не делать дополнтиельный запрос к RPC 
    async def swap(
        self,
        pair_address: str,
        token_in_mint: str,
        token_out_mint: str,
        amount_in: float,
        slippage: float = 1,
        skip_preflight: bool = True,
        pool_data: Optional[tuple] = None,
        cached_blockhash: Optional[str] = None,
        skip_confirmation: bool = True
    ) -> Optional[str]:
        try:
            
            if pool_data is None:
                if not cached_blockhash:
                    cached_blockhash = await self.client.get_latest_blockhash()
                    cached_blockhash = cached_blockhash.value.blockhash
                result = await self.get_swap_data_and_price(
                    pair_address,
                    token_in_mint,
                    token_out_mint,
                    cached_blockhash
                )
                if result is None:
                    return None
                
                pool_data, _ = result  
            
            (pool_keys, base_reserve, quote_reserve, token_account_in_check, token_account_out_check,
             blockhash, is_base_input, input_decimal, output_decimal) = pool_data
            
            reserve_in = base_reserve if is_base_input else quote_reserve
            reserve_out = quote_reserve if is_base_input else base_reserve
            
            token_in_mint_pubkey = Pubkey.from_string(token_in_mint)
            token_out_mint_pubkey = Pubkey.from_string(token_out_mint)
            
            amount_out = self.calculate_amount_out(amount_in, reserve_in, reserve_out)
            minimum_amount_out = int(amount_out * ((100 - slippage) / 100) * (10 ** output_decimal))
            amount_in_raw = int(amount_in * (10 ** input_decimal))
            
            if token_account_in_check.value and not token_in_mint_pubkey == WSOL:
                token_account_in = token_account_in_check.value[0].pubkey
                create_in_ix = None
            else:
                if token_in_mint_pubkey == WSOL:
                    token_account_in, create_in_ix = await self.create_wsol_account_ixs(amount_in_raw)
                else:
                    token_account_in = get_associated_token_address(self.pubkey, token_in_mint_pubkey)
                    create_in_ix = create_associated_token_account(self.pubkey, self.pubkey, token_in_mint_pubkey)
            
            if token_account_out_check.value and not token_out_mint_pubkey == WSOL:
                token_account_out = token_account_out_check.value[0].pubkey
                create_out_ix = None
            else:
                if token_out_mint_pubkey == WSOL:
                    token_account_out, create_out_ix = await self.create_wsol_account_ixs()
                else:
                    token_account_out = get_associated_token_address(self.pubkey, token_out_mint_pubkey)
                    create_out_ix = create_associated_token_account(self.pubkey, self.pubkey, token_out_mint_pubkey)
               
            swap_ix = self._make_swap_instruction(
                amount_in=amount_in_raw,
                minimum_amount_out=minimum_amount_out,
                token_account_in=token_account_in,
                token_account_out=token_account_out,
                pool_keys=pool_keys,
            )

            instructions_before_swap = [
                set_compute_unit_limit(self.compute_unit_limit),
                set_compute_unit_price(self.compute_unit_price),
            ]
            instructions_after_swap = [
                swap_ix,
            ]

            if create_in_ix:
                if token_in_mint_pubkey == WSOL:
                    instructions_before_swap.append(create_in_ix[0])
                    instructions_before_swap.append(create_in_ix[1])
                    instructions_after_swap.append(create_in_ix[2])
                else:
                    instructions_before_swap.append(create_in_ix)
            
            if create_out_ix:
                if token_out_mint_pubkey == WSOL:
                    instructions_before_swap.append(create_out_ix[0])
                    instructions_before_swap.append(create_out_ix[1])
                    instructions_after_swap.append(create_out_ix[2])
                else:
                    instructions_before_swap.append(create_out_ix)
            
            instructions = instructions_before_swap + instructions_after_swap
            

            compiled_message = MessageV0.try_compile(
                self.pubkey,
                instructions,
                [],
                blockhash,
            )
            
            txn_sig = (await self.client.send_transaction(
                txn=VersionedTransaction(compiled_message, [self.keypair]),
                opts=TxOpts(skip_preflight=skip_preflight),
            )).value

            if skip_confirmation:
                return str(txn_sig)
            
            return await self._confirm_tx(txn_sig)
            
        except Exception as e:
            logger.error(f"[SOLANA | RAYDIUM] Swap error: {e}")
            return None
