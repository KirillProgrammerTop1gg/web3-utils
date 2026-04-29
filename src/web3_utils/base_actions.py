import json, aiohttp, logging, time, asyncio, base58
from functools import wraps
from InquirerPy import inquirer
from web3 import Web3, AsyncWeb3
from web3.contract import Contract
from web3.types import TxParams
from web3.middleware import ExtraDataToPOAMiddleware
from typing import List, Dict, Literal, Any, Union, Optional, Callable, TypeVar
from aiohttp_socks import ProxyConnector
from .errors import NotEnoughBalanceError
from pathlib import Path
from abc import ABC, abstractmethod
import base64
import httpx
from httpx_socks import SyncProxyTransport
from solana.rpc.api import Client
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders.message import MessageV0

BASE_DIR = Path(__file__).resolve().parent
ERC20_ABI_PATH = BASE_DIR / "erc20.json"

with open(ERC20_ABI_PATH, "r") as f:
    erc20_ABI = json.load(f)

F = TypeVar("F", bound=Callable)


def log_execution_time(func: F) -> F:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            args[0].logger.debug(f"{func.__name__} executed in {elapsed:.3f}s")
            return result
        except Exception:
            elapsed = time.time() - start_time
            args[0].logger.debug(f"{func.__name__} failed after {elapsed:.3f}s")
            raise

    return wrapper


def log_async_execution_time(func: F) -> F:
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = await func(*args, **kwargs)
            elapsed = time.time() - start_time
            args[0].logger.debug(f"{func.__name__} executed in {elapsed:.3f}s")
            return result
        except Exception:
            elapsed = time.time() - start_time
            args[0].logger.debug(f"{func.__name__} failed after {elapsed:.3f}s")
            raise

    return wrapper


class BaseAcc(ABC):
    min_need_value = 0.000001

    def __init__(
        self,
        acc: List[str],
        acc_index: int,
        logger_level_file: int = logging.INFO,
        logger_level_cmd: int = logging.INFO,
        logger_name: Optional[str] = None,
        logger_format: Optional[str] = None,
        logger_filepath: Optional[str] = None,
    ):
        self.address = acc[0]
        self._private_key = acc[1]
        self.acc_index = acc_index
        self.proxy = ""
        if acc[2]:
            ip, port, user, pwd = acc[2].split(":")
            self.proxy = f"socks5h://{user}:{pwd}@{ip}:{port}"
        self.web3 = None
        self.logger = self._setup_default_logger(
            logger_level_file,
            logger_level_cmd,
            logger_name,
            logger_format,
            logger_filepath,
        )
        self.logger.info(
            f"Account initialized {self.address[:6]}...{self.address[-4:]}"
        )
        if self.proxy:
            self.logger.debug(f"Using proxy: {ip}:{port}")

    def _setup_default_logger(
        self,
        file_level: int = logging.INFO,
        cmd_level: int = logging.INFO,
        name: Optional[str] = None,
        format: Optional[str] = None,
        path: Optional[str] = None,
    ) -> logging.Logger:
        logger = logging.getLogger(name if name else f"Account-{self.acc_index}")
        logger.setLevel(file_level)

        handler = logging.StreamHandler()
        handler.setLevel(cmd_level)

        formatter = logging.Formatter(
            (
                format
                if format
                else "%(asctime)s:%(msecs)03d | %(name)s | %(levelname)s | %(message)s"
            ),
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        if path:
            path = Path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    @abstractmethod
    def get_base_balance(
        self, result_type: Literal["wei", "ether"] = "wei"
    ) -> Union[int, str]:
        pass

    @abstractmethod
    async def reconnect_with_other_rpc(self, new_rpc: str) -> None:
        pass

    @abstractmethod
    def check_enough_balance(self, need_value: float) -> bool:
        pass

    @abstractmethod
    def get_gas_params(self) -> Dict[str, int]:
        pass

    @abstractmethod
    def sign_tx_with_pk(self, tx_data: TxParams) -> str:
        pass

    @abstractmethod
    def do_tx_with_data(
        self,
        to_address: str,
        value: float,
        gas_limit: int,
        data: str,
        value_type: Literal["wei", "ether"] = "ether",
    ) -> str:
        pass

    @abstractmethod
    def do_tx_with_ABI(
        self,
        tx_func_abi: Any,
        value: int,
        gas_limit: int,
        value_type: Literal["wei", "ether"] = "ether",
    ) -> str:
        pass

    @abstractmethod
    def get_token_contract(self, token_contract_address: str) -> Contract:
        pass

    @abstractmethod
    def get_token_balance(self, token_contract: Contract) -> int:
        pass

    @abstractmethod
    def transfer_token_to_address(
        self,
        token_contract: Contract,
        to_address: str,
        amount: float,
        amount_type: Literal["wei", "ether"] = "wei",
        gas_limit: int = 150000,
    ) -> None:
        pass


class Acc(BaseAcc):
    def __init__(
        self,
        acc: List[str],
        rpc: str,
        acc_index: int,
        logger_level_file: int = logging.INFO,
        logger_level_cmd: int = logging.INFO,
        logger_name: Optional[str] = None,
        logger_format: Optional[str] = None,
        logger_filepath: Optional[str] = None,
    ) -> None:
        super().__init__(
            acc,
            acc_index,
            logger_level_file,
            logger_level_cmd,
            logger_name,
            logger_format,
            logger_filepath,
        )
        self.logger.debug(f"Connecting to RPC: {rpc}")
        start_time = time.time()
        try:
            self.web3 = Web3(
                Web3.HTTPProvider(
                    rpc,
                    request_kwargs={
                        "proxies": {"http": self.proxy, "https": self.proxy}
                    },
                )
            )
            self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

            if self.web3.is_connected():
                chain_id = self.web3.eth.chain_id
                elapsed = time.time() - start_time
                self.logger.info(
                    f"Successfully connected to network, Chain ID: {chain_id} (took {elapsed:.3f}s)"
                )
            else:
                self.logger.error("Failed to connect to RPC")
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"Error initializing Web3 after {elapsed:.3f}s: {e}")
            raise

    @log_execution_time
    def reconnect_with_other_rpc(self, new_rpc: str) -> None:
        self.logger.debug(f"Connecting to new RPC: {new_rpc}")
        start_time = time.time()
        try:
            self.web3 = Web3(
                Web3.HTTPProvider(
                    new_rpc,
                    request_kwargs={
                        "proxies": {"http": self.proxy, "https": self.proxy}
                    },
                )
            )
            self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

            if self.web3.is_connected():
                chain_id = self.web3.eth.chain_id
                elapsed = time.time() - start_time
                self.logger.info(
                    f"Successfully connected to network, Chain ID: {chain_id} (took {elapsed:.3f}s)"
                )
            else:
                self.logger.error("Failed to connect to RPC")
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"Error initializing Web3 after {elapsed:.3f}s: {e}")
            raise

    @log_execution_time
    def get_base_balance(self, result_type: Literal["wei", "ether"] = "wei"):
        try:
            self.logger.debug(
                f"Requesting balance for {self.address[:6]}...{self.address[-4:]}"
            )
            balance = self.web3.eth.get_balance(self.address)
            result = self.web3.from_wei(balance, result_type)
            self.logger.info(f"Balance: {result} {result_type.upper()}")
            return result
        except Exception as e:
            self.logger.error(f"Error getting balance: {e}")
            raise

    def check_enough_balance(self, need_value: float) -> bool:
        try:
            balance = self.get_base_balance()
            min_balance = need_value + self.web3.to_wei(self.min_need_value, "ether")
            has_enough = balance > min_balance

            if has_enough:
                self.logger.debug(
                    f"Balance check: ✓ Sufficient funds ({balance} wei > {min_balance} wei)"
                )
            else:
                self.logger.warning(
                    f"Balance check: ✗ Insufficient funds (required: {min_balance} wei, available: {balance} wei)"
                )
            return has_enough
        except Exception as e:
            self.logger.error(f"Error checking balance: {e}")
            return False

    @log_execution_time
    def get_gas_params(self) -> Dict[str, int]:
        try:
            block = self.web3.eth.get_block("latest")
            chain_id = self.web3.eth.chain_id

            if "baseFeePerGas" in block:
                base_fee = block["baseFeePerGas"]

                try:
                    suggested_tip = self.web3.eth.max_priority_fee
                except Exception:
                    suggested_tip = self.web3.to_wei(0.01, "gwei")

                min_reasonable_tip = self.web3.to_wei(0.001, "gwei")

                if suggested_tip < min_reasonable_tip:
                    priority_fee = self.web3.to_wei(0.01, "gwei")
                else:
                    priority_fee = suggested_tip

                max_reasonable_tip = max(base_fee // 2, self.web3.to_wei(0.1, "gwei"))
                priority_fee = min(priority_fee, max_reasonable_tip)

                gas_params = {
                    "type": 2,
                    "maxFeePerGas": base_fee + 2 * priority_fee,
                    "maxPriorityFeePerGas": priority_fee,
                }

                self.logger.debug(
                    f"Chain {chain_id}: EIP-1559 | "
                    f"Base: {self.web3.from_wei(base_fee, 'gwei'):.6f} | "
                    f"Priority: {self.web3.from_wei(priority_fee, 'gwei'):.6f} | "
                    f"MaxFee: {self.web3.from_wei(gas_params['maxFeePerGas'], 'gwei'):.6f} gwei"
                )
            else:
                gas_price = self.web3.eth.gas_price
                gas_params = {"gasPrice": gas_price}

                self.logger.debug(
                    f"Chain {chain_id}: Legacy | "
                    f"GasPrice: {self.web3.from_wei(gas_price, 'gwei'):.6f} gwei"
                )

            return gas_params

        except Exception as e:
            self.logger.error(f"Error getting gas params: {e}")
            raise

    @log_execution_time
    def sign_tx_with_pk(self, tx_data: TxParams) -> tuple[str, tuple]:
        try:
            web3, private_key = self.web3, self._private_key
            tx_data.update(self.get_gas_params())

            self.logger.debug(
                f"Signing transaction: nonce={tx_data.get('nonce')}, to={tx_data.get('to')}"
            )
            signed_tx = web3.eth.account.sign_transaction(tx_data, private_key)

            self.logger.info("Sending signed transaction...")
            txn_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            txn_hash_str = f"0x{txn_hash.hex()}"

            self.logger.info(f"✓ Transaction sent: {txn_hash_str}")
            return txn_hash_str, signed_tx
        except Exception as e:
            self.logger.error(f"Error signing/sending transaction: {e}")
            raise

    @log_execution_time
    def do_tx_with_data(
        self,
        to_address: str,
        value: float,
        gas_limit: int,
        data: str,
        value_type: Literal["wei", "ether"] = "ether",
    ) -> tuple[str, tuple]:
        web3, address = self.web3, self.address

        self.logger.info(
            f"Preparing transaction: to={to_address[:6]}...{to_address[-4:]}, "
            f"value={value} {value_type}, gas_limit={gas_limit}"
        )

        try:
            if not self.check_enough_balance(value):
                raise NotEnoughBalanceError("Not enough money to do the tx!")

            tx_data = {
                "data": data,
                "from": address,
                "to": self.web3.to_checksum_address(to_address),
                "value": (
                    value if value_type == "wei" else self.web3.to_wei(value, "ether")
                ),
                "gas": gas_limit,
                "nonce": web3.eth.get_transaction_count(address),
                "chainId": web3.eth.chain_id,
            }

            self.logger.debug(
                f"Transaction data: {data[:20]}..."
                if len(data) > 20
                else f"Transaction data: {data}"
            )
            txn_hash_str, signed_tx = self.sign_tx_with_pk(tx_data)
            # print(f"HEX: {txn_hash_str}")
            return txn_hash_str, signed_tx
        except NotEnoughBalanceError as e:
            self.logger.error(f"Insufficient funds for transaction: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error executing transaction: {e}")
            raise

    @log_execution_time
    def do_tx_with_ABI(
        self,
        tx_func_abi: Any,
        value: int,
        gas_limit: int,
        value_type: Literal["wei", "ether"] = "wei",
    ) -> str:
        web3, address = self.web3, self.address

        self.logger.info(
            f"Preparing transaction via ABI: value={value} {value_type}, gas_limit={gas_limit}"
        )

        try:
            if not self.check_enough_balance(value):
                raise NotEnoughBalanceError("Not enough money to do the tx!")

            tx_data = tx_func_abi.build_transaction(
                {
                    "from": address,
                    "gas": gas_limit,
                    "value": (
                        value
                        if value_type == "wei"
                        else self.web3.to_wei(value, "ether")
                    ),
                    "nonce": web3.eth.get_transaction_count(address),
                    "chainId": web3.eth.chain_id,
                }
            )

            txn_hash_str, signed_tx = self.sign_tx_with_pk(tx_data)
            # print(f"HEX: {txn_hash_str}")
            return txn_hash_str
        except NotEnoughBalanceError as e:
            self.logger.error(f"Insufficient funds for transaction: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error executing transaction via ABI: {e}")
            raise

    @log_execution_time
    def get_token_contract(self, token_contract_address: str) -> Contract:
        try:
            self.logger.debug(f"Getting token contract: {token_contract_address}")
            token_contract = self.web3.eth.contract(
                address=self.web3.to_checksum_address(token_contract_address),
                abi=erc20_ABI,
            )
            self.logger.info(
                f"✓ Token contract loaded: {token_contract_address[:6]}...{token_contract_address[-4:]}"
            )
            return token_contract
        except Exception as e:
            self.logger.error(f"Error getting token contract: {e}")
            raise

    @log_execution_time
    def get_token_balance(self, token_contract: Contract) -> int:
        try:
            self.logger.debug(
                f"Requesting token balance for {self.address[:6]}...{self.address[-4:]}"
            )
            balance = token_contract.functions.balanceOf(
                self.web3.to_checksum_address(self.address)
            ).call()

            try:
                decimals = token_contract.functions.decimals().call()
                readable_balance = balance / (10**decimals)
                self.logger.info(f"Token balance: {readable_balance} ({balance} wei)")
            except:
                self.logger.info(f"Token balance: {balance} wei")

            return balance
        except Exception as e:
            self.logger.error(f"Error getting token balance: {e}")
            raise

    @log_execution_time
    def transfer_token_to_address(
        self,
        token_contract: Contract,
        to_address: str,
        amount: float,
        amount_type: Literal["wei", "ether"] = "wei",
        gas_limit: int = 150000,
    ) -> None:
        try:
            self.logger.info(
                f"Transferring token: to={to_address[:6]}...{to_address[-4:]}, "
                f"amount={amount} {amount_type}, gas_limit={gas_limit}"
            )

            tx_data = token_contract.functions.transfer(
                self.web3.to_checksum_address(to_address),
                amount if amount_type == "wei" else self.web3.to_wei(amount, "ether"),
            )
            return self.do_tx_with_ABI(tx_data, 0, gas_limit)
        except Exception as e:
            self.logger.error(f"Error transferring token: {e}")
            raise


class AsyncAcc(BaseAcc):
    def __init__(
        self,
        acc: List[str],
        rpc: str,
        acc_index: int,
        logger_level_file: int = logging.INFO,
        logger_level_cmd: int = logging.INFO,
        logger_name: Optional[str] = None,
        logger_format: Optional[str] = None,
        logger_filepath: Optional[str] = None,
    ) -> None:
        super().__init__(
            acc,
            acc_index,
            logger_level_file,
            logger_level_cmd,
            logger_name,
            logger_format,
            logger_filepath,
        )
        self.logger.debug(f"Initializing async connection to RPC: {rpc}")

        try:
            if self.proxy:
                connector = ProxyConnector.from_url(self.proxy)
                self.session = aiohttp.ClientSession(connector=connector)
                self.logger.debug("Created session with proxy")
            else:
                self.session = aiohttp.ClientSession()
                self.logger.debug("Created session without proxy")

            self.web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc))
            self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            self.logger.info("Async Web3 provider initialized")
        except Exception as e:
            self.logger.error(f"Error initializing AsyncWeb3: {e}")
            raise

    @log_async_execution_time
    async def connect_session(self):
        try:
            self.logger.debug("Connecting session to Web3 provider...")
            await self.web3.provider.cache_async_session(self.session)

            if await self.web3.is_connected():
                chain_id = await self.web3.eth.chain_id
                self.logger.info(
                    f"✓ Session connected to network, Chain ID: {chain_id}"
                )
            else:
                self.logger.warning(
                    "Session created, but network connection not confirmed"
                )
        except Exception as e:
            self.logger.error(f"Error connecting session: {e}")
            raise

    async def close_connection(self):
        try:
            self.logger.debug("Closing connection...")
            if not self.session.closed:
                await self.session.close()
                self.logger.info("✓ Connection closed")
            else:
                self.logger.debug("Connection was already closed")
        except Exception as e:
            self.logger.error(f"Error closing connection: {e}")

    @log_async_execution_time
    async def get_base_balance(self, result_type: Literal["wei", "ether"] = "wei"):
        try:
            self.logger.debug(
                f"Requesting balance for {self.address[:6]}...{self.address[-4:]}"
            )
            balance = await self.web3.eth.get_balance(self.address)
            result = self.web3.from_wei(balance, result_type)
            self.logger.info(f"Balance: {result} {result_type.upper()}")
            return result
        except Exception as e:
            self.logger.error(f"Error getting balance: {e}")
            raise

    @log_async_execution_time
    async def reconnect_with_other_rpc(self, new_rpc: str) -> None:
        self.logger.debug(f"Connecting to new RPC: {new_rpc}")
        start_time = time.time()
        try:
            self.web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(new_rpc))
            self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            self.connect_session()

            if await self.web3.is_connected():
                chain_id = self.web3.eth.chain_id
                elapsed = time.time() - start_time
                self.logger.info(
                    f"Successfully connected to network, Chain ID: {chain_id} (took {elapsed:.3f}s)"
                )
            else:
                self.logger.error("Failed to connect to RPC")
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(f"Error initializing Web3 after {elapsed:.3f}s: {e}")
            raise

    async def check_enough_balance(self, need_value: float) -> bool:
        try:
            balance = await self.get_base_balance()
            min_balance = need_value + self.web3.to_wei(self.min_need_value, "ether")
            has_enough = balance > min_balance

            if has_enough:
                self.logger.debug(
                    f"Balance check: ✓ Sufficient funds ({balance} wei > {min_balance} wei)"
                )
            else:
                self.logger.warning(
                    f"Balance check: ✗ Insufficient funds (required: {min_balance} wei, available: {balance} wei)"
                )
            return has_enough
        except Exception as e:
            self.logger.error(f"Error checking balance: {e}")
            return False

    @log_execution_time
    async def get_gas_params(self) -> Dict[str, int]:
        try:
            block = await self.web3.eth.get_block("latest")
            chain_id = await self.web3.eth.chain_id

            if "baseFeePerGas" in block:
                base_fee = block["baseFeePerGas"]

                try:
                    suggested_tip = await self.web3.eth.max_priority_fee
                except Exception:
                    suggested_tip = self.web3.to_wei(0.01, "gwei")

                min_reasonable_tip = self.web3.to_wei(0.001, "gwei")

                if suggested_tip < min_reasonable_tip:
                    priority_fee = self.web3.to_wei(0.01, "gwei")
                else:
                    priority_fee = suggested_tip

                max_reasonable_tip = max(base_fee // 2, self.web3.to_wei(0.1, "gwei"))
                priority_fee = min(priority_fee, max_reasonable_tip)

                gas_params = {
                    "type": 2,
                    "maxFeePerGas": base_fee + 2 * priority_fee,
                    "maxPriorityFeePerGas": priority_fee,
                }

                self.logger.debug(
                    f"Chain {chain_id}: EIP-1559 | "
                    f"Base: {self.web3.from_wei(base_fee, 'gwei'):.6f} | "
                    f"Priority: {self.web3.from_wei(priority_fee, 'gwei'):.6f} | "
                    f"MaxFee: {self.web3.from_wei(gas_params['maxFeePerGas'], 'gwei'):.6f} gwei"
                )
            else:
                gas_price = await self.web3.eth.gas_price
                gas_params = {"gasPrice": gas_price}

                self.logger.debug(
                    f"Chain {chain_id}: Legacy | "
                    f"GasPrice: {self.web3.from_wei(gas_price, 'gwei'):.6f} gwei"
                )

            return gas_params

        except Exception as e:
            self.logger.error(f"Error getting gas params: {e}")
            raise

    @log_async_execution_time
    async def sign_tx_with_pk(self, tx_data: TxParams) -> tuple[str, tuple]:
        try:
            web3, private_key = self.web3, self._private_key
            gas_params = await self.get_gas_params()
            tx_data.update(gas_params)

            self.logger.debug(
                f"Signing transaction: nonce={tx_data.get('nonce')}, to={tx_data.get('to')}"
            )
            signed_tx = web3.eth.account.sign_transaction(tx_data, private_key)

            self.logger.info("Sending signed transaction...")
            txn_hash = await web3.eth.send_raw_transaction(signed_tx.raw_transaction)
            txn_hash_str = f"0x{txn_hash.hex()}"

            self.logger.info(f"✓ Transaction sent: {txn_hash_str}")
            return txn_hash_str, signed_tx
        except Exception as e:
            self.logger.error(f"Error signing/sending transaction: {e}")
            raise

    @log_async_execution_time
    async def do_tx_with_data(
        self,
        to_address: str,
        value: float,
        gas_limit: int,
        data: str,
        value_type: Literal["wei", "ether"] = "ether",
    ) -> tuple[str, tuple]:
        web3, address = self.web3, self.address

        self.logger.info(
            f"Preparing transaction: to={f'{to_address[:6]}...{to_address[-4:]}' if to_address else 'Contract creation'}, "
            f"value={value} {value_type}, gas_limit={gas_limit}"
        )

        try:
            if not await self.check_enough_balance(value):
                raise NotEnoughBalanceError("Not enough money to do the tx!")

            tx_data = {
                "data": data,
                "from": address,
                "to": to_address,
                "value": (
                    value if value_type == "wei" else self.web3.to_wei(value, "ether")
                ),
                "gas": gas_limit,
                "nonce": await web3.eth.get_transaction_count(address),
                "chainId": await web3.eth.chain_id,
            }

            self.logger.debug(
                f"Transaction data: {data[:20]}..."
                if len(data) > 20
                else f"Transaction data: {data}"
            )
            txn_hash_str, signed_tx = await self.sign_tx_with_pk(tx_data)
            print(f"HEX: {txn_hash_str}")
            return txn_hash_str, signed_tx
        except NotEnoughBalanceError as e:
            self.logger.error(f"Insufficient funds for transaction: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error executing transaction: {e}")
            raise

    @log_async_execution_time
    async def do_tx_with_ABI(
        self,
        tx_func_abi: Any,
        value: int,
        gas_limit: int,
        value_type: Literal["wei", "ether"] = "ether",
    ) -> str:
        web3, address = self.web3, self.address

        self.logger.info(
            f"Preparing transaction via ABI: value={value} {value_type}, gas_limit={gas_limit}"
        )

        try:
            if not await self.check_enough_balance(value):
                raise NotEnoughBalanceError("Not enough money to do the tx!")

            tx_data = await tx_func_abi.build_transaction(
                {
                    "from": address,
                    "gas": gas_limit,
                    "value": (
                        value
                        if value_type == "wei"
                        else self.web3.to_wei(value, "ether")
                    ),
                    "nonce": await web3.eth.get_transaction_count(address),
                    "chainId": await web3.eth.chain_id,
                }
            )

            txn_hash_str, signed_tx = await self.sign_tx_with_pk(tx_data)
            print(f"HEX: {txn_hash_str}")
            return txn_hash_str
        except NotEnoughBalanceError as e:
            self.logger.error(f"Insufficient funds for transaction: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error executing transaction via ABI: {e}")
            raise

    @log_async_execution_time
    async def get_token_contract(self, token_contract_address: str) -> Contract:
        try:
            self.logger.debug(f"Getting token contract: {token_contract_address}")
            token_contract = self.web3.eth.contract(
                address=self.web3.to_checksum_address(token_contract_address),
                abi=erc20_ABI,
            )
            self.logger.info(
                f"✓ Token contract loaded: {token_contract_address[:6]}...{token_contract_address[-4:]}"
            )
            return token_contract
        except Exception as e:
            self.logger.error(f"Error getting token contract: {e}")
            raise

    @log_async_execution_time
    async def get_token_balance(self, token_contract: Contract) -> int:
        try:
            self.logger.debug(
                f"Requesting token balance for {self.address[:6]}...{self.address[-4:]}"
            )
            balance = await token_contract.functions.balanceOf(
                self.web3.to_checksum_address(self.address)
            ).call()

            try:
                decimals = await token_contract.functions.decimals().call()
                readable_balance = balance / (10**decimals)
                self.logger.info(f"Token balance: {readable_balance} ({balance} wei)")
            except:
                self.logger.info(f"Token balance: {balance} wei")

            return balance
        except Exception as e:
            self.logger.error(f"Error getting token balance: {e}")
            raise

    @log_async_execution_time
    async def transfer_token_to_address(
        self,
        token_contract: Contract,
        to_address: str,
        amount: float,
        amount_type: Literal["wei", "ether"] = "wei",
        gas_limit: int = 150000,
    ) -> None:
        try:
            self.logger.info(
                f"Transferring token: to={to_address[:6]}...{to_address[-4:]}, "
                f"amount={amount} {amount_type}, gas_limit={gas_limit}"
            )

            tx_data = token_contract.functions.transfer(
                self.web3.to_checksum_address(to_address),
                amount if amount_type == "wei" else self.web3.to_wei(amount, "ether"),
            )
            txn_hash = await self.do_tx_with_ABI(tx_data, 0, gas_limit)
            return txn_hash
        except Exception as e:
            self.logger.error(f"Error transferring token: {e}")
            raise

    @log_async_execution_time
    async def approve_contract_on_token(
        self, token_contract: Contract, contract_address: str, gas_limit: int = 100000
    ) -> str:
        try:
            max_approval = 2**256 - 1

            self.logger.info(
                f"Approving token: contract={contract_address[:6]}...{contract_address[-4:]}, "
                f"amount=MAX (2^256-1), gas_limit={gas_limit}"
            )

            tx_data = token_contract.functions.approve(
                self.web3.to_checksum_address(contract_address), max_approval
            )
            txn_hash = await self.do_tx_with_ABI(tx_data, 0, gas_limit)

            self.logger.info(
                f"Token approval successful: txn_hash={txn_hash}, "
                f"contract={contract_address[:6]}...{contract_address[-4:]}"
            )

            return txn_hash
        except Exception as e:
            self.logger.error(f"Error approving token: {e}")
            raise


class SolAcc(BaseAcc):
    def __init__(
        self,
        acc: List[str],
        acc_index: int,
        logger_level_file: int = logging.INFO,
        logger_level_cmd: int = logging.INFO,
        logger_name: Optional[str] = None,
        logger_format: Optional[str] = None,
        logger_filepath: Optional[str] = None,
    ) -> None:
        super().__init__(
            acc,
            acc_index,
            logger_level_file,
            logger_level_cmd,
            logger_name,
            logger_format,
            logger_filepath,
        )
        kp = Keypair.from_bytes(base58.b58decode(self._private_key))
        self.keypair = kp
        self.pubkey = kp.pubkey()
        self.secret = kp.secret()
        self.logger.debug(f"Connecting to Solana RPC")
        start_time = time.time()
        try:
            if self.proxy:
                transport = SyncProxyTransport.from_url(self.proxy)
                httpx_client = httpx.Client(transport=transport)
                self.sol_client = Client(
                    "https://api.mainnet.solana.com",
                    httpx_client=httpx_client,
                )
            else:
                self.sol_client = Client(
                    "https://api.mainnet.solana.com",
                )

            if self.sol_client.is_connected():
                elapsed = time.time() - start_time
                self.logger.info(
                    f"Successfully connected to Solana network (took {elapsed:.3f}s)"
                )
            else:
                self.logger.error("Failed to connect to Solana network")
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(
                f"Error initializing Solana Client after {elapsed:.3f}s: {e}"
            )
            raise

    def reconnect_with_other_rpc(self, new_rpc: str) -> None:
        self.logger.warning(
            "reconnect_with_other is not applicable to Solana. "
            "Solana has only 1 rpc. "
        )
        raise NotImplementedError("Solana has only 1 rpc. " "Don't use this method. ")

    @staticmethod
    def from_lamports(
        lamports: int, type: Literal["lamports", "sol"] = "sol"
    ) -> Union[float, int]:
        return lamports / 10**9 if type == "sol" else lamports

    @staticmethod
    def to_lamports(
        sol_amount: float, type: Literal["lamports", "sol"] = "lamports"
    ) -> Union[float, int]:
        return int(sol_amount * 10**9) if type == "lamports" else sol_amount

    @log_execution_time
    def get_base_balance(self, result_type: Literal["lamports", "sol"] = "lamports"):
        try:
            self.logger.debug(
                f"Requesting balance for {self.address[:6]}...{self.address[-4:]}"
            )
            balance = self.sol_client.get_balance(self.pubkey).value
            result = self.from_lamports(balance, result_type)
            self.logger.info(f"Balance: {result} {result_type.upper()}")
            return result
        except Exception as e:
            self.logger.error(f"Error getting balance: {e}")
            raise

    def check_enough_balance(self, need_value: float) -> bool:
        try:
            balance = self.get_base_balance()
            min_balance = self.to_lamports(need_value) + self.to_lamports(
                self.min_need_value
            )
            has_enough = balance > min_balance

            if has_enough:
                self.logger.debug(
                    f"Balance check: ✓ Sufficient funds ({balance} wei > {min_balance} wei)"
                )
            else:
                self.logger.warning(
                    f"Balance check: ✗ Insufficient funds (required: {min_balance} wei, available: {balance} wei)"
                )
            return has_enough
        except Exception as e:
            self.logger.error(f"Error checking balance: {e}")
            return False

    @log_execution_time
    def do_tx_with_instructions(self, instructions: List) -> str:
        latest_blockhash = self.sol_client.get_latest_blockhash()

        message = MessageV0.try_compile(
            payer=self.pubkey,
            instructions=instructions,
            address_lookup_table_accounts=[],
            recent_blockhash=latest_blockhash.value.blockhash,
        )

        tx = VersionedTransaction(message, [self.keypair])

        signed_tx = self.sol_client.send_transaction(
            tx, opts=TxOpts(skip_preflight=False, preflight_commitment="processed")
        )

        return signed_tx.value

    @log_execution_time
    def get_gas_params(self) -> Dict[str, int]:
        self.logger.warning(
            "get_gas_params is not applicable to Solana. "
            "Solana does not use gas parameters. "
            "Use do_tx_with_instructions instead."
        )
        raise NotImplementedError(
            "Solana transactions must be built from Instructions. "
            "Use do_tx_with_instructions."
        )

    @log_execution_time
    def sign_tx_with_pk(self, tx_data: TxParams) -> str:
        self.logger.warning(
            "sign_tx_with_pk is not applicable to Solana. "
            "Transactions are built from Instructions and signed as a whole. "
            "Use do_tx_with_instructions instead."
        )
        raise NotImplementedError(
            "Build and sign transactions via do_tx_with_instructions."
        )

    @log_execution_time
    def do_tx_with_data(
        self,
        to_address: str,
        value: float,
        gas_limit: int,
        data: str,
        value_type: Literal["lamports", "sol"] = "sol",
    ) -> str:
        self.logger.warning(
            "do_tx_with_data (EVM-style) is not applicable to Solana. "
            "Solana does not support arbitrary calldata or gas limits. "
            "Use do_tx_with_instructions instead."
        )
        raise NotImplementedError(
            "Solana transactions are instruction-based. " "Use do_tx_with_instructions."
        )

    @log_execution_time
    def do_versioned_tx(self, tx_b64: str) -> str:
        try:
            raw_bytes = base64.b64decode(tx_b64)
            tx = VersionedTransaction.from_bytes(raw_bytes)

            msg_bytes = bytes(tx.message)
            signature = self.keypair.sign_message(msg_bytes)
            signed_tx = VersionedTransaction([signature], tx.message)

            result = self.sol_client.send_raw_transaction(
                bytes(signed_tx),
                opts=TxOpts(skip_preflight=False, preflight_commitment="processed"),
            )

            self.logger.info(f"Versioned tx sent: {result.value}")
            return str(result.value), base58.b58encode(bytes(signed_tx)).decode()

        except Exception as e:
            self.logger.error(f"Error sending versioned transaction: {e}")
            raise

    @log_execution_time
    def do_tx_with_ABI(
        self,
        tx_func_abi: Any,
        value: int,
        gas_limit: int,
        value_type: Literal["lamports", "sol"] = "sol",
    ) -> str:
        self.logger.warning(
            "do_tx_with_ABI is not applicable to Solana. "
            "Solana uses IDL and Instructions, not ABI. "
            "Use do_tx_with_instructions instead."
        )
        raise NotImplementedError(
            "Solana is instruction-based (IDL). " "Use do_tx_with_instructions."
        )

    @log_execution_time
    def get_token_contract(self, token_contract_address: str) -> Contract:
        try:
            self.logger.debug(f"Getting token contract: {token_contract_address}")
            token_contract = self.web3.eth.contract(
                address=self.web3.to_checksum_address(token_contract_address),
                abi=erc20_ABI,
            )
            self.logger.info(
                f"✓ Token contract loaded: {token_contract_address[:6]}...{token_contract_address[-4:]}"
            )
            return token_contract
        except Exception as e:
            self.logger.error(f"Error getting token contract: {e}")
            raise

    @log_execution_time
    def get_token_balance(self, token_contract: Contract) -> int:
        try:
            self.logger.debug(
                f"Requesting token balance for {self.address[:6]}...{self.address[-4:]}"
            )
            balance = token_contract.functions.balanceOf(
                self.web3.to_checksum_address(self.address)
            ).call()

            try:
                decimals = token_contract.functions.decimals().call()
                readable_balance = balance / (10**decimals)
                self.logger.info(f"Token balance: {readable_balance} ({balance} wei)")
            except:
                self.logger.info(f"Token balance: {balance} wei")

            return balance
        except Exception as e:
            self.logger.error(f"Error getting token balance: {e}")
            raise

    @log_execution_time
    def transfer_token_to_address(
        self,
        token_contract: Contract,
        to_address: str,
        amount: float,
        amount_type: Literal["wei", "ether"] = "wei",
        gas_limit: int = 150000,
    ) -> None:
        try:
            self.logger.info(
                f"Transferring token: to={to_address[:6]}...{to_address[-4:]}, "
                f"amount={amount} {amount_type}, gas_limit={gas_limit}"
            )

            tx_data = token_contract.functions.transfer(
                self.web3.to_checksum_address(to_address),
                amount if amount_type == "wei" else self.web3.to_wei(amount, "ether"),
            )
            return self.do_tx_with_ABI(tx_data, 0, gas_limit)
        except Exception as e:
            self.logger.error(f"Error transferring token: {e}")
            raise


def select_accs(accs: List[List[str]]) -> List[List[str]]:
    selected_accs = []
    while selected_accs == []:
        selected_accs = inquirer.checkbox(
            message="Choose at least 1 account: ",
            choices=[
                {"name": f"acc-{idx+1}", "value": acc} for idx, acc in enumerate(accs)
            ],
        ).execute()
        print(selected_accs)
    return selected_accs
