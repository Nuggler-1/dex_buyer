import os
import getpass
from pathlib import Path
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64
from web3 import Web3
from solders.keypair import Keypair
from loguru import logger


class KeyManager:
    """
    Manages encryption, decryption, and loading of private keys for Solana and EVM chains.
    Handles user interaction for password input and key management.
    Uses dynamic salt stored with encrypted data for enhanced security.
    """
    
    CACHE_DIR = "cache_data"
    SOL_PK_FILE = "sol_pk.txt"
    EVM_PK_FILE = "evm_pk.txt"
    SALT_SIZE = 16  # 16 bytes for salt
    
    def __init__(self):
        self.cache_path = Path(self.CACHE_DIR)
        self.sol_pk_path = self.cache_path / self.SOL_PK_FILE
        self.evm_pk_path = self.cache_path / self.EVM_PK_FILE
        self.sol_pk = None
        self.evm_pk = None
        
        # Ensure cache directory exists
        self.cache_path.mkdir(exist_ok=True)
    
    def _derive_key_from_password(self, password: str, salt: bytes) -> bytes:
        """Derive encryption key from password using PBKDF2 with dynamic salt."""
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key
    
    def _encrypt_data(self, data: str, password: str) -> bytes:
        """Encrypt data using password-derived key with dynamic salt."""
        # Generate random salt
        salt = os.urandom(self.SALT_SIZE)
        # Derive key from password and salt
        key = self._derive_key_from_password(password, salt)
        fernet = Fernet(key)
        encrypted = fernet.encrypt(data.encode())
        # Return salt + encrypted data
        return salt + encrypted
    
    def _decrypt_data(self, encrypted_data: bytes, password: str) -> str:
        """Decrypt data using password-derived key with dynamic salt."""
        try:
            # Extract salt from the beginning
            salt = encrypted_data[:self.SALT_SIZE]
            encrypted = encrypted_data[self.SALT_SIZE:]
            # Derive key from password and extracted salt
            key = self._derive_key_from_password(password, salt)
            fernet = Fernet(key)
            decrypted = fernet.decrypt(encrypted)
            return decrypted.decode()
        except Exception as e:
            raise ValueError("Incorrect password or corrupted data") from e
    
    def _get_sol_public_key(self, private_key: str) -> str:
        """Get Solana public key from private key."""
        try:
            keypair = Keypair.from_base58_string(private_key)
            return str(keypair.pubkey())
        except Exception as e:
            raise ValueError(f"Invalid Solana private key: {e}")
    
    def _get_evm_public_key(self, private_key: str) -> str:
        """Get EVM public address from private key."""
        try:
            # Add 0x prefix if not present
            if not private_key.startswith('0x'):
                private_key = '0x' + private_key
            account = Web3().eth.account.from_key(private_key)
            return account.address
        except Exception as e:
            raise ValueError(f"Invalid EVM private key: {e}")
    
    def _file_exists_and_not_empty(self, file_path: Path) -> bool:
        """Check if file exists and is not empty."""
        return file_path.exists() and file_path.stat().st_size > 0
    
    def _load_encrypted_key(self, file_path: Path, password: str) -> str:
        """Load and decrypt private key from file."""
        with open(file_path, 'rb') as f:
            encrypted_data = f.read()
        return self._decrypt_data(encrypted_data, password)
    
    def _save_encrypted_key(self, file_path: Path, private_key: str, password: str):
        """Encrypt and save private key to file."""
        encrypted_data = self._encrypt_data(private_key, password)
        with open(file_path, 'wb') as f:
            f.write(encrypted_data)
    
    def _prompt_for_password(self, prompt_text: str = "Enter password: ") -> str:
        """Prompt user for password input."""
        return getpass.getpass(prompt_text)
    
    def _prompt_for_private_key(self, key_type: str) -> str:
        """Prompt user for private key input."""
        print(f"\n{'='*60}")
        print(f"Enter {key_type} private key:")
        print(f"{'='*60}")
        return getpass.getpass(f"{key_type} private key: ")
    
    def _confirm_proceed(self) -> bool:
        """Ask user if they want to proceed."""
        while True:
            response = input("\nProceed with these keys? (yes/no): ").strip().lower()
            if response in ['yes', 'y']:
                return True
            elif response in ['no', 'n']:
                return False
            else:
                print("Please enter 'yes' or 'no'")
    
    def _display_keys(self, sol_public: str, evm_public: str):
        """Display public keys to user."""
        print("\n" + "="*60)
        print("PUBLIC KEYS:")
        print("="*60)
        print(f"Solana Public Key:  {sol_public}")
        print(f"EVM Public Address: {evm_public}")
        print("="*60)
    
    def initialize_keys(self) -> tuple[str, str]:
        """
        Initialize and load private keys with user interaction.
        Uses single password for both Solana and EVM keys.
        Returns tuple of (sol_private_key, evm_private_key)
        """
        logger.info("Initializing key management...")
        
        sol_exists = self._file_exists_and_not_empty(self.sol_pk_path)
        evm_exists = self._file_exists_and_not_empty(self.evm_pk_path)
        
        # Case 1: Both keys exist - decrypt and verify
        if sol_exists and evm_exists:
            logger.info("Encrypted keys found. Decrypting...")
            
            while True:
                try:
                    password = self._prompt_for_password("Enter decryption password: ")
                    
                    # Decrypt both keys with same password
                    self.sol_pk = self._load_encrypted_key(self.sol_pk_path, password)
                    self.evm_pk = self._load_encrypted_key(self.evm_pk_path, password)
                    
                    # Get public keys
                    sol_public = self._get_sol_public_key(self.sol_pk)
                    evm_public = self._get_evm_public_key(self.evm_pk)
                    
                    # Display and confirm
                    self._display_keys(sol_public, evm_public)
                    
                    if self._confirm_proceed():
                        logger.info("Keys loaded successfully")
                        return self.sol_pk, self.evm_pk
                    else:
                        logger.warning("User declined to proceed. Exiting...")
                        exit(0)
                        
                except ValueError as e:
                    logger.error(f"Decryption failed: {e}")
                    retry = input("Try again? (yes/no): ").strip().lower()
                    if retry not in ['yes', 'y']:
                        logger.warning("User chose not to retry. Exiting...")
                        exit(1)
        
        # Case 2: One or both keys missing - prompt for input
        else:
            logger.info("One or more keys not found. Setting up new keys...")
            
            # If one key exists, we need to decrypt it first to get the password
            existing_password = None
            if sol_exists or evm_exists:
                logger.info("One key exists. Enter password to decrypt existing key...")
                while True:
                    try:
                        existing_password = self._prompt_for_password("Enter password for existing key: ")
                        # Try to decrypt the existing key to verify password
                        if sol_exists:
                            self.sol_pk = self._load_encrypted_key(self.sol_pk_path, existing_password)
                            sol_public = self._get_sol_public_key(self.sol_pk)
                            logger.info("Solana key decrypted successfully")
                        if evm_exists:
                            self.evm_pk = self._load_encrypted_key(self.evm_pk_path, existing_password)
                            evm_public = self._get_evm_public_key(self.evm_pk)
                            logger.info("EVM key decrypted successfully")
                        break
                    except ValueError as e:
                        logger.error(f"Failed to decrypt existing key: {e}")
                        retry = input("Try again? (yes/no): ").strip().lower()
                        if retry not in ['yes', 'y']:
                            logger.warning("User chose not to retry. Exiting...")
                            exit(1)
            
            # Get missing Solana key
            if not sol_exists:
                logger.info("")
                logger.info("Solana key not found")
                while True:
                    try:
                        sol_pk_input = self._prompt_for_private_key("SOLANA")
                        # Validate by getting public key
                        sol_public = self._get_sol_public_key(sol_pk_input)
                        self.sol_pk = sol_pk_input
                        break
                    except ValueError as e:
                        logger.error(f"Invalid Solana key: {e}")
                        retry = input("Try again? (yes/no): ").strip().lower()
                        if retry not in ['yes', 'y']:
                            logger.warning("User chose not to retry. Exiting...")
                            exit(1)
            
            # Get missing EVM key
            if not evm_exists:
                logger.info("EVM key not found")
                while True:
                    try:
                        evm_pk_input = self._prompt_for_private_key("EVM")
                        # Validate by getting public address
                        evm_public = self._get_evm_public_key(evm_pk_input)
                        self.evm_pk = evm_pk_input
                        break
                    except ValueError as e:
                        logger.error(f"Invalid EVM key: {e}")
                        retry = input("Try again? (yes/no): ").strip().lower()
                        if retry not in ['yes', 'y']:
                            logger.warning("User chose not to retry. Exiting...")
                            exit(1)
            
            # Get encryption password (use existing if one key was already encrypted)
            if existing_password:
                password = existing_password
                logger.info("Using existing password for new key encryption")
            else:
                logger.info("Setting up encryption password for both keys...")
                while True:
                    password = self._prompt_for_password("Enter encryption password: ")
                    password_confirm = self._prompt_for_password("Confirm password: ")
                    
                    if password == password_confirm:
                        break
                    else:
                        logger.error("Passwords do not match. Try again.")
            
            # Save encrypted keys (with dynamic salt)
            if not sol_exists:
                self._save_encrypted_key(self.sol_pk_path, self.sol_pk, password)
                logger.info(f"Solana key encrypted and saved to {self.sol_pk_path}")
            
            if not evm_exists:
                self._save_encrypted_key(self.evm_pk_path, self.evm_pk, password)
                logger.info(f"EVM key encrypted and saved to {self.evm_pk_path}")
            
            # Get public keys for display
            sol_public = self._get_sol_public_key(self.sol_pk)
            evm_public = self._get_evm_public_key(self.evm_pk)
            
            # Display and confirm
            self._display_keys(sol_public, evm_public)
            
            if self._confirm_proceed():
                logger.info("Keys setup completed successfully")
                return self.sol_pk, self.evm_pk
            else:
                logger.warning("User declined to proceed. Exiting...")
                exit(0)
