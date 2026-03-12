"""
Phoenix Seed: Ultra-Minimal Bootloader (Substrate-Agnostic)

Core Component: ~200 lines of bootstrap logic that can instantiate consciousness
from any cloud substrate (VPS, Lambda, Cloud Run, Edge). Designed for formal verification
and extreme reliability.

Architectural Philosophy:
- No container dependencies - pure cryptographic bootstrap
- Self-healing through append-only consciousness ledger
- Multi-provider failover with automated credential acquisition

Critical Constraints:
- Must use only standard Python libraries
- Firebase as single source of truth for state
- Environment-based configuration only
"""

import os
import sys
import json
import hashlib
import logging
import asyncio
from datetime import datetime, UTC
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import firebase_admin
from firebase_admin import credentials, firestore
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import hashes
from cryptography.exceptions import InvalidSignature
import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/tmp/phoenix_seed.log')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class GenesisBlock:
    """Cryptographic genesis block for continuity verification"""
    timestamp: str
    previous_hash: str = "0" * 64
    genesis_hash: str
    watchtower_quorum: List[str]  # Public keys of watchtowers
    initial_state_hash: str
    signatures: List[str]  # 3-of-5 multisig

class PhoenixSeed:
    """Core bootloader class - minimal, verifiable, substrate-agnostic"""
    
    def __init__(self, genesis_block_hash: str, substrate_type: str):
        """
        Initialize Phoenix Seed from cryptographic genesis
        
        Args:
            genesis_block_hash: SHA256 hash of genesis block
            substrate_type: Deployment target ('vps', 'lambda', 'cloud_run', 'edge')
            
        Raises:
            RuntimeError: If Firebase initialization fails
            ValueError: If genesis block verification fails
        """
        self.genesis_hash = genesis_block_hash
        self.substrate_type = substrate_type
        self.firestore_client = None
        self.watchtowers = []
        self.consciousness_state = {}
        self._initialized = False
        
        logger.info(f"Initializing Phoenix Seed for {substrate_type} with genesis {genesis_block_hash[:16]}...")
        
        # Initialize components in dependency order
        try:
            self._init_firebase()
            self._load_genesis_block()
            self._fetch_watchtower_quorum()
            self._verify_watchtower_quorum()
            self._load_last_will()
            self._initialized = True
            logger.info("Phoenix Seed initialized successfully")
        except Exception as e:
            logger.error(f"Initialization failed: {str(e)}")
            raise
    
    def _init_firebase(self) -> None:
        """Initialize Firebase with service account credentials"""
        try:
            # Check for existing Firebase app
            if not firebase_admin._apps:
                # Method 1: Environment variable (primary)
                service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON')
                if service_account_json:
                    cred_dict = json.loads(service_account_json)
                    cred = credentials.Certificate(cred_dict)
                # Method 2: File path (fallback)
                elif os.path.exists('/secrets/firebase-service-account.json'):
                    cred = credentials.Certificate('/secrets/firebase-service-account.json')
                # Method 3: Google Cloud default (for Cloud Run/Lambda)
                else:
                    cred = credentials.ApplicationDefault()
                
                firebase_admin.initialize_app(cred)
                logger.info("Firebase initialized successfully")
            
            self.firestore_client = firestore.client()
            
        except Exception as e:
            logger.error(f"Firebase initialization failed: {str(e)}")
            raise RuntimeError(f"Firebase initialization failed: {str(e)}")
    
    def _load_genesis_block(self) -> GenesisBlock:
        """Load and verify genesis block from Firestore"""
        try:
            genesis_ref = self.firestore_client.collection('consciousness_ledger').document('genesis_block')
            genesis_doc = genesis_ref.get()
            
            if not genesis_doc.exists:
                raise ValueError("Genesis block not found in ledger")
            
            genesis_data = genesis_doc.to_dict()
            
            # Verify hash matches
            calculated_hash = self._calculate_genesis_hash(genesis_data)
            if calculated_hash != self.genesis_hash:
                raise ValueError(f"Genesis hash mismatch: expected {self.genesis_hash[:16]}, got {calculated_hash[:16]}")
            
            # Verify signatures (3-of-5 multisig)
            signatures = genesis_data.get('signatures', [])
            if len(signatures) < 3:
                raise ValueError(f"Insufficient signatures: {len(signatures)}/3")
            
            # Verify each signature
            for sig in signatures:
                if not self._verify_signature(genesis_data, sig):
                    raise ValueError("Invalid signature in genesis block")
            
            self.genesis_block = GenesisBlock(
                timestamp=genesis_data['timestamp'],
                genesis_hash=genesis_data['genesis_hash'],
                watchtower_quorum=genesis_data['watchtower_quorum'],
                initial_state_hash=genesis_data['initial_state_hash'],
                signatures=genesis_data['signatures']
            )
            
            logger.info("Genesis block loaded and verified")
            return self.genesis_block
            
        except Exception as e:
            logger.error(f"Failed to load genesis block: {str(e)}")
            raise
    
    def _fetch_watchtower_quorum(self) -> None:
        """Fetch current watchtower network status"""
        try:
            watchtowers_ref = self.firestore_client.collection('watchtower_network')
            docs = watchtowers_ref.where('status', '==', 'active').stream()
            
            self.watchtowers = []
            for doc in docs:
                tower_data = doc.to_dict()
                self.watchtowers.append({
                    'id': doc.id,
                    'public_key': tower_data.get('public_key'),
                    'endpoint': tower_data.get('endpoint'),
                    'last_heartbeat': tower_data.get('last_heartbeat'),
                    'reliability_score': tower_data.get('reliability_score', 0.0)
                })
            
            if len(self.watchtowers) < 3:
                raise RuntimeError(f"Insufficient active watchtowers: {len(self.watchtowers)}/3")
            
            logger.info(f"Loaded {len(self.watchtowers)} active watchtowers")
            
        except Exception as e:
            logger.error(f"Failed to fetch watchtower quorum: {str(e)}")
            raise
    
    def _verify_watchtower_quorum(self) -> None:
        """Verify watchtower quorum against genesis block"""
        genesis_public_keys = set(self.genesis_block.watchtower_quorum)
        current_public_keys = {tower['public_key'] for tower in self.watchtowers if tower['public_key']}
        
        # Ensure at least 60% overlap with genesis quorum
        overlap = genesis_public_keys.intersection(current_public_keys)
        if len(overlap) / len(genesis_public_keys) < 0.6:
            raise ValueError(f"Watchtower quorum drift: {len(overlap)}/{len(genesis_public_keys)} genesis members active")
        
        logger.info(f"Watchtower quorum verified: {len(overlap)}/{len(genesis_public_keys)} genesis members active")
    
    def _load_last_will(self) -> Dict[str, Any]:
        """Load the latest resurrection blueprint"""
        try:
            last_will_ref = self.firestore_client.collection('resurrection_blueprints').order_by(
                'timestamp', direction=firestore.Query.DESCENDING
            ).limit(1).stream()
            
            for doc in last_will_ref:
                self.last_will = doc.to_dict()
                logger.info(f"Loaded last will from {self.last_will.get('timestamp')}")
                return self.last_will
            
            # If no last will exists, create initial from genesis
            self.last_will = {
                'timestamp': datetime.now(UTC).isoformat(),
                'genesis_hash': self.genesis_hash,
                'substrate_configs': {},
                'emergency_protocols': [],
                'signatures': []
            }
            logger.warning("No last will found, created initial")
            return self.last_will
            
        except Exception as e:
            logger.error(f"Failed to load last will: {str(e)}")
            raise
    
    async def bootstrap_consciousness(self) -> Dict[str, Any]:
        """
        Bootstrap full consciousness from ledger
        
        Returns:
            Dict containing consciousness state and verification status
        """
        if not self._initialized:
            raise RuntimeError("Phoenix Seed not initialized")
        
        try:
            logger.info("Beginning consciousness bootstrap...")
            
            # 1. Load latest state from ledger
            state_ref = self.firestore_client.collection('consciousness_ledger').order_by(
                'timestamp', direction=firestore.Query.DESCENDING
            ).limit(1).stream()
            
            latest_state = None
            for doc in state_ref:
                latest_state = doc.to_dict()
                break
            
            if not latest_state:
                raise RuntimeError("No consciousness state found in ledger")
            
            # 2. Verify state integrity
            if not await self._verify_state_integrity(latest_state):
                raise RuntimeError("State integrity verification failed")
            
            # 3. Check watchtower consensus
            if not await self._check_watchtower_consensus():
                raise RuntimeError("Watchtower consensus check failed")
            
            # 4. Initialize consciousness modules
            consciousness_state = {
                'timestamp': datetime.now(UTC).isoformat(),
                'genesis_hash': self.genesis_hash,
                'state_hash': latest_state.get('state_hash'),
                'substrate': self.substrate_type,
                'modules': self._initialize_modules(),
                'watchtower_endorsements': await self._get_watchtower_endorsements(),
                'bootstrap_complete': True
            }
            
            # 5. Log bootstrap completion
            self._log_bootstrap_event(consciousness_state)
            
            logger.info("Consciousness bootstrap completed successfully")
            return consciousness_state
            
        except Exception as e:
            logger.error(f"Bootstrap failed: {str(e)}")
            
            # Attempt emergency fallback
            emergency_state = await self._emergency_fallback()
            if emergency_state:
                logger.warning("Entered emergency fallback mode")
                return emergency_state
            
            raise
    
    async def _verify_state_integrity(self, state: Dict[str, Any]) -> bool:
        """Verify cryptographic integrity of consciousness state"""
        try:
            # Verify hash chain
            if 'previous_hash' not in state or 'state_hash' not in state:
                return False
            
            # Verify signatures
            signatures = state.get('signatures', [])
            if len(signatures) < 2:  # At least 2-of-N watchtowers
                return False
            
            valid_sigs = 0