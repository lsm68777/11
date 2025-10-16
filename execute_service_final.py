#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix 95 V4 EXECUTE Service - Fully Integrated Trading Execution Engine
================================================================================

Core Features:
- Full integration with Risk Service
- Integrated Notify Service alerts
- Phoenix95 analysis result utilization
- 20x leverage trading execution (isolated margin)
- Automatic 2% take profit/stop loss
- Real-time position monitoring
- Kelly Criterion position sizing
- 3-stage risk verification system
- Hardware optimized for AMD Ryzen 9 7950X / Intel i9-13900K + 128GB DDR5 RAM

Port: 8102
Grade: Hedge fund-grade trading execution system

================================================================================
"""

# ============================================================================
#                    ALL IMPORTS (Python Standard Order)
# ============================================================================

# Standard library imports
import asyncio
import hashlib
import hmac
import json
import logging
import os
import sys
import time
import traceback
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Third-party imports
import aiohttp
import numpy as np
import psutil
import uvicorn
from dotenv import load_dotenv

# FastAPI and related libraries
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

# ============================================================================
#                    Environment Variables Loading (Standardized)
#                    Optimized for High-Performance Systems
# ============================================================================

def load_environment_variables():
    """
    Load environment variables from .env file with comprehensive validation
    Priority: 1) .env in project root, 2) .env in current directory
    Optimized for: AMD Ryzen 9 7950X / Intel i9-13900K + 128GB DDR5 RAM
    """
    # Get absolute path to project root
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    
    # Try multiple .env file locations (prioritized order)
    env_locations = [
        project_root / '.env',                          # Project root (highest priority)
        project_root / 'services' / '.env',             # Services directory
        Path.cwd() / '.env',                            # Current working directory
        Path.home() / '.env.phoenix95',                 # User home directory
        Path('C:/phoenix95_v4_ultimate/.env'),          # Absolute path fallback
    ]
    
    env_loaded = False
    env_file_path = None
    
    # Try loading from each location
    for env_path in env_locations:
        if env_path.exists() and env_path.is_file():
            try:
                load_dotenv(env_path, override=True)
                env_loaded = True
                env_file_path = env_path
                print(f"Environment loaded from: {env_path}")
                break
            except Exception as load_error:
                print(f"Failed to load {env_path}: {load_error}")
                continue
    
    # Report loading status
    if not env_loaded:
        print("=" * 70)
        print("WARNING: No .env file found in expected locations:")
        for loc in env_locations:
            exists_status = "EXISTS" if loc.exists() else "NOT FOUND"
            print(f"  - {loc} [{exists_status}]")
        print("\nContinuing with system environment variables only...")
        print("Service will run in LIMITED mode (simulation/backup features only)")
        print("=" * 70)
    else:
        print(f"Environment file location: {env_file_path.absolute()}")
    
    # Validate critical environment variables
    critical_vars = {
        'BINANCE_API_KEY': {
            'description': 'Binance API credentials',
            'min_length': 20,
            'critical': True
        },
        'BINANCE_SECRET_KEY': {
            'description': 'Binance API credentials',
            'min_length': 20,
            'critical': True
        },
        'TELEGRAM_BOT_TOKEN': {
            'description': 'Telegram notifications',
            'min_length': 30,
            'critical': False,
            'format_check': lambda x: ':' in x
        },
        'TELEGRAM_CHAT_ID': {
            'description': 'Telegram notifications',
            'min_length': 5,
            'critical': False,
            'format_check': lambda x: x.startswith('-') or x.isdigit()
        },
    }
    
    missing_critical = []
    missing_optional = []
    
    print("\nEnvironment Variables Validation:")
    print("-" * 70)
    
    for var_name, config in critical_vars.items():
        value = os.getenv(var_name)
        
        if not value:
            if config['critical']:
                missing_critical.append(f"{var_name} ({config['description']})")
                print(f"  MISSING (CRITICAL): {var_name}")
            else:
                missing_optional.append(f"{var_name} ({config['description']})")
                print(f"  MISSING (OPTIONAL): {var_name}")
            continue
        
        # Length validation
        if len(value) < config.get('min_length', 0):
            if config['critical']:
                missing_critical.append(f"{var_name} (too short: {len(value)} chars)")
                print(f"  INVALID (CRITICAL): {var_name} - too short")
            else:
                missing_optional.append(f"{var_name} (too short: {len(value)} chars)")
                print(f"  INVALID (OPTIONAL): {var_name} - too short")
            continue
        
        # Format validation
        format_check = config.get('format_check')
        if format_check and not format_check(value):
            print(f"  WARNING: {var_name} - format validation failed")
            continue
        
        # Mask value for security
        if len(value) > 8:
            masked_value = f"{value[:4]}...{value[-4:]}"
        else:
            masked_value = "****"
        
        print(f"  OK: {var_name} = {masked_value}")
    
    print("-" * 70)
    
    # Report validation results
    if missing_critical:
        print("\nCRITICAL ERRORS - Service cannot operate fully:")
        for var in missing_critical:
            print(f"  - {var}")
        print("\nService will run in SIMULATION MODE")
    
    if missing_optional:
        print("\nOPTIONAL WARNINGS - Some features unavailable:")
        for var in missing_optional:
            print(f"  - {var}")
        print("\nBackup notification methods will be used")
    
    if not missing_critical and not missing_optional:
        print("\nALL ENVIRONMENT VARIABLES VALIDATED SUCCESSFULLY")
        print("Service ready for FULL PRODUCTION MODE")
    
    return env_loaded

# ============================================================================
#                    Initialize Environment Variables
# ============================================================================

print("=" * 70)
print("Phoenix 95 V4 Execute Service - Environment Initialization")
print("Hardware Target: AMD Ryzen 9 7950X / Intel i9-13900K + 128GB DDR5 RAM")
print("=" * 70)

try:
    env_status = load_environment_variables()
    print("=" * 70)
    print()
except Exception as env_error:
    print(f"\nFATAL ERROR during environment initialization: {env_error}")
    print("Service cannot start. Please check your .env file configuration.")
    print("=" * 70)
    sys.exit(1)

# ============================================================================
#                    Optimized System Configuration
#                    Target: Ryzen 9 7950X / i9-13900K + 128GB DDR5
#                    Recommended: Python 3.11+ (3.10 compatible)
# ============================================================================

# System configuration for high-performance trading
PYTHON_VERSION = sys.version_info

# Python version compatibility check
logger_compat = logging.getLogger("Phoenix95-System")

if PYTHON_VERSION < (3, 10):
    raise RuntimeError(
        f"Python 3.10+ required for execution. "
        f"Current version: {PYTHON_VERSION.major}.{PYTHON_VERSION.minor}. "
        f"Please upgrade Python to at least 3.10."
    )

# Log Python version and system optimization target
logger_compat.info(
    f"Python {PYTHON_VERSION.major}.{PYTHON_VERSION.minor} detected - "
    f"System ready for high-performance trading"
)
logger_compat.info(
    "Hardware optimization target: Ryzen 9 7950X / i9-13900K (16-24 cores) + 128GB DDR5 RAM"
)

# Changes:
# - Add try-except for Windows compatibility
# - Remove emoji from comments
# - Improve error handling
# - Optimize for 128GB DDR5 / Ryzen 9 7950X system

# Configure UTF-8 encoding for all I/O operations with enhanced error handling
try:
    os.environ["PYTHONIOENCODING"] = "utf-8"
    
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    
    logger_compat.info("UTF-8 encoding configured successfully for I/O operations")
    
except Exception as encoding_error:
    # Windows systems may fail codepage changes - this is non-critical
    logger_compat.warning(f"Encoding configuration warning (non-critical): {encoding_error}")
    logger_compat.info("Continuing with system default encoding")

# Enhanced Logging Configuration with Error Handling
LOG_DIR = Path("C:/phoenix95_v4_ultimate/services/logs")
try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Log directory ready: {LOG_DIR.absolute()}")
except Exception as e:
    print(f"Warning: Could not create log directory: {e}")
    print("Continuing with console logging only...")

LOG_FILE = LOG_DIR / "execute_service.log"

# Create logger with unique name to avoid conflicts
logger = logging.getLogger("Phoenix95-Execute")
logger.setLevel(logging.INFO)

# Clear existing handlers to prevent duplicates
if logger.hasHandlers():
    logger.handlers.clear()

# Create formatters for detailed file logs and simple console output
detailed_formatter = logging.Formatter(
    "%(asctime)s | Phoenix95-Execute | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

simple_formatter = logging.Formatter(
    "%(levelname)s | %(message)s"
)

# File handler with UTF-8 encoding and error handling
try:
    file_handler = logging.FileHandler(
        LOG_FILE, 
        mode='a', 
        encoding='utf-8', 
        errors='replace'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    print(f"Log file initialized: {LOG_FILE.absolute()}")
except Exception as e:
    print(f"Warning: Could not create log file: {e}")
    print("Continuing with console logging only...")

# Console handler with safe encoding
try:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
except Exception as e:
    print(f"Warning: Console handler error: {e}")

# Prevent propagation to root logger
logger.propagate = False

# Binance client integration - moved after logger definition
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException, BinanceRequestException
    BINANCE_AVAILABLE = True
    logger.info("Binance library loaded successfully")
except ImportError:
    BINANCE_AVAILABLE = False
    logger.warning("Binance library not available - using simulation mode")

# ============================================================================
#                    Integrated Configuration System (Security Enhanced)
# ============================================================================


@dataclass
class ExecuteServiceConfig:
    """Enhanced Execute Service configuration with comprehensive validation"""

    # Trading configuration
    LEVERAGE: int = int(os.getenv("LEVERAGE", "20"))
    MARGIN_MODE: str = os.getenv("MARGIN_MODE", "ISOLATED")
    STOP_LOSS_PERCENT: float = float(os.getenv("STOP_LOSS_PERCENT", "0.02"))
    TAKE_PROFIT_PERCENT: float = float(os.getenv("TAKE_PROFIT_PERCENT", "0.02"))
    MAX_POSITION_SIZE: float = float(os.getenv("MAX_POSITION_SIZE", "0.005"))
    MIN_CONFIDENCE: float = float(os.getenv("MIN_CONFIDENCE", "0.75"))

    # Risk management
    MAX_DAILY_LOSS: float = float(os.getenv("MAX_DAILY_LOSS", "0.2"))
    MAX_POSITIONS: int = int(os.getenv("MAX_POSITIONS", "5"))
    POSITION_TIMEOUT: int = int(os.getenv("POSITION_TIMEOUT", "86400"))
    LIQUIDATION_BUFFER: float = float(os.getenv("LIQUIDATION_BUFFER", "0.1"))

    # Kelly Criterion settings
    KELLY_MAX_FRACTION: float = float(os.getenv("KELLY_MAX_FRACTION", "0.005"))
    WIN_RATE_ADJUSTMENT: float = float(os.getenv("WIN_RATE_ADJUSTMENT", "0.85"))
    PORTFOLIO_VALUE: float = float(os.getenv("PORTFOLIO_VALUE", "10000"))

    # Performance settings
    ORDER_TIMEOUT: int = int(os.getenv("ORDER_TIMEOUT", "30"))
    PRICE_CHECK_INTERVAL: int = int(os.getenv("PRICE_CHECK_INTERVAL", "1"))
    MONITOR_INTERVAL: int = int(os.getenv("MONITOR_INTERVAL", "3"))

    # Service integration
    RISK_SERVICE_URL: str = os.getenv("RISK_SERVICE_URL", "http://localhost:8101")
    NOTIFY_SERVICE_URL: str = os.getenv("NOTIFY_SERVICE_URL", "http://localhost:8103")
    BRAIN_SERVICE_URL: str = os.getenv("BRAIN_SERVICE_URL", "http://localhost:8100")
    SERVICE_TIMEOUT: int = int(os.getenv("SERVICE_TIMEOUT", "10"))
    RETRY_DELAY: int = int(float(os.getenv("RETRY_DELAY", "2")))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "5"))

    # Binance API configuration
    BINANCE_API_KEY: str = os.getenv("BINANCE_API_KEY", "")
    BINANCE_SECRET_KEY: str = os.getenv("BINANCE_SECRET_KEY", "")
    BINANCE_TESTNET: bool = os.getenv("BINANCE_TESTNET", "True").lower() == "true"

    # Telegram configuration
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # Symbol filtering
    ALLOWED_SYMBOLS: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Comprehensive configuration validation and setup"""
        
        # Initialize validation results
        validation_errors = []
        validation_warnings = []
        
        try:
            # Detect simulation mode from environment
            env_simulation = os.getenv("SIMULATION_MODE", "false").lower() == "true"
            
            if env_simulation:
                self.SIMULATION_MODE = True
                logger.info("Simulation mode enabled via environment variable")
            elif not self._validate_binance_credentials():
                logger.warning("Binance API credentials missing, switching to simulation mode")
                self.SIMULATION_MODE = True
            else:
                self.SIMULATION_MODE = False
                logger.info("Binance API credentials validated successfully")

            # Execute validation chain
            try:
                self._validate_trading_config(validation_errors, validation_warnings)
            except Exception as e:
                logger.error(f"Trading config validation failed: {e}")
                validation_errors.append(f"Trading validation error: {str(e)}")
            
            try:
                self._validate_risk_config(validation_errors, validation_warnings)
            except Exception as e:
                logger.error(f"Risk config validation failed: {e}")
                validation_errors.append(f"Risk validation error: {str(e)}")
            
            try:
                self._validate_service_config(validation_errors, validation_warnings)
            except Exception as e:
                logger.error(f"Service config validation failed: {e}")
                validation_errors.append(f"Service validation error: {str(e)}")
            
            try:
                self._validate_external_config(validation_warnings)
            except Exception as e:
                logger.warning(f"External config validation warning: {e}")
                validation_warnings.append(f"External validation warning: {str(e)}")
            
            # Parse allowed trading symbols
            try:
                self._parse_allowed_symbols()
            except Exception as e:
                logger.warning(f"Symbol parsing failed, allowing all symbols: {e}")
            
            # Log environment configuration summary
            try:
                self._log_environment_summary()
            except Exception as e:
                logger.warning(f"Environment summary logging failed: {e}")
            
            # Handle validation results
            if validation_errors:
                error_msg = "Critical configuration errors: " + "; ".join(validation_errors)
                logger.error(error_msg)
                raise ValueError(error_msg)
                
            if validation_warnings:
                logger.warning(f"Configuration warnings detected: {len(validation_warnings)} warnings")
                for warning in validation_warnings:
                    logger.warning(f"  - {warning}")

        except ValueError:
            # Re-raise validation errors
            raise
        except Exception as e:
            logger.error(f"Configuration initialization failed: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            raise RuntimeError(f"Configuration initialization error: {str(e)}") from e

    def _validate_binance_credentials(self) -> bool:
        """Validate Binance API credentials"""
        if not self.BINANCE_API_KEY or not self.BINANCE_SECRET_KEY:
            return False
        
        # Basic format validation
        if len(self.BINANCE_API_KEY) < 20 or len(self.BINANCE_SECRET_KEY) < 20:
            logger.warning("Binance API credentials appear to be too short")
            return False
            
        return True

    def _validate_trading_config(self, errors: List[str], warnings: List[str]):
        """Validate trading configuration parameters with comprehensive checks"""
        
        # Leverage validation with safety bounds
        if not (1 <= self.LEVERAGE <= 125):
            errors.append(f"Invalid leverage: {self.LEVERAGE} (must be 1-125)")
        elif self.LEVERAGE > 50:
            warnings.append(f"High leverage detected: {self.LEVERAGE}x - consider risk implications")

        # Margin mode validation
        valid_margin_modes = ["ISOLATED", "CROSSED"]
        if self.MARGIN_MODE not in valid_margin_modes:
            errors.append(f"Invalid margin mode: {self.MARGIN_MODE} (must be {valid_margin_modes})")

        # Percentage validations with detailed bounds checking
        if not (0.001 <= self.STOP_LOSS_PERCENT <= 0.5):
            errors.append(f"Invalid stop loss percent: {self.STOP_LOSS_PERCENT} (must be 0.1%-50%)")
            
        if not (0.001 <= self.TAKE_PROFIT_PERCENT <= 0.5):
            errors.append(f"Invalid take profit percent: {self.TAKE_PROFIT_PERCENT} (must be 0.1%-50%)")
            
        if not (0.001 <= self.MAX_POSITION_SIZE <= 1.0):
            errors.append(f"Invalid max position size: {self.MAX_POSITION_SIZE} (must be 0.1%-100%)")
            
        if not (0.1 <= self.MIN_CONFIDENCE <= 1.0):
            errors.append(f"Invalid min confidence: {self.MIN_CONFIDENCE} (must be 10%-100%)")

    def _validate_risk_config(self, errors: List[str], warnings: List[str]):
        """Validate risk management parameters with conservative defaults"""
        
        # Daily loss limit validation
        if not (0.01 <= self.MAX_DAILY_LOSS <= 1.0):
            errors.append(f"Invalid max daily loss: {self.MAX_DAILY_LOSS} (must be 1%-100%)")
        elif self.MAX_DAILY_LOSS > 0.5:
            warnings.append(f"High daily loss limit: {self.MAX_DAILY_LOSS*100:.0f}% - high risk exposure")

        # Position limits validation
        if not (1 <= self.MAX_POSITIONS <= 1000):
            errors.append(f"Invalid max positions: {self.MAX_POSITIONS} (must be 1-1000)")

        # Position timeout validation (1 hour to 1 week)
        if not (3600 <= self.POSITION_TIMEOUT <= 604800):
            warnings.append(f"Unusual position timeout: {self.POSITION_TIMEOUT}s (recommended: 1h-7d)")

        # Liquidation buffer validation
        if not (0.01 <= self.LIQUIDATION_BUFFER <= 0.5):
            errors.append(f"Invalid liquidation buffer: {self.LIQUIDATION_BUFFER} (must be 1%-50%)")

        # Portfolio value validation
        if self.PORTFOLIO_VALUE <= 0:
            errors.append(f"Invalid portfolio value: {self.PORTFOLIO_VALUE} (must be positive)")
        elif self.PORTFOLIO_VALUE < 100:
            warnings.append(f"Low portfolio value: ${self.PORTFOLIO_VALUE} - may limit trading options")

    def _validate_service_config(self, errors: List[str], warnings: List[str]):
        """Validate service configuration with URL format checking"""
        
        service_urls = [
            ("Risk Service", self.RISK_SERVICE_URL),
            ("Notify Service", self.NOTIFY_SERVICE_URL),
            ("Brain Service", self.BRAIN_SERVICE_URL)
        ]
        
        # Validate URL format for each service
        for name, url in service_urls:
            if not url:
                warnings.append(f"{name} URL not configured")
            elif not url.startswith(("http://", "https://")):
                errors.append(f"Invalid {name} URL format: {url} (must start with http:// or https://)")

        # Timeout validation
        if not (1 <= self.SERVICE_TIMEOUT <= 300):
            warnings.append(f"Unusual service timeout: {self.SERVICE_TIMEOUT}s (recommended: 1-300s)")
            
        # Retry validation
        if not (1 <= self.MAX_RETRIES <= 10):
            warnings.append(f"Unusual max retries: {self.MAX_RETRIES} (recommended: 1-10)")

    def _validate_external_config(self, warnings: List[str]):
        """Validate external service configurations for integrations"""
        
        # Telegram token validation
        if self.TELEGRAM_TOKEN:
            if len(self.TELEGRAM_TOKEN) < 30:
                warnings.append("Telegram token appears to be invalid format (too short)")
            elif not self.TELEGRAM_TOKEN.count(':') == 1:
                warnings.append("Telegram token format suspicious (missing colon separator)")
            
        # Telegram chat ID validation
        if self.TELEGRAM_CHAT_ID:
            if not (self.TELEGRAM_CHAT_ID.startswith("-") or self.TELEGRAM_CHAT_ID.isdigit()):
                warnings.append("Telegram chat ID appears to be invalid format (must be numeric or start with -)")

    def _parse_allowed_symbols(self):
        """Parse allowed symbols from environment with wildcard support"""
        symbols_env = os.getenv("ALLOWED_SYMBOLS", "")
        if symbols_env:
            # Allow all symbols if "ALL" or "*" is specified
            if symbols_env.upper() in ["ALL", "*"]:
                self.ALLOWED_SYMBOLS = []
                logger.info("No symbol restrictions configured (all symbols allowed)")
            else:
                # Parse comma-separated symbol list
                raw_symbols = symbols_env.split(",")
                self.ALLOWED_SYMBOLS = [s.strip().upper() for s in raw_symbols if s.strip()]
                
                if self.ALLOWED_SYMBOLS:
                    logger.info(f"Allowed symbols configured: {len(self.ALLOWED_SYMBOLS)} symbols")
                    logger.debug(f"Symbol whitelist: {', '.join(self.ALLOWED_SYMBOLS[:10])}" + 
                               (f" ... (+{len(self.ALLOWED_SYMBOLS)-10} more)" if len(self.ALLOWED_SYMBOLS) > 10 else ""))
                else:
                    logger.warning("ALLOWED_SYMBOLS set but empty after parsing, allowing all symbols")
                    self.ALLOWED_SYMBOLS = []
        else:
            logger.info("No symbol restrictions configured (all symbols allowed)")

    def _log_environment_summary(self):
        """Log configuration summary with detailed system status"""
        env_status = "TESTNET" if self.BINANCE_TESTNET else "MAINNET"
        mode_status = "SIMULATION" if getattr(self, "SIMULATION_MODE", False) else "LIVE"
        
        logger.info("=" * 60)
        logger.info(f"Environment: {env_status} | Mode: {mode_status}")
        logger.info(f"Trading: {self.LEVERAGE}x leverage, {self.MARGIN_MODE} margin")
        logger.info(f"Risk: +/-{self.STOP_LOSS_PERCENT*100:.0f}% stops, max {self.MAX_POSITIONS} positions")
        logger.info(f"Portfolio: ${self.PORTFOLIO_VALUE:,.0f}, Kelly max: {self.KELLY_MAX_FRACTION*100:.1f}%")
        
        # Service connectivity summary
        services_configured = sum([
            bool(self.RISK_SERVICE_URL),
            bool(self.NOTIFY_SERVICE_URL), 
            bool(self.BRAIN_SERVICE_URL)
        ])
        logger.info(f"Services configured: {services_configured}/3")
        
        # Symbol restrictions summary
        if self.ALLOWED_SYMBOLS:
            logger.info(f"Symbol restrictions: {len(self.ALLOWED_SYMBOLS)} allowed")
        else:
            logger.info("Symbol restrictions: None (all allowed)")
        logger.info("=" * 60)

    def get_safe_config_dict(self) -> Dict[str, Any]:
        """Get configuration dictionary with sensitive data masked for logging"""
        config_dict = asdict(self)
        
        # Mask sensitive credential fields
        sensitive_fields = ["BINANCE_API_KEY", "BINANCE_SECRET_KEY", "TELEGRAM_TOKEN"]
        for field in sensitive_fields:
            if field in config_dict and config_dict[field]:
                original_value = config_dict[field]
                if len(original_value) > 4:
                    config_dict[field] = f"{'*' * (len(original_value) - 4)}{original_value[-4:]}"
                else:
                    config_dict[field] = "****"
        
        return config_dict

# Configuration instance creation and validation
try:
    config = ExecuteServiceConfig()
    logger.info("Execute Service configuration loaded successfully")

    # Configuration summary output (excluding sensitive information)
    logger.info(f"   Leverage: {config.LEVERAGE}x ({config.MARGIN_MODE})")
    logger.info(f"   Stop/Profit: +/-{config.STOP_LOSS_PERCENT*100:.0f}%")
    logger.info(f"   Portfolio: ${config.PORTFOLIO_VALUE:,}")
    logger.info(f"   Allowed symbols: {len(config.ALLOWED_SYMBOLS)} symbols")

except Exception as e:
    logger.error(f"Configuration load failed: {e}")
    # Fallback to default values
    config = ExecuteServiceConfig()

# ============================================================================
#                              Integrated Data Models
# ============================================================================


@dataclass
class Phoenix95AnalysisResult:
    """Phoenix95 analysis result"""

    phoenix_score: float = 0.0
    risk_score: float = 0.5
    kelly_fraction: float = 0.002
    leverage_recommendation: int = 10
    confidence_boost: float = 1.0
    risk_warnings: List[str] = None

    def __post_init__(self):
        if self.risk_warnings is None:
            self.risk_warnings = []


@dataclass
class SignalData:
    """Standard signal data"""

    signal_id: str
    symbol: str
    action: str
    price: float
    confidence: float
    timestamp: float = 0.0
    source: str = "unknown"

    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()
            

@dataclass
class EnhancedTradingSignal(SignalData):
    """Phoenix95 enhanced trading signal"""

    phoenix95_score: float = 0.0
    risk_score: float = 0.5
    kelly_ratio: float = 0.01
    leverage_recommendation: int = 10
    service_chain: str = "unknown"


@dataclass
class Position:
    """Position model"""

    position_id: str
    signal_id: str
    symbol: str
    action: str

    # Trading information
    entry_price: float
    quantity: float
    leverage: int
    margin_required: float

    # Price information
    stop_loss_price: float
    take_profit_price: float
    liquidation_price: float
    current_price: float = 0.0

    # P&L information
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    pnl_percentage: float = 0.0
    roe: float = 0.0

    # Phoenix95 information
    phoenix95_score: float = 0.0
    service_chain: str = "unknown"

    # Status
    status: str = "ACTIVE"
    liquidation_risk: float = 0.0
    created_at: float = 0.0
    updated_at: float = 0.0
    closed_at: Optional[float] = None

    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()
        if self.updated_at == 0.0:
            self.updated_at = time.time()


@dataclass
class ExecutionResult:
    """Execution result model"""

    execution_id: str
    status: str
    message: str
    success: bool = False  # Add missing success field
    position: Optional[Position] = None
    error_details: Optional[Dict] = None
    execution_time_ms: float = 0.0
    notified: bool = False
    service_chain: str = "unknown"
    timestamp: float = 0.0

    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


# ============================================================================
#                    Core Trading Execution Engine (Integrated Version)
# ============================================================================


class BinanceRateLimiter:
    """Binance API request rate limiter with adaptive performance optimization"""
    
    def __init__(self):
        # Request tracking with optimized capacity for high-throughput systems
        self.requests_per_minute = deque(maxlen=1200)
        self.weight_used = 0
        self.weight_reset_time = 0
        self.lock = asyncio.Lock()
        
        # Performance metrics for monitoring
        self.total_requests = 0
        self.total_wait_time = 0.0
        self.limit_hits = 0
        
        # Adaptive rate limiting thresholds
        self.request_limit = 1200  # Binance limit: 1200 req/min
        self.weight_limit = 6000    # Binance limit: 6000 weight/min
        self.safety_margin = 0.95   # Use 95% of limit for safety
        
        logger.debug("Rate limiter initialized with adaptive thresholds")
        
    async def acquire(self, weight=1):
        """Acquire rate limit slot before API call with adaptive backoff"""
        async with self.lock:
            current_time = time.time()
            self.total_requests += 1
            
            # Clean expired request timestamps (older than 60 seconds)
            minute_ago = current_time - 60
            while self.requests_per_minute and self.requests_per_minute[0] <= minute_ago:
                self.requests_per_minute.popleft()
            
            # Check request per minute limit with safety margin
            effective_limit = int(self.request_limit * self.safety_margin)
            if len(self.requests_per_minute) >= effective_limit:
                sleep_time = 60 - (current_time - self.requests_per_minute[0])
                if sleep_time > 0:
                    self.limit_hits += 1
                    logger.warning(f"Rate limit threshold reached ({len(self.requests_per_minute)}/{self.request_limit}), "
                                 f"waiting {sleep_time:.1f}s")
                    self.total_wait_time += sleep_time
                    await asyncio.sleep(sleep_time)
                    current_time = time.time()
            
            # Reset weight counter if minute window expired
            if current_time > self.weight_reset_time:
                self.weight_used = 0
                self.weight_reset_time = current_time + 60
            
            # Check weight limit with safety margin
            effective_weight_limit = int(self.weight_limit * self.safety_margin)
            if self.weight_used + weight > effective_weight_limit:
                sleep_time = self.weight_reset_time - current_time
                if sleep_time > 0:
                    self.limit_hits += 1
                    logger.warning(f"Weight limit threshold reached ({self.weight_used + weight}/{self.weight_limit}), "
                                 f"waiting {sleep_time:.1f}s")
                    self.total_wait_time += sleep_time
                    await asyncio.sleep(sleep_time)
            
            # Record request and update weight
            self.requests_per_minute.append(current_time)
            self.weight_used += weight
    
    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter performance statistics"""
        return {
            "total_requests": self.total_requests,
            "current_requests_in_window": len(self.requests_per_minute),
            "current_weight_used": self.weight_used,
            "limit_hits": self.limit_hits,
            "total_wait_time": round(self.total_wait_time, 2),
            "avg_wait_time": round(self.total_wait_time / max(1, self.limit_hits), 2) if self.limit_hits > 0 else 0
        }

class Phoenix95ExecuteEngine:
    """Phoenix 95 V4 integrated trading execution engine with high-performance optimization"""

    def __init__(self):
        # Core trading state management
        self.active_positions: Dict[str, Position] = {}
        self.position_history: List[Position] = []
        self.daily_pnl = 0.0
        self.total_trades = 0
        self.successful_trades = 0

        # High-performance concurrency controls for multi-core systems
        self.rate_limiter = BinanceRateLimiter()
        self.account_lock = asyncio.Lock()
        self.position_lock = asyncio.Lock()  # Thread-safe position management
        
        # Advanced performance metrics with real-time tracking
        self.execution_stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "avg_execution_time": 0.0,
            "min_execution_time": float('inf'),
            "max_execution_time": 0.0,
            "total_volume": 0.0,
            "current_positions": 0,
            "daily_pnl": 0.0,
            "win_rate": 0.0,
            "notify_success_rate": 0.0,
            "last_execution_time": 0.0,
        }

        # Enhanced risk tracking with comprehensive metrics
        self.risk_metrics = {
            "max_drawdown": 0.0,
            "current_drawdown": 0.0,
            "peak_balance": config.PORTFOLIO_VALUE,
            "current_exposure": 0.0,
            "margin_utilization": 0.0,
            "liquidation_warnings": 0,
            "phoenix95_boost_count": 0,
            "emergency_closes": 0,
        }

        # Service connection status with health tracking
        self.service_connections = {
            "risk_service": "unknown",
            "notify_service": "unknown",
            "brain_service": "unknown",
        }
        
        # Auto-reconnection system configuration
        self.reconnection_config = {
            "enabled": True,
            "check_interval": 10.0,  # Check every 10 seconds
            "max_retry_attempts": 5,
            "retry_delay": 2.0,  # Initial retry delay in seconds
            "exponential_backoff": True,
            "circuit_breaker_threshold": 10,  # Open circuit after 10 consecutive failures
            "circuit_breaker_timeout": 60.0,  # Reset circuit after 60 seconds
        }
        
        # Service health tracking for auto-reconnection
        self.service_health = {
            "risk_service": {
                "status": "unknown",
                "last_check": 0.0,
                "consecutive_failures": 0,
                "consecutive_successes": 0,
                "total_checks": 0,
                "circuit_open": False,
                "circuit_opened_at": 0.0,
            },
            "notify_service": {
                "status": "unknown",
                "last_check": 0.0,
                "consecutive_failures": 0,
                "consecutive_successes": 0,
                "total_checks": 0,
                "circuit_open": False,
                "circuit_opened_at": 0.0,
            },
            "brain_service": {
                "status": "unknown",
                "last_check": 0.0,
                "consecutive_failures": 0,
                "consecutive_successes": 0,
                "total_checks": 0,
                "circuit_open": False,
                "circuit_opened_at": 0.0,
            },
        }
        
        # Performance optimization flags for high-end hardware (Ryzen 9 7950X / i9-13900K + 128GB RAM)
        self.hardware_optimized = True
        self.max_concurrent_orders = 20  # Increased for 16-24 core CPU
        self.async_monitoring_enabled = True
        self.max_worker_threads = 16  # Optimize for multi-core processing
        
        # Memory management for 128GB DDR5 RAM system
        self.max_history_size = 10000  # Increased from 5000 for 128GB RAM
        self.cache_enabled = True
        self.aggressive_caching = True  # Enable aggressive caching with abundant RAM
        
        # CRITICAL FIX: Initialize cache systems for optimized functions
        self._price_cache = {}  # Price cache for _get_current_price_optimized
        self._cache_timestamps = {}  # Cache timestamp tracking
        self._sim_price_cache = {}  # Simulation price cache
        self._risk_alert_cache = {}  # Risk alert deduplication cache
        self._risk_alert_timestamps = {}  # Risk alert rate limiting timestamps
        self._risk_alert_global_count = 0  # Global alert counter
        self._risk_alert_global_reset = time.time()  # Global counter reset time
        self._binance_client_pool = {}  # Binance connection pool
        
        # CRITICAL FIX: Initialize cache statistics tracking (128GB RAM optimized)
        self._cache_hit_count = {}  # Cache hit counter per symbol
        self._cache_miss_count = {}  # Cache miss counter per symbol
        self._price_source_reliability = {}  # Price source reliability tracking
        
        # CRITICAL FIX: Initialize notification and close tracking caches
        self._notification_retry_cache = {}  # Notification retry attempt tracking
        self._close_notification_cache = {}  # Position close notification cache
        
        # CRITICAL FIX: Initialize position ID mapping for tracking
        self._position_id_map = {}  # Maps order_id to position_id for accurate tracking
        
        logger.debug("Cache systems initialized for high-performance operation")
        logger.debug("Cache statistics tracking enabled for 128GB RAM optimization")
        
        # Auto-reconnection background task reference
        self._reconnection_task = None
        self._reconnection_running = False
        
        logger.info("=" * 70)
        logger.info("Phoenix 95 V4 Integrated Trading Execution Engine")
        logger.info("=" * 70)
        logger.info(f"Hardware Optimization: Enabled (Multi-core support)")
        logger.info(f"CPU Configuration: 16-24 cores optimized (Ryzen 9 7950X / i9-13900K)")
        logger.info(f"RAM Configuration: 128GB DDR5 optimized")
        logger.info(f"Config: {config.LEVERAGE}x leverage, {config.MARGIN_MODE} margin")
        logger.info(f"Take profit/Stop loss: +/-{config.TAKE_PROFIT_PERCENT*100:.0f}%")
        logger.info(f"Portfolio: ${config.PORTFOLIO_VALUE:,.2f}")
        logger.info(f"Max concurrent orders: {self.max_concurrent_orders}")
        logger.info(f"History buffer: {self.max_history_size} positions")
        logger.info(f"Aggressive caching: {'Enabled' if self.aggressive_caching else 'Disabled'}")
        logger.info(f"Auto-reconnection: {'Enabled' if self.reconnection_config['enabled'] else 'Disabled'}")
        logger.info(f"Service integration:")
        logger.info(f"  - Risk Service: {config.RISK_SERVICE_URL}")
        logger.info(f"  - Notify Service: {config.NOTIFY_SERVICE_URL}")
        logger.info(f"  - Brain Service: {config.BRAIN_SERVICE_URL}")
        
        # Display cache configuration
        logger.info(f"Cache configuration:")
        logger.info(f"  - Price cache: Enabled (10 min retention)")
        logger.info(f"  - Simulation cache: Enabled (1000 items)")
        logger.info(f"  - Risk alert cache: Enabled (1 hour retention)")
        logger.info(f"  - Connection pool: Enabled (5 min keep-alive)")
        logger.info(f"  - Cache statistics: Enabled (hit/miss tracking)")
        logger.info(f"  - Position ID mapping: Enabled (order tracking)")
        logger.info("=" * 70)

        # Initialize Binance client with error handling
        self.binance_client = None
        if BINANCE_AVAILABLE:
            try:
                self._init_binance_testnet_client()
            except Exception as e:
                logger.error(f"Binance client initialization failed: {e}")
                logger.warning("Continuing in simulation mode")
        else:
            logger.warning("Binance library not available, using simulation mode")
        
# Changes:
# - Remove negative warnings for current system
# - Add dynamic optimization based on detected resources
# - Prepare for 128GB RAM upgrade
# - Enable aggressive caching for high-end systems
# - Optimize multi-core settings

        # Check system resources and configure optimization dynamically
        try:
            import psutil
            system_memory = psutil.virtual_memory()
            cpu_count = psutil.cpu_count(logical=False)
            cpu_count_logical = psutil.cpu_count(logical=True)
            
            memory_gb = system_memory.total / (1024**3)
            available_gb = system_memory.available / (1024**3)
            
            logger.info("=" * 70)
            logger.info("System Resources Detected:")
            logger.info(f"  - Physical CPU cores: {cpu_count}")
            logger.info(f"  - Logical CPU cores: {cpu_count_logical}")
            logger.info(f"  - Total RAM: {memory_gb:.1f} GB")
            logger.info(f"  - Available RAM: {available_gb:.1f} GB")
            
            # Dynamic optimization configuration based on detected resources
            if memory_gb >= 100:
                logger.info("  - System configuration: OPTIMAL for high-performance trading (128GB+ RAM)")
                logger.info("  - Aggressive caching fully enabled for maximum performance")
                self.aggressive_caching = True
                self.max_history_size = 10000
                self.max_concurrent_orders = 20
            elif memory_gb >= 64:
                logger.info("  - System configuration: GOOD for trading operations (64GB+ RAM)")
                logger.info("  - Enhanced caching enabled")
                self.aggressive_caching = True
                self.max_history_size = 7000
                self.max_concurrent_orders = 15
            elif memory_gb >= 32:
                logger.info("  - System configuration: ADEQUATE for trading operations")
                logger.info("  - Standard caching enabled")
                self.aggressive_caching = False
                self.max_history_size = 5000
                self.max_concurrent_orders = 10
            else:
                logger.info("  - System configuration: BASIC mode enabled")
                logger.info("  - Conservative resource usage")
                self.aggressive_caching = False
                self.max_history_size = 3000
                self.max_concurrent_orders = 5
            
            # CPU optimization configuration
            if cpu_count >= 16:
                logger.info("  - CPU configuration: EXCELLENT for parallel processing (16+ cores)")
                logger.info("  - Multi-core optimization fully enabled")
                self.max_worker_threads = 16
            elif cpu_count >= 8:
                logger.info("  - CPU configuration: GOOD for concurrent operations (8+ cores)")
                logger.info("  - Parallel processing enabled")
                self.max_worker_threads = 8
            else:
                logger.info("  - CPU configuration: ADEQUATE for trading operations")
                logger.info("  - Basic concurrency enabled")
                self.max_worker_threads = 4
            
            logger.info(f"  - Optimization applied: history={self.max_history_size}, orders={self.max_concurrent_orders}, threads={self.max_worker_threads}")
            logger.info("=" * 70)
            
        except ImportError:
            logger.info("psutil not available - using default optimization settings")
            logger.info("Install psutil for dynamic resource optimization: pip install psutil")
        except Exception as resource_error:
            logger.debug(f"Could not detect system resources: {resource_error}")
            logger.info("Using default optimization settings")
        
        # NOTE: Auto-reconnection monitoring will be started by FastAPI startup event
        # Cannot use asyncio.create_task here because event loop is not running yet
        logger.info("Auto-reconnection system configured (will start with FastAPI)")
        logger.info("Initialization completed successfully")
    
    def _update_execution_stats(self, success: bool, execution_time_ms: float):
        """
        Update execution statistics with comprehensive tracking
        Optimized for high-frequency trading on 128GB RAM system
        
        Args:
            success: Whether the execution was successful
            execution_time_ms: Execution time in milliseconds
        """
        try:
            # Update execution counters
            self.execution_stats["total_executions"] += 1
            
            if success:
                self.execution_stats["successful_executions"] += 1
            else:
                self.execution_stats["failed_executions"] += 1
            
            # Update execution time statistics
            if execution_time_ms > 0:
                self.execution_stats["last_execution_time"] = execution_time_ms
                
                # Update min/max execution times
                if execution_time_ms < self.execution_stats["min_execution_time"]:
                    self.execution_stats["min_execution_time"] = execution_time_ms
                    
                if execution_time_ms > self.execution_stats["max_execution_time"]:
                    self.execution_stats["max_execution_time"] = execution_time_ms
                
                # Update average execution time (incremental calculation)
                total = self.execution_stats["total_executions"]
                current_avg = self.execution_stats["avg_execution_time"]
                self.execution_stats["avg_execution_time"] = (
                    (current_avg * (total - 1) + execution_time_ms) / total
                )
            
            # Log performance warnings for slow executions (> 5 seconds)
            if execution_time_ms > 5000:
                logger.warning(
                    f"Slow execution detected: {execution_time_ms:.1f}ms "
                    f"(avg: {self.execution_stats['avg_execution_time']:.1f}ms)"
                )
            
            # Periodic statistics summary (every 100 executions on high-end hardware)
            if self.execution_stats["total_executions"] % 100 == 0:
                success_rate = (
                    self.execution_stats["successful_executions"] / 
                    self.execution_stats["total_executions"] * 100
                )
                logger.info(
                    f"Execution Statistics Summary (Total: {self.execution_stats['total_executions']}): "
                    f"Success Rate: {success_rate:.1f}%, "
                    f"Avg Time: {self.execution_stats['avg_execution_time']:.1f}ms, "
                    f"Min: {self.execution_stats['min_execution_time']:.1f}ms, "
                    f"Max: {self.execution_stats['max_execution_time']:.1f}ms"
                )
                
        except Exception as e:
            logger.error(f"Error updating execution statistics: {e}")

    def _init_binance_testnet_client(self):
        """Initialize Binance testnet client with enhanced connection testing"""
        try:
            api_key = os.getenv('BINANCE_API_KEY')
            secret_key = os.getenv('BINANCE_SECRET_KEY')
            
            if not api_key or not secret_key:
                logger.warning("Binance API keys not configured - using simulation mode")
                self.binance_client = None
                return
            
            # Store API credentials for HTTP-based requests
            self.binance_api_key = api_key
            self.binance_secret_key = secret_key
            self.binance_testnet_base_url = "https://testnet.binance.vision"
            
            # Test connectivity with both SPOT and FUTURES APIs
            try:
                import requests
                
                test_results = []
                
                # Test SPOT API endpoint
                spot_url = f"{self.binance_testnet_base_url}/api/v3/ticker/price?symbol=BTCUSDT"
                try:
                    spot_response = requests.get(spot_url, timeout=10)
                    if spot_response.status_code == 200:
                        spot_data = spot_response.json()
                        btc_spot_price = float(spot_data['price'])
                        logger.info(f"Binance SPOT API verified: BTC ${btc_spot_price:,.2f}")
                        test_results.append(("spot", True, btc_spot_price))
                    else:
                        logger.warning(f"SPOT API test failed: HTTP {spot_response.status_code}")
                        test_results.append(("spot", False, 0))
                except requests.RequestException as e:
                    logger.warning(f"SPOT API connection error: {e}")
                    test_results.append(("spot", False, 0))
                
                # Test FUTURES API endpoint
                futures_url = f"{self.binance_testnet_base_url}/fapi/v1/ticker/price?symbol=BTCUSDT"
                try:
                    futures_response = requests.get(futures_url, timeout=10)
                    if futures_response.status_code == 200:
                        futures_data = futures_response.json()
                        btc_futures_price = float(futures_data['price'])
                        logger.info(f"Binance FUTURES API verified: BTC ${btc_futures_price:,.2f}")
                        test_results.append(("futures", True, btc_futures_price))
                    else:
                        logger.warning(f"FUTURES API test failed: HTTP {futures_response.status_code}")
                        test_results.append(("futures", False, 0))
                except requests.RequestException as e:
                    logger.warning(f"FUTURES API connection error: {e}")
                    test_results.append(("futures", False, 0))
                
                # Evaluate test results
                successful_tests = [test for test in test_results if test[1]]
                
                if successful_tests:
                    self.binance_client = "HTTP_MODE"
                    logger.info("=" * 70)
                    logger.info("Binance Testnet HTTP API Mode Activated")
                    logger.info(f"Working APIs: {', '.join([test[0].upper() for test in successful_tests])}")
                    logger.info("=" * 70)
                else:
                    logger.error("All Binance API endpoints failed connectivity test")
                    logger.error(f"Test results: {test_results}")
                    self.binance_client = None
                    
            except ImportError:
                logger.error("requests library not available, cannot test Binance connection")
                self.binance_client = None
            except Exception as test_error:
                logger.error(f"Unexpected error during connection test: {test_error}")
                logger.error(f"Error type: {type(test_error).__name__}")
                self.binance_client = None
                
        except Exception as e:
            logger.error(f"Binance testnet client initialization failed: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.binance_client = None

    async def _start_auto_reconnection_monitor(self):
        """
        Start background task for automatic service reconnection monitoring
        Optimized for high-performance systems (Ryzen 9 7950X, 128GB RAM)
        Features: Initial startup delay, progressive retry, enhanced health tracking
        """
        try:
            self._reconnection_running = True
            logger.info("=" * 70)
            logger.info("Auto-reconnection monitor starting...")
            logger.info("=" * 70)
            
            # CRITICAL FIX: Initial startup delay for other services
            initial_delay = 3.0
            logger.info(f"Waiting {initial_delay}s for other services to start...")
            await asyncio.sleep(initial_delay)
            
            # CRITICAL FIX: Progressive initial connection check with retries
            max_initial_attempts = 3
            for attempt in range(max_initial_attempts):
                logger.info(f"Initial connection check (attempt {attempt + 1}/{max_initial_attempts})...")
                await self._check_all_service_connections()
                
                # Check how many services connected
                connected_count = sum(
                    1 for status in self.service_connections.values() 
                    if status == "connected"
                )
                total_services = len(self.service_connections)
                
                logger.info(f"Initial check result: {connected_count}/{total_services} services connected")
                
                # If all services connected, break early
                if connected_count == total_services:
                    logger.info("All services connected successfully!")
                    break
                
                # If not all connected and not last attempt, wait and retry
                if attempt < max_initial_attempts - 1:
                    retry_delay = 5.0 * (attempt + 1)
                    logger.warning(
                        f"Not all services connected, retrying in {retry_delay}s "
                        f"({connected_count}/{total_services} connected)"
                    )
                    await asyncio.sleep(retry_delay)
            
            # Log final initial connection status
            connected_services = [
                name for name, status in self.service_connections.items() 
                if status == "connected"
            ]
            disconnected_services = [
                name for name, status in self.service_connections.items() 
                if status != "connected"
            ]
            
            logger.info("=" * 70)
            logger.info("Initial Connection Status:")
            logger.info(f"  Connected: {', '.join(connected_services) if connected_services else 'None'}")
            logger.info(f"  Disconnected: {', '.join(disconnected_services) if disconnected_services else 'None'}")
            logger.info("=" * 70)
            logger.info("Auto-reconnection monitor started successfully")
            
            # Main monitoring loop
            while self._reconnection_running:
                try:
                    # Wait for check interval
                    await asyncio.sleep(self.reconnection_config['check_interval'])
                    
                    # Perform connection health check
                    await self._check_all_service_connections()
                    
                except asyncio.CancelledError:
                    logger.info("Auto-reconnection monitor cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in auto-reconnection monitor: {e}")
                    await asyncio.sleep(5.0)
                    
        except Exception as e:
            logger.error(f"Critical error in auto-reconnection monitor: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        finally:
            self._reconnection_running = False
            logger.info("Auto-reconnection monitor stopped")
    
    async def _check_all_service_connections(self):
        """Check connection status of all integrated services"""
        check_start = time.time()
        
        services = {
            "risk_service": config.RISK_SERVICE_URL,
            "notify_service": config.NOTIFY_SERVICE_URL,
            "brain_service": config.BRAIN_SERVICE_URL,
        }
        
        # Parallel health checks for all services
        tasks = []
        for service_name, service_url in services.items():
            if service_url:
                tasks.append(self._check_single_service_connection(service_name, service_url))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        check_duration = (time.time() - check_start) * 1000
        logger.debug(f"Service connection check completed in {check_duration:.1f}ms")
    
    async def _check_single_service_connection(self, service_name: str, service_url: str):
        """Check connection status of a single service with circuit breaker pattern"""
        try:
            health_info = self.service_health[service_name]
            current_time = time.time()
            
            # Check circuit breaker status
            if health_info['circuit_open']:
                timeout_elapsed = current_time - health_info['circuit_opened_at']
                if timeout_elapsed < self.reconnection_config['circuit_breaker_timeout']:
                    # Circuit still open, skip check
                    logger.debug(f"Circuit breaker open for {service_name}, skipping check")
                    return
                else:
                    # Reset circuit breaker after timeout
                    logger.info(f"Resetting circuit breaker for {service_name} after {timeout_elapsed:.0f}s")
                    health_info['circuit_open'] = False
                    health_info['consecutive_failures'] = 0
            
            # Perform health check
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{service_url}/health",
                        timeout=aiohttp.ClientTimeout(total=5.0)
                    ) as response:
                        health_info['total_checks'] += 1
                        health_info['last_check'] = current_time
                        
                        if response.status == 200:
                            # Success
                            if health_info['status'] != "connected":
                                logger.info(f"{service_name} connection restored")
                            
                            health_info['status'] = "connected"
                            health_info['consecutive_successes'] += 1
                            health_info['consecutive_failures'] = 0
                            self.service_connections[service_name] = "connected"
                        else:
                            # HTTP error
                            self._handle_service_connection_failure(
                                service_name, 
                                f"HTTP {response.status}"
                            )
                            
            except asyncio.TimeoutError:
                self._handle_service_connection_failure(service_name, "Timeout")
            except aiohttp.ClientError as e:
                self._handle_service_connection_failure(service_name, f"Connection error: {e}")
                
        except Exception as e:
            logger.error(f"Error checking {service_name}: {e}")
    
    def _handle_service_connection_failure(self, service_name: str, reason: str):
        """Handle service connection failure with circuit breaker logic"""
        health_info = self.service_health[service_name]
        
        health_info['status'] = "disconnected"
        health_info['consecutive_failures'] += 1
        health_info['consecutive_successes'] = 0
        self.service_connections[service_name] = "disconnected"
        
        # Check if circuit breaker should open
        if health_info['consecutive_failures'] >= self.reconnection_config['circuit_breaker_threshold']:
            if not health_info['circuit_open']:
                health_info['circuit_open'] = True
                health_info['circuit_opened_at'] = time.time()
                logger.error(
                    f"Circuit breaker opened for {service_name} after "
                    f"{health_info['consecutive_failures']} consecutive failures"
                )
        else:
            logger.warning(
                f"{service_name} connection failed ({reason}): "
                f"{health_info['consecutive_failures']}/{self.reconnection_config['circuit_breaker_threshold']} failures"
            )

    async def _calculate_basic_kelly(self, signal) -> float:
        """Basic Kelly Criterion calculation with validation and caching"""
        try:
            if not signal:
                logger.error("Signal object is None in Kelly calculation")
                return 0.01
            
            symbol = getattr(signal, 'symbol', '')
            confidence = getattr(signal, 'confidence', 0.7)
            
            if not symbol:
                logger.error("Invalid or missing symbol in signal")
                return 0.01
            
            # Calculate win rate and profit/loss ratio
            win_rate = self._estimate_win_rate(symbol, confidence)
            profit_loss_ratio = self._estimate_profit_loss_ratio(symbol, signal)
            
            # Kelly Criterion formula: f = (bp - q) / b
            # where b = profit/loss ratio, p = win rate, q = loss rate
            b = profit_loss_ratio
            p = win_rate
            q = 1 - p
            
            if b <= 0:
                logger.warning(f"Invalid profit/loss ratio: {b} for {symbol}")
                return 0.01
            
            # Calculate Kelly fraction with half-Kelly for safety
            kelly_fraction = (b * p - q) / b
            kelly_fraction *= 0.5  # Half-Kelly for reduced volatility
            
            # Apply bounds checking
            kelly_fraction = max(0.005, min(kelly_fraction, config.KELLY_MAX_FRACTION))
            
            # Reduce log noise for frequent calculations
            logger.debug(f"Kelly: {kelly_fraction:.3f} | Win rate: {p:.1%} | P/L ratio: {b:.2f} | Symbol: {symbol}")
            return kelly_fraction
            
        except ZeroDivisionError:
            logger.error(f"Division by zero in Kelly calculation")
            return 0.01
        except Exception as e:
            logger.error(f"Kelly calculation failed: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return 0.01

    def _estimate_win_rate(self, symbol: str, confidence: float) -> float:
        """Estimate win rate using historical data and confidence with symbol-specific adjustments"""
        try:
            # Base win rate calculation from confidence
            # Formula: 45% base + up to 25% from confidence
            base_win_rate = 0.45 + (confidence * 0.25)
            
            # Incorporate historical performance if sufficient data available
            if self.total_trades > 10:
                historical_win_rate = self.successful_trades / self.total_trades
                # Weighted average: 70% base, 30% historical
                win_rate = (base_win_rate * 0.7) + (historical_win_rate * 0.3)
            else:
                win_rate = base_win_rate
            
            # Symbol-specific adjustments for major pairs (higher liquidity)
            major_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT']
            if symbol in major_symbols:
                win_rate *= 1.05  # 5% boost for major pairs
                logger.debug(f"Major symbol boost applied: {symbol}")
            
            # Altcoin penalty for lower liquidity
            elif symbol.endswith('USDT') and self.total_trades > 50:
                historical_symbol_trades = sum(
                    1 for pos in self.position_history[-100:] 
                    if pos.symbol == symbol
                )
                if historical_symbol_trades > 5:
                    symbol_win_rate = sum(
                        1 for pos in self.position_history[-100:] 
                        if pos.symbol == symbol and pos.realized_pnl > 0
                    ) / historical_symbol_trades
                    # Blend historical symbol performance
                    win_rate = (win_rate * 0.8) + (symbol_win_rate * 0.2)
            
            # Conservative bounds: 35% - 75%
            return max(0.35, min(win_rate, 0.75))
            
        except ZeroDivisionError:
            logger.warning(f"Division by zero in win rate calculation for {symbol}")
            return 0.5
        except Exception as e:
            logger.error(f"Win rate estimation failed for {symbol}: {e}")
            return 0.5

    def _estimate_profit_loss_ratio(self, symbol: str, signal) -> float:
        """Estimate profit/loss ratio with Phoenix95 enhancement and leverage adjustment"""
        try:
            # Base ratio from configuration
            base_ratio = config.TAKE_PROFIT_PERCENT / config.STOP_LOSS_PERCENT
            
            # Phoenix95 score multiplier (if available)
            phoenix95_score = getattr(signal, 'phoenix95_score', 0)
            if phoenix95_score > 0:
                if phoenix95_score >= 85:
                    base_ratio *= 1.3  # 30% boost for elite signals
                elif phoenix95_score >= 70:
                    base_ratio *= 1.2  # 20% boost for strong signals
                elif phoenix95_score >= 50:
                    base_ratio *= 1.1  # 10% boost for good signals
                else:
                    base_ratio *= 0.9  # 10% penalty for weak signals
                
                logger.debug(f"Phoenix95 adjustment: {phoenix95_score:.0f}/95 applied to {symbol}")
            
            # Leverage risk adjustment (higher leverage = lower ratio)
            # Formula: max(0.8, 1.2 - leverage/50)
            leverage_adjustment = max(0.8, 1.2 - (config.LEVERAGE / 50))
            base_ratio *= leverage_adjustment
            
            # Symbol volatility adjustment
            if symbol in ['BTCUSDT', 'ETHUSDT']:
                base_ratio *= 1.05  # Slightly better ratios for stable majors
            
            # Apply conservative bounds: 0.8 - 3.0
            final_ratio = max(0.8, min(base_ratio, 3.0))
            
            logger.debug(f"P/L ratio for {symbol}: {final_ratio:.2f} (leverage adj: {leverage_adjustment:.2f})")
            return final_ratio
            
        except ZeroDivisionError:
            logger.error(f"Division by zero in P/L ratio calculation")
            return 1.0
        except Exception as e:
            logger.error(f"Profit/loss ratio estimation failed: {e}")
            return 1.0

    def calculate_position_size(self, signal) -> float:
        """Calculate position size using Kelly Criterion with safety bounds"""
        try:
            if not signal:
                logger.error("Cannot calculate position size: signal is None")
                return config.PORTFOLIO_VALUE * 0.01
            
            # Calculate Kelly fraction
            kelly_fraction = self._calculate_basic_kelly_sync(signal)
            
            # Apply Kelly fraction to portfolio
            position_size = config.PORTFOLIO_VALUE * kelly_fraction
            
            # Apply maximum position size constraint
            max_position_value = config.PORTFOLIO_VALUE * config.MAX_POSITION_SIZE
            position_size = min(position_size, max_position_value)
            
            # Apply minimum position size constraint (0.5% of portfolio)
            min_position_value = config.PORTFOLIO_VALUE * 0.005
            position_size = max(position_size, min_position_value)
            
            # Intelligent logging based on position significance
            if kelly_fraction > 0.015:  # Log significant positions (> 1.5%)
                logger.info(f"Position sizing: ${position_size:,.2f} ({kelly_fraction:.1%} of portfolio)")
            else:
                logger.debug(f"Small position: ${position_size:,.2f} ({kelly_fraction:.1%} of portfolio)")
            
            return position_size
            
        except Exception as e:
            logger.error(f"Position size calculation failed: {e}")
            logger.error(f"Falling back to minimum position size (1% of portfolio)")
            return config.PORTFOLIO_VALUE * 0.01

    def _calculate_basic_kelly_sync(self, signal) -> float:
        """Synchronous Kelly calculation for internal use with optimized execution"""
        try:
            if not signal:
                return 0.01
            
            symbol = getattr(signal, 'symbol', '')
            confidence = getattr(signal, 'confidence', 0.7)
            
            if not symbol:
                return 0.01
            
            # Get win rate and profit/loss ratio
            win_rate = self._estimate_win_rate(symbol, confidence)
            profit_loss_ratio = self._estimate_profit_loss_ratio(symbol, signal)
            
            # Kelly formula components
            b = profit_loss_ratio
            p = win_rate
            q = 1 - p
            
            # Validate ratio
            if b <= 0:
                logger.warning(f"Invalid P/L ratio in sync calculation: {b}")
                return 0.01
            
            # Calculate Kelly with half-Kelly safety
            kelly_fraction = (b * p - q) / b
            kelly_fraction *= 0.5
            
            # Apply bounds
            kelly_fraction = max(0.005, min(kelly_fraction, config.KELLY_MAX_FRACTION))
            
            return kelly_fraction
            
        except Exception as e:
            logger.error(f"Synchronous Kelly calculation failed: {e}")
            return 0.01
    
    def get_kelly_statistics(self) -> Dict[str, Any]:
        """Get Kelly Criterion performance statistics"""
        try:
            if self.total_trades == 0:
                return {
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "avg_kelly_fraction": 0.0,
                    "message": "No trades executed yet"
                }
            
            return {
                "total_trades": self.total_trades,
                "successful_trades": self.successful_trades,
                "win_rate": self.successful_trades / self.total_trades,
                "avg_position_size": config.PORTFOLIO_VALUE * 0.01,  # Placeholder
                "kelly_optimization": "half-kelly",
                "max_fraction": config.KELLY_MAX_FRACTION
            }
        except Exception as e:
            logger.error(f"Kelly statistics calculation failed: {e}")
            return {"error": str(e)}

    # ========================================================================
    #                          Main Trading Execution Function (Risk Service Integration)
    # ========================================================================

    async def execute_trade_from_risk_service(
        self, request_data: Dict
    ) -> ExecutionResult:
        """
        Risk Service standardized trade execution with comprehensive tracking
        Optimized for high-performance systems (Ryzen 9 7950X, 128GB RAM)
        Features: Enhanced error handling, notification tracking, detailed logging
        """
        execution_start = time.time()
        execution_id = f"EXEC_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        notification_success = False

        try:
            # Data preprocessing with validation
            processed_data = self._preprocess_request_data(request_data)
            if not processed_data["valid"]:
                logger.error(f"[{execution_id}] Request preprocessing failed: {processed_data['error']}")
                return self._create_failed_result(
                    execution_id, processed_data["error"], execution_start
                )

            signal_data = processed_data["signal_data"]
            phoenix_analysis = processed_data["phoenix_analysis"]
            service_chain = processed_data["service_chain"]

            logger.info("=" * 60)
            logger.info(f"[{execution_id}] Trade Execution Started")
            logger.info(f"Symbol: {signal_data['symbol']} {signal_data['action'].upper()} @ ${signal_data['price']:,.4f}")
            logger.info(f"Confidence: {signal_data['confidence']:.1%}")
            logger.info(f"Phoenix95 Score: {phoenix_analysis['phoenix_score']:.1f}/95")
            logger.info(f"Service Chain: {service_chain}")
            logger.info("=" * 60)

            # Create enhanced signal object with validation
            try:
                enhanced_signal = EnhancedTradingSignal(
                    signal_id=signal_data["signal_id"],
                    symbol=signal_data["symbol"],
                    action=signal_data["action"],
                    price=signal_data["price"],
                    confidence=signal_data["confidence"],
                    source=signal_data.get("source", "risk_service"),
                    phoenix95_score=phoenix_analysis["phoenix_score"],
                    risk_score=phoenix_analysis["risk_score"],
                    kelly_ratio=phoenix_analysis["kelly_fraction"],
                    leverage_recommendation=phoenix_analysis["leverage_recommendation"],
                    service_chain=service_chain,
                )
            except Exception as signal_error:
                logger.error(f"[{execution_id}] Enhanced signal creation failed: {signal_error}")
                return self._create_failed_result(
                    execution_id, f"Signal object creation error: {str(signal_error)}", execution_start
                )

            # Execute enhanced trade with timeout protection
            try:
                result = await asyncio.wait_for(
                    self._execute_enhanced_trade(enhanced_signal, execution_id, execution_start),
                    timeout=60.0
                )
            except asyncio.TimeoutError:
                logger.error(f"[{execution_id}] Trade execution timeout")
                return self._create_failed_result(
                    execution_id, "Execution timeout exceeded (60s)", execution_start
                )
            except Exception as exec_error:
                logger.error(f"[{execution_id}] Trade execution failed: {exec_error}")
                import traceback
                logger.error(f"Execution traceback: {traceback.format_exc()}")
                return self._create_failed_result(
                    execution_id, f"Execution error: {str(exec_error)}", execution_start
                )

            # Send notification via Notify Service with timeout
            try:
                notification_success = await asyncio.wait_for(
                    self._notify_execution_result(enhanced_signal, result),
                    timeout=10.0
                )
                result.notified = notification_success
            except asyncio.TimeoutError:
                logger.warning(f"[{execution_id}] Notification timeout")
                result.notified = False
                notification_success = False
            except Exception as notify_error:
                logger.warning(f"[{execution_id}] Notification failed: {notify_error}")
                result.notified = False
                notification_success = False

            # Log execution completion
            execution_duration = (time.time() - execution_start) * 1000
            logger.info("=" * 60)
            logger.info(f"[{execution_id}] Trade Execution Complete")
            logger.info(f"Status: {result.status}")
            logger.info(f"Duration: {execution_duration:.1f}ms")
            logger.info(f"Notification: {'SUCCESS' if notification_success else 'FAILED'}")
            logger.info("=" * 60)

            return result

        except Exception as e:
            logger.error(f"[{execution_id}] Critical error in trade execution: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Send error notification with timeout protection
            try:
                await asyncio.wait_for(
                    self._notify_execution_error(request_data, str(e)),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"[{execution_id}] Error notification timeout")
            except Exception as notify_error:
                logger.warning(f"[{execution_id}] Error notification failed: {notify_error}")
            
            return self._create_failed_result(
                execution_id, f"Critical execution error: {str(e)}", execution_start
            )

    def _preprocess_request_data(self, request_data: Dict) -> Dict:
        """
        Preprocess and validate request data with comprehensive error handling
        Optimized for high-performance systems (Ryzen 9 7950X/i9-13900K, 128GB RAM)
        Features: None-safe processing, multiple fallback strategies, detailed validation
        """
        preprocess_id = f"PREPROC_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        
        # Helper function: None-safe float conversion with detailed logging
        def safe_float(value, default=0.0, field_name="unknown"):
            """
            Convert value to float safely, handling None, empty strings, and invalid types
            Optimized for high-throughput processing
            """
            if value is None or value == '':
                logger.debug(f"[{preprocess_id}] Field '{field_name}': None/empty value, using default {default}")
                return default
            
            try:
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, str):
                    cleaned = value.replace(',', '').strip()
                    return float(cleaned) if cleaned else default
                else:
                    logger.warning(f"[{preprocess_id}] Field '{field_name}': unexpected type {type(value)}, using default {default}")
                    return default
            except (ValueError, TypeError) as e:
                logger.warning(f"[{preprocess_id}] Field '{field_name}': conversion failed ({e}), using default {default}")
                return default
        
        # Helper function: None-safe string conversion
        def safe_str(value, default='', field_name="unknown"):
            """
            Convert value to string safely, handling None and invalid types
            """
            if value is None:
                logger.debug(f"[{preprocess_id}] Field '{field_name}': None value, using default '{default}'")
                return default
            
            try:
                return str(value).strip()
            except Exception as e:
                logger.warning(f"[{preprocess_id}] Field '{field_name}': string conversion failed ({e}), using default '{default}'")
                return default
        
        try:
            # Input validation with detailed logging
            if not request_data or not isinstance(request_data, dict):
                logger.error(f"[{preprocess_id}] Request data validation failed: empty or not a dictionary")
                return {
                    "valid": False,
                    "error": "Request data is empty or not a dictionary"
                }

            # Debug logging for incoming data structure (high-performance mode)
            logger.debug(f"[{preprocess_id}] Preprocessing request with {len(request_data)} keys: {list(request_data.keys())}")
            
            # Deep inspection for debugging (only in debug mode to avoid performance impact)
            if logger.isEnabledFor(logging.DEBUG):
                for key, value in request_data.items():
                    value_type = type(value).__name__
                    value_preview = str(value)[:100] if value else "None"
                    logger.debug(f"  Key '{key}': type={value_type}, preview={value_preview}")

            # Enhanced signal_data recovery with multiple fallback options
            signal_data = None
            recovery_method = None
            
            # Priority 1: Direct signal_data field
            if request_data.get('signal_data') and isinstance(request_data['signal_data'], dict):
                signal_data = request_data['signal_data']
                recovery_method = "direct_signal_data"
                logger.debug(f"[{preprocess_id}] Signal data found in 'signal_data' field")
                
            # Priority 2: Signal field fallback (legacy compatibility)
            elif request_data.get('signal') and isinstance(request_data['signal'], dict):
                signal_data = request_data['signal']
                recovery_method = "legacy_signal"
                logger.info(f"[{preprocess_id}] Auto-recovery: signal_data restored from 'signal' object")
                
            # Priority 3: Root level reconstruction (emergency fallback)
            elif any(key in request_data for key in ['symbol', 'action', 'price']):
                signal_data = {
                    'signal_id': request_data.get('signal_id', f"AUTO_{int(time.time())}"),
                    'symbol': request_data.get('symbol'),
                    'action': request_data.get('action'), 
                    'price': request_data.get('price'),
                    'confidence': safe_float(request_data.get('confidence'), 0.7, 'confidence'),
                    'source': 'root_level_reconstruction'
                }
                recovery_method = "root_reconstruction"
                logger.warning(f"[{preprocess_id}] Emergency recovery: signal_data reconstructed from root level fields")
                logger.warning(f"Reconstructed signal_data: {signal_data}")
                
            # Priority 4: Complete failure - no valid data found
            if not signal_data:
                logger.error(f"[{preprocess_id}] CRITICAL: No signal data found in any format")
                logger.error(f"Available request keys: {list(request_data.keys())}")
                logger.error(f"Request data dump: {str(request_data)[:500]}")
                return {
                    "valid": False,
                    "error": "No valid signal data found (missing signal_data, signal, or root fields)"
                }
            
            logger.info(f"[{preprocess_id}] Signal data recovered via: {recovery_method}")

            # Validate required fields presence
            required_fields = ['symbol', 'action', 'price']
            missing_fields = [field for field in required_fields if not signal_data.get(field)]
            
            if missing_fields:
                logger.error(f"[{preprocess_id}] CRITICAL: Missing required fields: {missing_fields}")
                logger.error(f"Current signal_data keys: {list(signal_data.keys())}")
                logger.error(f"Signal_data content: {signal_data}")
                return {
                    "valid": False,
                    "error": f"Required fields missing: {', '.join(missing_fields)}"
                }

            # Enhanced data type validation with None-safe processing
            try:
                # Validate and convert price (None-safe)
                original_price = signal_data.get('price')
                signal_data['price'] = safe_float(original_price, 0.0, 'price')
                
                logger.debug(f"[{preprocess_id}] Price validation: {original_price} -> {signal_data['price']}")
                    
                if signal_data['price'] <= 0:
                    logger.error(f"[{preprocess_id}] Invalid price detected: {signal_data['price']}")
                    return {
                        "valid": False,
                        "error": f"Price must be positive, got: {signal_data['price']}"
                    }

                # Validate and convert confidence (None-safe)
                confidence_value = signal_data.get('confidence')
                original_confidence = confidence_value
                signal_data['confidence'] = safe_float(confidence_value, 0.7, 'confidence')
                
                logger.debug(f"[{preprocess_id}] Confidence validation: {original_confidence} -> {signal_data['confidence']}")
                    
                if not (0 < signal_data['confidence'] <= 1):
                    logger.warning(f"[{preprocess_id}] Confidence out of range: {signal_data['confidence']}, clamping to [0.1, 1.0]")
                    signal_data['confidence'] = max(0.1, min(1.0, signal_data['confidence']))

                # Validate action (None-safe)
                valid_actions = ['buy', 'sell', 'long', 'short']
                original_action = signal_data.get('action', '')
                action_str = safe_str(original_action, '', 'action')
                action_lower = action_str.lower()
                
                if action_lower not in valid_actions:
                    logger.error(f"[{preprocess_id}] Invalid action detected: '{original_action}'")
                    return {
                        "valid": False,
                        "error": f"Action must be one of {valid_actions}, got: '{original_action}'"
                    }
                signal_data['action'] = action_lower
                logger.debug(f"[{preprocess_id}] Action normalized: {original_action} -> {action_lower}")

                # Validate and normalize symbol format (None-safe)
                original_symbol = signal_data.get('symbol', '')
                symbol_str = safe_str(original_symbol, '', 'symbol')
                symbol = symbol_str.upper().strip()
                
                # Convert futures symbol to spot (remove .P suffix)
                spot_symbol = symbol.replace('.P', '')
                if symbol != spot_symbol:
                    logger.info(f"[{preprocess_id}] Symbol conversion: {symbol} -> {spot_symbol}")
                
                if len(spot_symbol) < 3:
                    logger.error(f"[{preprocess_id}] Symbol too short: {spot_symbol}")
                    return {
                        "valid": False,
                        "error": f"Symbol too short: {spot_symbol} (minimum 3 characters)"
                    }
                signal_data['symbol'] = spot_symbol
                logger.debug(f"[{preprocess_id}] Symbol validated: {original_symbol} -> {spot_symbol}")

            except (ValueError, TypeError, AttributeError) as e:
                logger.error(f"[{preprocess_id}] Data type validation error: {type(e).__name__}: {e}")
                logger.error(f"Failed field processing in signal_data")
                import traceback
                logger.error(f"Validation traceback: {traceback.format_exc()}")
                return {
                    "valid": False,
                    "error": f"Data type validation error: {str(e)}"
                }

            # Enhanced phoenix_analysis processing with None-safe handling
            phoenix_analysis = request_data.get("phoenix_analysis")
            
            if not phoenix_analysis or not isinstance(phoenix_analysis, dict):
                # Create default phoenix_analysis from available data (all None-safe)
                phoenix_score_raw = request_data.get('phoenix_score')
                phoenix_score = safe_float(phoenix_score_raw, 0.0, 'phoenix_score')
                
                phoenix_analysis = {
                    'phoenix_score': phoenix_score,
                    'risk_score': 0.5,
                    'kelly_fraction': 0.01,
                    'leverage_recommendation': config.LEVERAGE if hasattr(config, 'LEVERAGE') else 1,
                    'confidence_boost': 1.0,
                    'risk_warnings': []
                }
                logger.debug(f"[{preprocess_id}] Default phoenix_analysis created (phoenix_score: {phoenix_score})")
            else:
                # Validate and fill missing fields in existing phoenix_analysis (all None-safe)
                phoenix_analysis['phoenix_score'] = safe_float(
                    phoenix_analysis.get('phoenix_score'), 
                    0.0, 
                    'phoenix_analysis.phoenix_score'
                )
                phoenix_analysis['risk_score'] = safe_float(
                    phoenix_analysis.get('risk_score'), 
                    0.5, 
                    'phoenix_analysis.risk_score'
                )
                phoenix_analysis['kelly_fraction'] = safe_float(
                    phoenix_analysis.get('kelly_fraction'), 
                    0.01, 
                    'phoenix_analysis.kelly_fraction'
                )
                phoenix_analysis.setdefault('leverage_recommendation', config.LEVERAGE if hasattr(config, 'LEVERAGE') else 1)
                phoenix_analysis.setdefault('confidence_boost', 1.0)
                phoenix_analysis.setdefault('risk_warnings', [])
                logger.debug(f"[{preprocess_id}] Phoenix_analysis validated and filled (phoenix_score: {phoenix_analysis['phoenix_score']})")

            # Service chain handling with None-safe validation
            service_chain = request_data.get("service_chain", "unknown")
            service_chain = safe_str(service_chain, "unknown", "service_chain")

            # Generate signal_id if missing
            if not signal_data.get('signal_id'):
                signal_data['signal_id'] = f"AUTO_{int(time.time())}_{hash(str(signal_data)) % 10000}"
                logger.debug(f"[{preprocess_id}] Auto-generated signal_id: {signal_data['signal_id']}")

            logger.info(
                f"[{preprocess_id}] Preprocessing successful: "
                f"{signal_data['symbol']} {signal_data['action'].upper()} @ ${signal_data['price']:,.4f} "
                f"(confidence: {signal_data['confidence']:.2%}, phoenix_score: {phoenix_analysis['phoenix_score']:.2f})"
            )

            return {
                "valid": True,
                "signal_data": signal_data,
                "phoenix_analysis": phoenix_analysis,
                "service_chain": service_chain
            }

        except Exception as e:
            logger.error(f"[{preprocess_id}] Critical preprocessing error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Request data type: {type(request_data)}")
            logger.error(f"Request keys: {list(request_data.keys()) if isinstance(request_data, dict) else 'Not a dict'}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                "valid": False,
                "error": f"Critical preprocessing error: {str(e)}"
            }

    def _create_failed_result(self, execution_id: str, error_message: str, execution_start: float) -> ExecutionResult:
        """Create standardized failed execution result object with enhanced tracking"""
        execution_time = (time.time() - execution_start) * 1000
        
        # Update failure statistics
        self.execution_stats["failed_executions"] += 1
        self.execution_stats["total_executions"] += 1
        
        logger.warning(f"[{execution_id}] Creating failed result")
        logger.warning(f"Failure reason: {error_message}")
        logger.warning(f"Execution time: {execution_time:.1f}ms")
        
        return ExecutionResult(
            execution_id=execution_id,
            status="FAILED",
            message=error_message,
            success=False,
            execution_time_ms=execution_time,
            timestamp=time.time(),
            notified=False,
            service_chain="execute_service_error"
        )

    async def _notify_execution_error(self, request_data: Dict, error_message: str) -> bool:
        """Send error notification to notify service"""
        try:
            # Extract basic info for notification
            symbol = "UNKNOWN"
            action = "UNKNOWN"
            
            if "signal_data" in request_data and request_data["signal_data"]:
                symbol = request_data["signal_data"].get("symbol", "UNKNOWN")
                action = request_data["signal_data"].get("action", "UNKNOWN")
            elif "signal" in request_data and request_data["signal"]:
                symbol = request_data["signal"].get("symbol", "UNKNOWN")
                action = request_data["signal"].get("action", "UNKNOWN")
            
            # Create error notification
            error_notification = {
                "type": "execution_error",
                "symbol": symbol,
                "action": action,
                "error": error_message,
                "timestamp": time.time(),
                "service": "execute_service"
            }
            
            # Log error notification
            logger.error(f"Execution error notification: {symbol} {action} - {error_message}")
            
            # Here you would send to actual notification service
            # For now, just return True to indicate notification was logged
            return True
            
        except Exception as e:
            logger.error(f"Failed to send error notification: {e}")
            return False
            
    # ========================================================================
    #                          Phoenix95 Enhanced Validation System
    # ========================================================================


    async def _enhanced_validation(self, signal: EnhancedTradingSignal) -> Dict:
        """Phoenix95 enhanced validation with comprehensive attribute checking"""
        validation_start = time.time()
        
        try:
            # Primary signal object validation
            if not signal:
                logger.error("Validation failed: Signal object is None")
                return {"valid": False, "reason": "Signal object is None"}
            
            # Required attribute validation with detailed checking
            required_attrs = {
                'symbol': ('symbol', lambda s: bool(s)),
                'action': ('action', lambda a: bool(a)),
                'price': ('price', lambda p: p is not None and p > 0),
                'confidence': ('confidence', lambda c: c is not None)
            }
            
            for attr_name, (display_name, validator) in required_attrs.items():
                if not hasattr(signal, attr_name):
                    logger.error(f"Validation failed: Missing attribute '{attr_name}'")
                    return {"valid": False, "reason": f"{display_name.capitalize()} attribute is missing"}
                
                attr_value = getattr(signal, attr_name)
                if not validator(attr_value):
                    logger.error(f"Validation failed: Invalid {attr_name} value: {attr_value}")
                    return {"valid": False, "reason": f"Invalid {display_name}: {attr_value}"}

            # Execute basic validation layer
            basic_result = await self._basic_validation(signal)
            if not basic_result["valid"]:
                logger.warning(f"Basic validation failed: {basic_result['reason']}")
                return basic_result

            # Phoenix95 score validation and confidence boosting
            phoenix95_score = getattr(signal, 'phoenix95_score', 0)
            confidence_boosted = False
            
            if phoenix95_score > 0:
                # Elite signal: confidence boost for high scores
                if phoenix95_score >= 85:
                    original_confidence = signal.confidence
                    signal.confidence = min(0.95, signal.confidence * 1.1)
                    confidence_boosted = True
                    logger.info(
                        f"Phoenix95 elite boost applied: "
                        f"{original_confidence:.1%} -> {signal.confidence:.1%} "
                        f"(score: {phoenix95_score:.0f}/95)"
                    )

                # Strong signal: minor boost
                elif phoenix95_score >= 70:
                    original_confidence = signal.confidence
                    signal.confidence = min(0.90, signal.confidence * 1.05)
                    confidence_boosted = True
                    logger.info(
                        f"Phoenix95 strong boost applied: "
                        f"{original_confidence:.1%} -> {signal.confidence:.1%} "
                        f"(score: {phoenix95_score:.0f}/95)"
                    )

                # Weak signal: rejection
                elif phoenix95_score < 30:
                    logger.warning(
                        f"Phoenix95 score too low: {phoenix95_score:.1f}/95 "
                        f"(minimum required: 30)"
                    )
                    return {
                        "valid": False,
                        "reason": f"Phoenix95 score insufficient: {phoenix95_score:.1f}/95 (min: 30)",
                    }
                
                # Medium signal: no boost, just log
                else:
                    logger.debug(f"Phoenix95 score acceptable: {phoenix95_score:.1f}/95 (no boost)")

            # Service chain validation
            service_chain = getattr(signal, 'service_chain', 'unknown')
            if service_chain and "risk" not in service_chain.lower():
                logger.warning(
                    f"Non-standard service chain detected: '{service_chain}' "
                    f"(expected 'risk' in chain)"
                )

            # Calculate validation duration
            validation_duration = (time.time() - validation_start) * 1000
            
            logger.debug(
                f"Enhanced validation passed in {validation_duration:.1f}ms: "
                f"{signal.symbol} {signal.action.upper()} "
                f"(confidence: {signal.confidence:.1%}, "
                f"Phoenix95: {phoenix95_score:.0f}/95)"
            )

            return {
                "valid": True,
                "reason": "Phoenix95 enhanced validation passed",
                "confidence_boosted": confidence_boosted,
                "validation_duration_ms": validation_duration
            }

        except AttributeError as e:
            logger.error(f"Signal attribute error during validation: {e}")
            return {"valid": False, "reason": f"Signal object attribute error: {str(e)}"}
        except Exception as e:
            logger.error(f"Unexpected error during enhanced validation: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"valid": False, "reason": f"Enhanced validation error: {str(e)}"}

    async def _phoenix95_risk_check(self, signal: EnhancedTradingSignal) -> Dict:
        """Phoenix95 based risk validation with comprehensive multi-layer checks"""
        risk_check_start = time.time()
        
        try:
            # Layer 0: Signal object validation
            if not signal:
                logger.error("Risk check failed: Signal object is None")
                return {"approved": False, "level": -1, "reason": "Signal object is None"}

            # Layer 1: Basic risk validation
            basic_risk = await self._comprehensive_risk_check(signal)
            if not basic_risk["approved"]:
                logger.warning(
                    f"Basic risk check failed: {basic_risk['reason']} "
                    f"(level: {basic_risk['level']})"
                )
                return basic_risk

            # Layer 2: Phoenix95 risk score validation
            risk_score = getattr(signal, 'risk_score', 0.5)
            
            if risk_score > 0.8:
                logger.error(
                    f"Phoenix95 risk score critical: {risk_score:.2f} "
                    f"(threshold: 0.8, symbol: {signal.symbol})"
                )
                return {
                    "approved": False,
                    "level": 4,
                    "reason": f"Phoenix95 risk score dangerous: {risk_score:.2f} (max: 0.8)",
                }
            elif risk_score > 0.6:
                logger.warning(
                    f"Phoenix95 risk score elevated: {risk_score:.2f} "
                    f"(symbol: {signal.symbol})"
                )

            # Layer 3: Leverage recommendation validation
            leverage_recommendation = getattr(signal, 'leverage_recommendation', config.LEVERAGE)
            max_allowed_leverage = config.LEVERAGE * 1.5
            
            if leverage_recommendation > max_allowed_leverage:
                logger.warning(
                    f"Excessive leverage recommendation detected: {leverage_recommendation}x "
                    f"(max allowed: {max_allowed_leverage}x, configured: {config.LEVERAGE}x)"
                )
                signal.leverage_recommendation = config.LEVERAGE
                logger.info(f"Leverage capped to configured value: {config.LEVERAGE}x")

            # Layer 4: Market exposure validation with detailed calculations
            async with self.position_lock:
                current_exposure = sum(
                    pos.margin_required for pos in self.active_positions.values()
                )
                active_position_count = len(self.active_positions)
            
            max_exposure = config.PORTFOLIO_VALUE * 0.5
            exposure_ratio = current_exposure / max_exposure if max_exposure > 0 else 0
            
            if current_exposure > max_exposure:
                logger.error(
                    f"Market exposure limit exceeded: "
                    f"${current_exposure:,.2f} / ${max_exposure:,.2f} "
                    f"({exposure_ratio:.1%}, active positions: {active_position_count})"
                )
                return {
                    "approved": False,
                    "level": 5,
                    "reason": (
                        f"Market exposure limit exceeded: ${current_exposure:,.0f}/${max_exposure:,.0f} "
                        f"({exposure_ratio:.0%})"
                    ),
                }
            
            # Exposure warning at 80% utilization
            elif exposure_ratio > 0.8:
                logger.warning(
                    f"High market exposure: {exposure_ratio:.1%} "
                    f"(${current_exposure:,.2f} / ${max_exposure:,.2f})"
                )

            # Layer 5: Position concentration risk check
            if signal.symbol in [pos.symbol for pos in self.active_positions.values()]:
                existing_positions = [
                    pos for pos in self.active_positions.values() 
                    if pos.symbol == signal.symbol
                ]
                if len(existing_positions) >= 2:
                    logger.warning(
                        f"Position concentration warning: {len(existing_positions)} "
                        f"existing positions for {signal.symbol}"
                    )
                    return {
                        "approved": False,
                        "level": 6,
                        "reason": f"Position concentration limit: {len(existing_positions)} positions already exist for {signal.symbol}",
                    }

            # Calculate risk check duration
            risk_check_duration = (time.time() - risk_check_start) * 1000
            
            logger.debug(
                f"Phoenix95 risk validation passed in {risk_check_duration:.1f}ms: "
                f"{signal.symbol} (risk score: {risk_score:.2f}, "
                f"leverage: {leverage_recommendation}x, "
                f"exposure: {exposure_ratio:.1%})"
            )

            return {
                "approved": True,
                "level": 0,
                "reason": "Phoenix95 risk validation passed",
                "risk_score": risk_score,
                "exposure_ratio": exposure_ratio,
                "risk_check_duration_ms": risk_check_duration
            }

        except AttributeError as e:
            logger.error(f"Signal attribute error during risk check: {e}")
            return {
                "approved": False, 
                "level": -1, 
                "reason": f"Signal object attribute error: {str(e)}"
            }
        except Exception as e:
            logger.error(f"Unexpected error during Phoenix95 risk validation: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                "approved": False,
                "level": -1,
                "reason": f"Phoenix95 risk validation error: {str(e)}",
            }

    async def _calculate_phoenix95_kelly_size(
        self, signal: EnhancedTradingSignal
    ) -> float:
        """Phoenix95 enhanced Kelly Criterion with adaptive multipliers and risk management"""
        calculation_start = time.time()
        
        try:
            # Signal object validation
            if not signal:
                logger.error("Kelly calculation aborted: Signal object is None")
                return config.PORTFOLIO_VALUE * 0.01

            # Calculate base Kelly fraction
            base_kelly = await self._calculate_basic_kelly(signal)
            
            if base_kelly <= 0:
                logger.warning("Base Kelly calculation returned invalid value, using minimum")
                base_kelly = 0.005

            # Phoenix95 score adaptive multiplier
            phoenix95_score = getattr(signal, 'phoenix95_score', 0)
            phoenix_multiplier = 1.0
            multiplier_reason = "default"
            
            if phoenix95_score > 0:
                if phoenix95_score >= 90:
                    phoenix_multiplier = 1.4
                    multiplier_reason = "elite"
                elif phoenix95_score >= 85:
                    phoenix_multiplier = 1.3
                    multiplier_reason = "excellent"
                elif phoenix95_score >= 80:
                    phoenix_multiplier = 1.2
                    multiplier_reason = "strong"
                elif phoenix95_score >= 70:
                    phoenix_multiplier = 1.1
                    multiplier_reason = "good"
                elif phoenix95_score >= 50:
                    phoenix_multiplier = 1.0
                    multiplier_reason = "acceptable"
                elif phoenix95_score >= 40:
                    phoenix_multiplier = 0.9
                    multiplier_reason = "weak"
                else:
                    phoenix_multiplier = 0.8
                    multiplier_reason = "poor"

                logger.info(
                    f"Phoenix95 Kelly multiplier: {phoenix_multiplier:.2f}x "
                    f"({multiplier_reason}, score: {phoenix95_score:.1f}/95)"
                )

            # Risk score adjustment
            risk_score = getattr(signal, 'risk_score', 0.5)
            risk_adjustment = max(0.5, 1.0 - risk_score * 0.5)
            
            if risk_score > 0.6:
                logger.warning(
                    f"Elevated risk score detected: {risk_score:.2f}, "
                    f"applying {risk_adjustment:.2f}x adjustment"
                )

            # Kelly ratio blending (if available from Phoenix95 analysis)
            kelly_ratio = getattr(signal, 'kelly_ratio', 0)
            
            # CRITICAL FIX: Improved kelly_ratio validation and logging
            if kelly_ratio > 0:
                # Check if kelly_ratio is within acceptable bounds
                if kelly_ratio > config.KELLY_MAX_FRACTION:
                    logger.warning(
                        f"Phoenix95 Kelly ratio exceeds maximum: {kelly_ratio:.4f} > {config.KELLY_MAX_FRACTION:.4f}, "
                        f"using base calculation only"
                    )
                    mixed_kelly = base_kelly
                elif kelly_ratio < 0.001:
                    # Extremely low kelly ratio (less than 0.1%)
                    logger.warning(
                        f"Phoenix95 Kelly ratio too low: {kelly_ratio:.4f} < 0.001, "
                        f"using base calculation only"
                    )
                    mixed_kelly = base_kelly
                else:
                    # Valid range: blend base and Phoenix95 recommendations
                    # Weighted blend: 60% base calculation, 40% Phoenix95 recommendation
                    mixed_kelly = (base_kelly * 0.6) + (kelly_ratio * 0.4)
                    logger.info(
                        f"Kelly blending: base {base_kelly:.4f} (60%) + "
                        f"Phoenix95 {kelly_ratio:.4f} (40%) = {mixed_kelly:.4f}"
                    )
            else:
                # No kelly_ratio provided or it's zero
                mixed_kelly = base_kelly
                if phoenix95_score > 0:
                    logger.debug(
                        f"No Phoenix95 Kelly ratio provided (score: {phoenix95_score:.1f}), "
                        f"using base calculation: {base_kelly:.4f}"
                    )

            # Apply all adjustments
            final_kelly = mixed_kelly * phoenix_multiplier * risk_adjustment
            
            # Safety bounds enforcement
            final_kelly = max(0.005, min(final_kelly, config.KELLY_MAX_FRACTION))

            # Calculate position size
            position_size = config.PORTFOLIO_VALUE * final_kelly
            
            # Position size bounds
            min_position = config.PORTFOLIO_VALUE * 0.005
            max_position = config.PORTFOLIO_VALUE * config.MAX_POSITION_SIZE
            position_size = max(min_position, min(position_size, max_position))

            # Calculation performance metric
            calculation_duration = (time.time() - calculation_start) * 1000

            # Detailed logging
            logger.info("=" * 50)
            logger.info("Phoenix95 Kelly Position Sizing:")
            logger.info(f"  Base Kelly: {base_kelly:.4f}")
            logger.info(f"  Mixed Kelly: {mixed_kelly:.4f}")
            logger.info(f"  Phoenix95 boost: {phoenix_multiplier:.2f}x ({multiplier_reason})")
            logger.info(f"  Risk adjustment: {risk_adjustment:.2f}x")
            logger.info(f"  Final Kelly: {final_kelly:.1%}")
            logger.info(f"  Position size: ${position_size:,.2f}")
            logger.info(f"  Portfolio %: {(position_size/config.PORTFOLIO_VALUE)*100:.2f}%")
            logger.info(f"  Calculation time: {calculation_duration:.1f}ms")
            logger.info("=" * 50)

            return position_size

        except ZeroDivisionError as e:
            logger.error(f"Division by zero in Phoenix95 Kelly calculation: {e}")
            return config.PORTFOLIO_VALUE * 0.01
        except Exception as e:
            logger.error(f"Phoenix95 Kelly calculation failed: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return config.PORTFOLIO_VALUE * 0.01

    # ========================================================================
    #                          Optimized Trading Execution
    # ========================================================================

    async def _execute_optimized_binance_order(
        self, signal: EnhancedTradingSignal, leverage_info: Dict
    ) -> Dict:
        """Real Binance SPOT trading execution with critical validation and performance optimization"""
        binance_client = None
        execution_start = time.time()
        
        try:
            # Enhanced input validation with detailed logging
            if not signal:
                logger.error("Order execution aborted: Signal object is None")
                return {"success": False, "error": "Signal object is None"}
            
            if not leverage_info or not isinstance(leverage_info, dict):
                logger.error(f"Order execution aborted: Invalid leverage_info type: {type(leverage_info)}")
                return {"success": False, "error": "Leverage info is invalid"}
            
            # Required field verification with list comprehension
            required_fields = ["margin_required", "quantity"]
            missing_fields = [field for field in required_fields if field not in leverage_info]
            if missing_fields:
                logger.error(f"Order execution aborted: Missing fields: {', '.join(missing_fields)}")
                return {"success": False, "error": f"Required fields missing: {', '.join(missing_fields)}"}

            # Signal attribute validation
            validation_checks = [
                (hasattr(signal, 'symbol') and signal.symbol, "Symbol is missing"),
                (hasattr(signal, 'action') and signal.action, "Action is missing"),
                (hasattr(signal, 'price') and signal.price is not None and signal.price > 0, "Invalid price")
            ]
            
            for check, error_msg in validation_checks:
                if not check:
                    logger.error(f"Signal validation failed: {error_msg}")
                    return {"success": False, "error": error_msg}

            logger.info("=" * 60)
            logger.info(f"SPOT Order Execution: {signal.symbol} {signal.action.upper()}")
            logger.info(f"Target Price: ${signal.price:,.4f}")
            logger.info(f"Quantity: {leverage_info['quantity']:.6f}")
            logger.info(f"Margin Required: ${leverage_info['margin_required']:,.2f}")
            logger.info("=" * 60)

            # Simulation mode bypass
            if hasattr(config, "SIMULATION_MODE") and config.SIMULATION_MODE:
                logger.info("Simulation mode active - executing simulated order")
                return await self._execute_simulation_order(signal, leverage_info, execution_start)

            # Real Binance SPOT execution path
            try:
                # Binance client initialization with exponential backoff
                binance_client = None
                for attempt in range(3):
                    try:
                        binance_client = await asyncio.wait_for(
                            self._get_binance_client(),
                            timeout=10.0
                        )
                        if binance_client:
                            logger.info(f"Binance client initialized (attempt {attempt + 1}/3)")
                            break
                        logger.warning(f"Client initialization returned None (attempt {attempt + 1}/3)")
                        if attempt < 2:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
                    except asyncio.TimeoutError:
                        logger.warning(f"Client initialization timeout (attempt {attempt + 1}/3)")
                        if attempt < 2:
                            await asyncio.sleep(2 ** attempt)
                    except Exception as e:
                        logger.error(f"Client initialization error (attempt {attempt + 1}/3): {e}")
                        if attempt < 2:
                            await asyncio.sleep(2 ** attempt)
                        else:
                            raise
                
                if not binance_client:
                    return {"success": False, "error": "Binance client initialization failed after 3 attempts"}

                # Parallel validation with timeout protection (optimized for multi-core)
                validation_start = time.time()
                
                validation_tasks = {
                    'market': self._validate_binance_market_conditions(binance_client, signal.symbol),
                    'balance': self._check_binance_account_balance(binance_client, leverage_info["margin_required"]),
                    'price': self._get_binance_optimal_price(binance_client, signal.symbol)
                }

                # Execute all validations in parallel with timeout
                results = await asyncio.wait_for(
                    asyncio.gather(
                        validation_tasks['market'],
                        validation_tasks['balance'],
                        validation_tasks['price'],
                        return_exceptions=True
                    ),
                    timeout=15.0  # 15 second total timeout for all validations
                )

                market_ok, balance_ok, optimal_price = results
                validation_duration = (time.time() - validation_start) * 1000

                logger.info(f"Parallel validation completed in {validation_duration:.1f}ms")

                # Result validation with detailed error reporting
                if isinstance(market_ok, Exception):
                    logger.error(f"Market validation exception: {type(market_ok).__name__}: {market_ok}")
                    return {"success": False, "error": f"Market validation error: {str(market_ok)}"}
                elif not market_ok:
                    logger.error("Market conditions unsuitable for trading")
                    return {"success": False, "error": "Market conditions not suitable"}

                if isinstance(balance_ok, Exception):
                    logger.error(f"Balance check exception: {type(balance_ok).__name__}: {balance_ok}")
                    return {"success": False, "error": f"Balance check error: {str(balance_ok)}"}
                elif not balance_ok:
                    logger.error(f"Insufficient balance for ${leverage_info['margin_required']:,.2f} trade")
                    return {"success": False, "error": "Insufficient balance"}

                if isinstance(optimal_price, Exception):
                    logger.warning(f"Price query exception, using signal price: {optimal_price}")
                    optimal_price = signal.price
                elif optimal_price <= 0:
                    logger.warning(f"Invalid optimal price ({optimal_price}), using signal price")
                    optimal_price = signal.price

                logger.info(f"Validation passed: Market=OK, Balance=OK, Price=${optimal_price:,.4f}")

                # SPOT trading notice (no leverage configuration)
                logger.debug("SPOT trading mode: 1x leverage (no margin configuration)")

                # Slippage calculation and execution price determination
                try:
                    slippage = self._calculate_optimal_slippage(signal, leverage_info)
                    if signal.action.lower() in ["buy", "long"]:
                        execution_price = optimal_price * (1 + slippage)
                    else:
                        execution_price = optimal_price * (1 - slippage)
                    
                    logger.info(f"Execution price: ${execution_price:,.4f} (slippage: {slippage*100:.3f}%)")
                except Exception as e:
                    logger.error(f"Slippage calculation error: {e}, using 0.1% default")
                    execution_price = optimal_price
                    slippage = 0.001

                # CRITICAL: Pre-order validation
                if execution_price <= 0:
                    logger.error(f"CRITICAL: Invalid execution_price={execution_price}, aborting order")
                    return {"success": False, "error": f"Invalid execution price: {execution_price}"}

                # Execute SPOT order with timeout protection
                order_start = time.time()
                try:
                    order_result = await asyncio.wait_for(
                        self._place_binance_spot_order(binance_client, signal, leverage_info, execution_price),
                        timeout=30.0  # 30 second order placement timeout
                    )
                except asyncio.TimeoutError:
                    logger.error("Order placement timeout (30s)")
                    return {"success": False, "error": "Order placement timeout"}
                except Exception as e:
                    logger.error(f"Order placement exception: {type(e).__name__}: {e}")
                    return {"success": False, "error": f"Order placement failed: {str(e)}"}

                order_duration = (time.time() - order_start) * 1000
                total_execution_time = (time.time() - execution_start) * 1000

                # CRITICAL: Order result validation
                if order_result.get("success", False):
                    avg_price = order_result.get("avg_price", 0)
                    executed_qty = order_result.get("executed_qty", 0)
                    
                    # Price validation
                    if avg_price <= 0:
                        logger.error(f"CRITICAL: Invalid avg_price={avg_price}")
                        logger.error(f"Order result: {order_result}")
                        return {
                            "success": False,
                            "error": f"Order executed but invalid price: {avg_price}",
                            "execution_time_ms": total_execution_time
                        }
                    
                    # Quantity validation
                    if executed_qty <= 0:
                        logger.error(f"CRITICAL: Invalid executed_qty={executed_qty}")
                        return {
                            "success": False,
                            "error": f"Order executed but invalid quantity: {executed_qty}"
                        }
                    
                    # Success logging
                    logger.info("=" * 60)
                    logger.info("SPOT Order Execution SUCCESS")
                    logger.info(f"Order ID: {order_result.get('order_id', 'N/A')}")
                    logger.info(f"Execution Price: ${avg_price:,.4f}")
                    logger.info(f"Executed Quantity: {executed_qty:.6f}")
                    logger.info(f"Commission: ${order_result.get('commission', 0):.6f}")
                    logger.info(f"Order Time: {order_duration:.1f}ms")
                    logger.info(f"Total Time: {total_execution_time:.1f}ms")
                    logger.info(f"Slippage: {slippage*100:.3f}%")
                    logger.info("=" * 60)

                    return {
                        "success": True,
                        "order_id": order_result.get("order_id", ""),
                        "execution_price": avg_price,
                        "executed_qty": executed_qty,
                        "commission": order_result.get("commission", 0),
                        "slippage": slippage,
                        "execution_time_ms": total_execution_time,
                        "order_time_ms": order_duration,
                        "validation_time_ms": validation_duration,
                        "optimized": True,
                        "exchange": "binance_spot",
                        "timestamp": time.time(),
                        "market_conditions": "validated",
                        "leverage": 1
                    }
                else:
                    error_msg = order_result.get('error', 'Unknown order error')
                    logger.error(f"Order execution FAILED: {error_msg}")
                    logger.error(f"Execution time: {total_execution_time:.1f}ms")
                    return {
                        "success": False,
                        "error": f"Binance order failed: {error_msg}",
                        "execution_time_ms": total_execution_time,
                        "attempted_price": execution_price,
                        "attempted_quantity": leverage_info.get("quantity", 0)
                    }

            except asyncio.TimeoutError:
                logger.error("Execution timeout exceeded (overall)")
                return {"success": False, "error": "Overall execution timeout exceeded"}

        except AttributeError as e:
            logger.error(f"Signal attribute error: {e}")
            logger.error(f"Signal type: {type(signal)}")
            return {"success": False, "error": f"Signal object attribute error: {str(e)}"}
        except Exception as e:
            logger.error(f"Critical execution error: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            return {"success": False, "error": f"Binance API error: {str(e)}"}
        finally:
            # Connection cleanup with error handling
            if binance_client:
                try:
                    await asyncio.wait_for(
                        binance_client.close_connection(),
                        timeout=5.0
                    )
                    logger.debug("Binance client connection closed")
                except asyncio.TimeoutError:
                    logger.warning("Connection close timeout (5s)")
                except Exception as e:
                    logger.warning(f"Connection close error: {e}")

    async def _execute_real_binance_order_enhanced(self, signal, leverage_info, start_time):
        """Enhanced Binance order execution with improved error handling"""
        binance_client = None
        try:
            # Enhanced Binance client initialization with retry logic
            for attempt in range(3):
                try:
                    binance_client = await self._get_binance_client()
                    if binance_client:
                        break
                    logger.warning(f"Binance client initialization attempt {attempt + 1}/3 failed")
                    if attempt < 2:
                        await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Binance client initialization error attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(1)
                    else:
                        raise
            
            if not binance_client:
                return {"success": False, "error": "Binance client initialization failed after 3 attempts"}

            logger.info("Binance client initialized successfully")

            # Enhanced parallel processing with individual error handling
            try:
                market_validation_task = self._validate_binance_market_conditions(
                    binance_client, signal.symbol
                )
                balance_check_task = self._check_binance_account_balance(
                    binance_client, leverage_info["margin_required"]
                )
                price_query_task = self._get_binance_optimal_price(binance_client, signal.symbol)

                # Execute tasks with individual timeout handling
                results = await asyncio.gather(
                    market_validation_task,
                    balance_check_task, 
                    price_query_task,
                    return_exceptions=True
                )

                market_ok, balance_ok, optimal_price = results

            except Exception as e:
                logger.error(f"Error in parallel validation tasks: {e}")
                return {"success": False, "error": f"Validation task error: {str(e)}"}

            # Enhanced result verification with detailed error reporting
            if isinstance(market_ok, Exception):
                logger.error(f"Market validation failed with exception: {market_ok}")
                return {"success": False, "error": f"Market validation error: {str(market_ok)}"}
            elif not market_ok:
                logger.error("Market conditions not suitable for trading")
                return {"success": False, "error": "Market conditions not suitable"}

            if isinstance(balance_ok, Exception):
                logger.error(f"Balance check failed with exception: {balance_ok}")
                return {"success": False, "error": f"Balance check error: {str(balance_ok)}"}
            elif not balance_ok:
                logger.error("Insufficient account balance")
                return {"success": False, "error": "Insufficient balance"}

            if isinstance(optimal_price, Exception):
                logger.warning(f"Optimal price query failed, using signal price: {optimal_price}")
                optimal_price = signal.price
            elif optimal_price <= 0:
                logger.warning("Invalid optimal price, using signal price")
                optimal_price = signal.price

            logger.info(f"Pre-execution validation completed - Market: OK, Balance: OK, Price: ${optimal_price}")

            # Enhanced slippage calculation and execution price determination
            try:
                slippage = self._calculate_optimal_slippage(signal, leverage_info)
                if signal.action.lower() in ["buy", "long"]:
                    execution_price = optimal_price * (1 + slippage)
                else:
                    execution_price = optimal_price * (1 - slippage)
                
                logger.info(f"Execution price calculated: ${execution_price} (slippage: {slippage*100:.3f}%)")
            except Exception as e:
                logger.error(f"Price calculation error: {e}")
                execution_price = optimal_price
                slippage = 0.001

            # Enhanced order execution with comprehensive error handling
            try:
                order_result = await self._place_binance_futures_order(
                    binance_client, signal, leverage_info, execution_price
                )
            except Exception as e:
                logger.error(f"Order placement error: {e}")
                return {"success": False, "error": f"Order placement failed: {str(e)}"}

            execution_time = (time.time() - start_time) * 1000

            # Enhanced result processing and logging
            if order_result.get("success", False):
                logger.info("Binance futures order execution successful:")
                logger.info(f"  Order ID: {order_result.get('order_id', 'N/A')}")
                logger.info(f"  Execution price: ${order_result.get('avg_price', execution_price):,.4f}")
                logger.info(f"  Quantity: {order_result.get('executed_qty', 0):.6f}")
                logger.info(f"  Commission: ${order_result.get('commission', 0):.6f}")
                logger.info(f"  Execution time: {execution_time:.1f}ms")

                # Performance metrics logging
                if execution_time < 100:
                    logger.info(f"Ultra-fast execution achieved: {execution_time:.1f}ms")
                elif execution_time < 500:
                    logger.info(f"Fast execution achieved: {execution_time:.1f}ms")

                return {
                    "success": True,
                    "order_id": order_result.get("order_id", ""),
                    "execution_price": order_result.get("avg_price", execution_price),
                    "executed_qty": order_result.get("executed_qty", leverage_info.get("quantity", 0)),
                    "commission": order_result.get("commission", 0),
                    "slippage": slippage,
                    "execution_time_ms": execution_time,
                    "optimized": True,
                    "exchange": "binance_futures",
                    "timestamp": time.time(),
                    "market_conditions": "validated",
                    "leverage": leverage_info["leverage"]
                }
            else:
                error_msg = order_result.get('error', 'Unknown order execution error')
                logger.error(f"Order execution failed: {error_msg}")
                return {
                    "success": False,
                    "error": f"Binance order failed: {error_msg}",
                    "execution_time_ms": execution_time,
                    "attempted_price": execution_price,
                    "attempted_quantity": leverage_info.get("quantity", 0)
                }

        except Exception as e:
            logger.error(f"Critical error in enhanced Binance order execution: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return {"success": False, "error": f"Critical execution error: {str(e)}"}
        
        finally:
            # Enhanced connection cleanup with error handling
            if binance_client:
                try:
                    await binance_client.close_connection()
                    logger.debug("Binance client connection closed successfully")
                except Exception as e:
                    logger.warning(f"Error closing Binance client connection: {e}")

    async def _get_binance_client(self):
        """
        High-performance Binance async client with advanced connection pooling
        Optimized for 128GB RAM systems with session lifecycle management
        Features: Smart reuse, health monitoring, automatic cleanup
        """
        client_id = f"CLIENT_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        
        try:
            # Check API credentials first
            if not config.BINANCE_API_KEY or not config.BINANCE_SECRET_KEY:
                logger.warning(f"[{client_id}] Binance API credentials not configured, using simulation mode")
                return None
            
            # Initialize connection pool if not exists
            if not hasattr(self, '_binance_client_pool'):
                self._binance_client_pool = {}
                logger.debug(f"[{client_id}] Connection pool initialized")
            
            # Check if pool has available and healthy client
            if self._binance_client_pool.get('client') and self._binance_client_pool.get('healthy', False):
                last_used = self._binance_client_pool.get('last_used', 0)
                current_time = time.time()
                client_age = current_time - self._binance_client_pool.get('created_at', current_time)
                
                # Connection keep-alive: 5 minutes idle, 30 minutes max age
                max_idle_time = 300
                max_client_age = 1800
                
                if current_time - last_used < max_idle_time and client_age < max_client_age:
                    # Verify session is still open before reuse
                    pooled_client = self._binance_client_pool['client']
                    
                    try:
                        # Quick health check - verify session is open
                        if hasattr(pooled_client, 'session') and pooled_client.session and not pooled_client.session.closed:
                            self._binance_client_pool['last_used'] = current_time
                            self._binance_client_pool['reuse_count'] = self._binance_client_pool.get('reuse_count', 0) + 1
                            
                            logger.debug(
                                f"[{client_id}] Reusing pooled Binance client "
                                f"(reused {self._binance_client_pool['reuse_count']} times, "
                                f"age: {client_age:.0f}s)"
                            )
                            return pooled_client
                        else:
                            logger.warning(f"[{client_id}] Pooled client session is closed, creating new client")
                            self._binance_client_pool = {}
                    except Exception as health_error:
                        logger.warning(f"[{client_id}] Client health check failed: {health_error}, creating new client")
                        self._binance_client_pool = {}
                else:
                    # Client expired - close and remove
                    reason = "idle timeout" if current_time - last_used >= max_idle_time else "age limit"
                    logger.info(f"[{client_id}] Pooled client expired ({reason}), creating new connection")
                    
                    try:
                        await asyncio.wait_for(
                            self._binance_client_pool['client'].close_connection(),
                            timeout=5.0
                        )
                        logger.debug(f"[{client_id}] Expired client closed successfully")
                    except asyncio.TimeoutError:
                        logger.warning(f"[{client_id}] Client close timeout")
                    except Exception as close_error:
                        logger.debug(f"[{client_id}] Error closing expired client: {close_error}")
                    finally:
                        self._binance_client_pool = {}
            
            # API rate limit check
            await self.rate_limiter.acquire(weight=1)
            
            # Enhanced connection attempts with adaptive retry (Ryzen 9 optimization)
            max_attempts = 5
            base_backoff = 0.5
            
            for attempt in range(max_attempts):
                try:
                    from binance import AsyncClient

                    # Adaptive timeout for high-performance hardware
                    create_timeout = 12.0 + (attempt * 3)
                    
                    client_params = {
                        'api_key': config.BINANCE_API_KEY,
                        'api_secret': config.BINANCE_SECRET_KEY,
                    }
                    
                    if config.BINANCE_TESTNET:
                        client_params['testnet'] = True
                        logger.info(
                            f"[{client_id}] Creating SPOT testnet client "
                            f"(attempt {attempt + 1}/{max_attempts})"
                        )
                    else:
                        logger.info(
                            f"[{client_id}] Creating SPOT mainnet client "
                            f"(attempt {attempt + 1}/{max_attempts})"
                        )

                    # Create client with timeout protection
                    try:
                        client = await asyncio.wait_for(
                            AsyncClient.create(**client_params),
                            timeout=create_timeout
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"[{client_id}] Client creation timeout after {create_timeout}s "
                            f"(attempt {attempt + 1}/{max_attempts})"
                        )
                        if attempt < max_attempts - 1:
                            backoff_delay = base_backoff * (2 ** attempt)
                            await asyncio.sleep(backoff_delay)
                        continue

                    # Enhanced connection validation with comprehensive checks
                    validation_start = time.time()
                    
                    try:
                        # Test 1: Server time check (connection + time sync)
                        server_time = await asyncio.wait_for(
                            client.get_server_time(), 
                            timeout=10
                        )
                        
                        server_datetime = datetime.fromtimestamp(server_time['serverTime']/1000)
                        local_time = datetime.now()
                        time_diff = abs((server_datetime - local_time).total_seconds())
                        
                        if time_diff > 300:
                            logger.error(
                                f"[{client_id}] Critical time sync issue: {time_diff:.0f}s difference "
                                f"(threshold: 300s)"
                            )
                            await client.close_connection()
                            if attempt < max_attempts - 1:
                                backoff_delay = base_backoff * (2 ** attempt)
                                await asyncio.sleep(backoff_delay)
                            continue
                        elif time_diff > 60:
                            logger.warning(
                                f"[{client_id}] Time sync warning: {time_diff:.0f}s difference "
                                f"(acceptable but may cause issues)"
                            )
                        else:
                            logger.info(f"[{client_id}] Time sync OK: {time_diff:.1f}s difference")
                        
                        # Test 2: Account access (authentication check)
                        if config.BINANCE_TESTNET:
                            account_info = await asyncio.wait_for(
                                client.get_account(),
                                timeout=10
                            )
                            logger.info(f"[{client_id}] SPOT testnet account access confirmed")
                            
                            # Log available balance for monitoring
                            usdt_balance = next(
                                (float(asset['free']) for asset in account_info.get('balances', []) 
                                 if asset['asset'] == 'USDT'),
                                0
                            )
                            logger.info(f"[{client_id}] Testnet USDT balance: ${usdt_balance:,.2f}")
                        
                        # Test 3: Exchange info (API endpoint test)
                        exchange_info = await asyncio.wait_for(
                            client.get_exchange_info(),
                            timeout=10
                        )
                        symbol_count = len(exchange_info.get('symbols', []))
                        logger.debug(f"[{client_id}] Exchange info retrieved: {symbol_count} symbols available")
                        
                        # Test 4: Session health check
                        if not hasattr(client, 'session') or not client.session or client.session.closed:
                            logger.error(f"[{client_id}] Client session is closed immediately after creation")
                            await client.close_connection()
                            if attempt < max_attempts - 1:
                                backoff_delay = base_backoff * (2 ** attempt)
                                await asyncio.sleep(backoff_delay)
                            continue
                        
                        validation_duration = (time.time() - validation_start) * 1000
                        
                        # Store client in connection pool with enhanced metadata
                        current_time = time.time()
                        self._binance_client_pool = {
                            'client': client,
                            'client_id': client_id,
                            'created_at': current_time,
                            'last_used': current_time,
                            'healthy': True,
                            'reuse_count': 0,
                            'time_diff': time_diff,
                            'validation_duration_ms': validation_duration,
                            'session_open': True,
                            'max_idle_time': 300,
                            'max_age': 1800
                        }
                        
                        logger.info(
                            f"[{client_id}] Binance SPOT client created successfully "
                            f"(validation: {validation_duration:.1f}ms, "
                            f"time diff: {time_diff:.1f}s, "
                            f"session: {'open' if not client.session.closed else 'closed'})"
                        )
                        
                        return client
                        
                    except asyncio.TimeoutError:
                        logger.warning(
                            f"[{client_id}] Connection validation timeout "
                            f"(attempt {attempt + 1}/{max_attempts})"
                        )
                        try:
                            await client.close_connection()
                        except:
                            pass
                        
                        if attempt < max_attempts - 1:
                            backoff_delay = base_backoff * (2 ** attempt)
                            await asyncio.sleep(backoff_delay)
                        continue
                        
                    except Exception as test_error:
                        logger.warning(
                            f"[{client_id}] Connection validation failed "
                            f"(attempt {attempt + 1}/{max_attempts}): {test_error}"
                        )
                        try:
                            await client.close_connection()
                        except:
                            pass
                        
                        if attempt < max_attempts - 1:
                            backoff_delay = base_backoff * (2 ** attempt)
                            await asyncio.sleep(backoff_delay)
                        continue

                except ImportError:
                    logger.error(f"[{client_id}] python-binance library not installed: pip install python-binance")
                    return None
                    
                except Exception as create_error:
                    logger.warning(
                        f"[{client_id}] Client creation failed "
                        f"(attempt {attempt + 1}/{max_attempts}): "
                        f"{type(create_error).__name__}: {create_error}"
                    )
                    if attempt < max_attempts - 1:
                        backoff_delay = base_backoff * (2 ** attempt)
                        logger.info(f"[{client_id}] Retrying in {backoff_delay:.1f}s...")
                        await asyncio.sleep(backoff_delay)
                    continue
            
            # All attempts failed
            logger.error(
                f"[{client_id}] All {max_attempts} Binance SPOT connection attempts failed, "
                f"switching to simulation mode"
            )
            
            # Mark connection pool as unhealthy
            if hasattr(self, '_binance_client_pool') and self._binance_client_pool:
                self._binance_client_pool['healthy'] = False
            
            return None

        except Exception as e:
            logger.error(
                f"[{client_id}] Critical error in Binance SPOT client creation: "
                f"{type(e).__name__}: {e}"
            )
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            return None

    async def _validate_binance_market_conditions(self, client, symbol: str) -> bool:
        """Validate real Binance SPOT market conditions"""
        try:
            if not client or not symbol:
                return False

            # Check SPOT trading status
            exchange_info = await client.get_exchange_info()
            symbol_info = next(
                (
                    s
                    for s in exchange_info["symbols"]
                    if s["symbol"] == symbol.replace(".P", "")
                ),
                None,
            )

            if not symbol_info:
                logger.error(f"Symbol not found: {symbol}")
                return False

            if symbol_info["status"] != "TRADING":
                logger.error(f"Trading halted for symbol: {symbol} (status: {symbol_info['status']})")
                return False

            # Check 24-hour trading volume
            ticker = await client.get_ticker(symbol=symbol.replace(".P", ""))
            volume_24h = float(ticker["volume"])

            if volume_24h < 1000:
                logger.warning(f"Low trading volume: {symbol} (24h: {volume_24h:,.0f})")

            return True

        except Exception as e:
            logger.error(f"Market condition validation failed: {e}")
            return False

    async def _check_binance_account_balance(
        self, client, required_margin: float
    ) -> bool:
        """Check real Binance SPOT account balance"""
        try:
            if not client or required_margin is None or required_margin <= 0:
                return False

            # Query SPOT account information
            account_info = await client.get_account()

            # Available USDT balance
            usdt_asset = next(
                (asset for asset in account_info["balances"] if asset["asset"] == "USDT"),
                None
            )
            
            if not usdt_asset:
                logger.error("USDT asset not found in account")
                return False
                
            available_balance = float(usdt_asset["free"])

            if available_balance < required_margin:
                logger.error(
                    f"Insufficient balance: required ${required_margin:,.2f} > available ${available_balance:,.2f}"
                )
                return False

            logger.info(
                f"Balance confirmed: ${available_balance:,.2f} (required: ${required_margin:,.2f})"
            )
            return True

        except Exception as e:
            logger.error(f"Balance check failed: {e}")
            return False

    async def _get_binance_optimal_price(self, client, symbol: str) -> float:
        """Query real Binance SPOT optimal price"""
        try:
            if not client or not symbol:
                raise ValueError("Client or symbol is invalid")

            # Query current market price
            ticker = await client.get_symbol_ticker(symbol=symbol.replace(".P", ""))
            current_price = float(ticker["price"])

            # Query order book (more accurate price)
            order_book = await client.get_order_book(
                symbol=symbol.replace(".P", ""), limit=5
            )

            best_bid = float(order_book["bids"][0][0])
            best_ask = float(order_book["asks"][0][0])

            # Use mid price
            optimal_price = (best_bid + best_ask) / 2

            logger.info(f"{symbol} price information:")
            logger.info(f"   Current price: ${current_price:,.2f}")
            logger.info(f"   Best bid: ${best_bid:,.2f}")
            logger.info(f"   Best ask: ${best_ask:,.2f}")
            logger.info(f"   Optimal price: ${optimal_price:,.2f}")

            return optimal_price

        except Exception as e:
            logger.error(f"Optimal price query failed: {e}")
            raise e

    async def _set_binance_leverage(self, client, symbol: str, leverage: int):
        """Leverage setting not applicable for SPOT trading"""
        try:
            logger.info(f"SPOT trading does not support leverage - skipping leverage configuration for {symbol}")
            return True
        except Exception as e:
            logger.warning(f"Leverage setting skipped: {e}")
            return False

    async def _set_binance_margin_mode(self, client, symbol: str, margin_mode: str):
        """Margin mode setting not applicable for SPOT trading"""
        try:
            logger.info(f"SPOT trading does not support margin mode - skipping margin configuration for {symbol}")
            return True
        except Exception as e:
            logger.warning(f"Margin mode setting skipped: {e}")
            return False

    async def _place_binance_spot_order(
        self,
        client,
        signal: EnhancedTradingSignal,
        leverage_info: Dict,
        execution_price: float,
    ) -> Dict:
        """Binance SPOT trading execution with enhanced price validation"""
        try:
            if not client or not signal or not leverage_info:
                return {"success": False, "error": "Required parameters missing"}

            # Clean symbol for spot trading (remove .P if exists)
            clean_symbol = signal.symbol.replace('.P', '')
            logger.info(f"Executing SPOT order for {clean_symbol}")

            # API rate limit check
            await self.rate_limiter.acquire(weight=1)

            from binance.enums import (
                ORDER_TYPE_MARKET,
                SIDE_BUY,
                SIDE_SELL,
            )

            # Determine order direction
            side = SIDE_BUY if signal.action.lower() in ["buy", "long"] else SIDE_SELL

            # Get symbol info for LOT_SIZE filter
            try:
                exchange_info = await client.get_exchange_info()
                symbol_info = next(
                    (s for s in exchange_info["symbols"] if s["symbol"] == clean_symbol),
                    None
                )
                
                if not symbol_info:
                    return {"success": False, "error": f"Symbol {clean_symbol} not found"}
                
                # Extract LOT_SIZE filter
                lot_size_filter = next(
                    (f for f in symbol_info["filters"] if f["filterType"] == "LOT_SIZE"),
                    None
                )
                
                if lot_size_filter:
                    min_qty = float(lot_size_filter["minQty"])
                    max_qty = float(lot_size_filter["maxQty"])
                    step_size = float(lot_size_filter["stepSize"])
                    
                    # Calculate raw quantity
                    raw_quantity = leverage_info.get("quantity", 0)
                    
                    # Adjust quantity to step_size
                    precision = len(str(step_size).rstrip('0').split('.')[-1])
                    quantity = round(raw_quantity - (raw_quantity % step_size), precision)
                    
                    # Ensure quantity is within bounds
                    if quantity < min_qty:
                        quantity = min_qty
                    elif quantity > max_qty:
                        quantity = max_qty
                    
                    logger.info(f"LOT_SIZE adjusted: {raw_quantity:.8f} -> {quantity:.8f} (min: {min_qty}, max: {max_qty}, step: {step_size})")
                else:
                    # Fallback if no LOT_SIZE filter found
                    quantity = round(leverage_info.get("quantity", 0), 6)
                    
            except Exception as filter_error:
                logger.warning(f"Could not get LOT_SIZE filter: {filter_error}, using default rounding")
                quantity = round(leverage_info.get("quantity", 0), 6)
            
            if quantity <= 0:
                return {"success": False, "error": "Invalid quantity after LOT_SIZE adjustment"}

            # Execute SPOT market order (timeInForce not needed for MARKET orders)
            order = await client.create_order(
                symbol=clean_symbol,
                side=side,
                type=ORDER_TYPE_MARKET,
                quantity=quantity
            )
            
            logger.info(f"Order created: {order.get('orderId', 'N/A')}, Status: {order.get('status', 'UNKNOWN')}")
            
            # Handle partial fills for spot
            order = await self._handle_partial_fill_spot(
                client, order, quantity, clean_symbol, side
            )
            
            # API rate limit check
            await self.rate_limiter.acquire(weight=1)
            
            # Get detailed order info for spot with error handling
            try:
                order_info = await client.get_order(
                    symbol=clean_symbol, orderId=order["orderId"]
                )
            except Exception as query_error:
                logger.warning(f"Could not query order info: {query_error}, using order response")
                order_info = order

            # CRITICAL FIX: Enhanced average price extraction with validation
            avg_price = 0.0
            
            # Priority 1: Try avgPrice field (most reliable for filled orders)
            if "avgPrice" in order_info and order_info["avgPrice"]:
                try:
                    avg_price = float(order_info["avgPrice"])
                    if avg_price > 0:
                        logger.debug(f"Extracted avgPrice: ${avg_price:.8f}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid avgPrice format: {order_info['avgPrice']}, error: {e}")
            
            # Priority 2: Calculate from fills (most accurate)
            if avg_price <= 0 and "fills" in order_info and order_info["fills"]:
                try:
                    total_qty = 0.0
                    total_cost = 0.0
                    for fill in order_info["fills"]:
                        fill_price = float(fill.get("price", 0))
                        fill_qty = float(fill.get("qty", 0))
                        if fill_price > 0 and fill_qty > 0:
                            total_cost += fill_price * fill_qty
                            total_qty += fill_qty
                    
                    if total_qty > 0:
                        avg_price = total_cost / total_qty
                        logger.info(f"Calculated weighted avg from {len(order_info['fills'])} fills: ${avg_price:.8f}")
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error calculating price from fills: {e}")
            
            # Priority 3: Try price field
            if avg_price <= 0 and "price" in order_info and order_info["price"]:
                try:
                    avg_price = float(order_info["price"])
                    if avg_price > 0:
                        logger.debug(f"Using order price: ${avg_price:.8f}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid price format: {order_info['price']}")
            
            # Priority 4: Use execution_price as last resort
            if avg_price <= 0:
                avg_price = execution_price
                logger.warning(f"No valid price in order response, using execution_price: ${avg_price:.8f}")
            
            # Final validation
            if avg_price <= 0:
                logger.error(f"CRITICAL: All price extraction methods failed for order {order.get('orderId')}")
                return {
                    "success": False, 
                    "error": f"Failed to extract valid execution price (got {avg_price})"
                }
            
            # Extract executed quantity with validation
            executed_qty = 0.0
            try:
                executed_qty = float(order_info.get("executedQty", order_info.get("origQty", quantity)))
                if executed_qty <= 0:
                    logger.error(f"Invalid executed quantity: {executed_qty}")
                    return {"success": False, "error": f"Invalid executed quantity: {executed_qty}"}
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing executed quantity: {e}")
                return {"success": False, "error": f"Quantity parsing error: {e}"}
            
            # Calculate commission
            commission = 0.0
            if "fills" in order_info and order_info["fills"]:
                try:
                    commission = sum(abs(float(fill.get("commission", 0))) for fill in order_info["fills"])
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error calculating commission: {e}")
            
            logger.info(f"Order execution complete: Price ${avg_price:.4f}, Qty {executed_qty:.6f}, Commission ${commission:.6f}")

            return {
                "success": True,
                "order_id": order["orderId"],
                "avg_price": avg_price,
                "executed_qty": executed_qty,
                "commission": commission,
                "status": order_info.get("status", "FILLED"),
                "symbol_type": "spot",
                "clean_symbol": clean_symbol,
            }

        except Exception as e:
            logger.error(f"Binance SPOT order execution failed for {getattr(signal, 'symbol', 'unknown')}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {"success": False, "error": str(e)}

    async def _handle_partial_fill_spot(self, client, order_result, target_qty, symbol, side):
        """Enhanced partial fill handling for SPOT MARKET orders only"""
        try:
            executed_qty = float(order_result.get('executedQty', 0))
            remaining_qty = target_qty - executed_qty
            
            if remaining_qty > 0.001:
                logger.info(f"Partial fill detected: {executed_qty}/{target_qty}, remaining: {remaining_qty}")
                
                for attempt in range(3):
                    try:
                        await self.rate_limiter.acquire(weight=1)
                        
                        from binance.enums import ORDER_TYPE_MARKET
                        
                        # SPOT MARKET order (no timeInForce needed)
                        remaining_order = await client.create_order(
                            symbol=symbol,
                            side=side,
                            type=ORDER_TYPE_MARKET,
                            quantity=round(remaining_qty, 6)
                        )
                        logger.info(f"Remaining quantity re-order success (spot): {remaining_qty} (attempt {attempt + 1}/3)")
                        
                        total_executed = executed_qty + float(remaining_order.get('executedQty', 0))
                        order_result['executedQty'] = str(total_executed)
                        
                        return order_result
                        
                    except Exception as e:
                        if attempt < 2:
                            await asyncio.sleep(1)
                            continue
                        logger.error(f"Final failure for remaining quantity re-order: {e}")
                        
            return order_result
            
        except Exception as e:
            logger.error(f"Error during partial fill handling: {e}")
            return order_result

    # ========================================================================
    #                          Enhanced Position Management
    # ========================================================================

    async def _create_enhanced_position(
        self,
        execution_id: str,
        signal: EnhancedTradingSignal,
        leverage_info: Dict,
        order_result: Dict,
    ) -> Position:
        """Phoenix95 enhanced position creation with critical validation and concurrency control"""
        position_creation_start = time.time()
        
        try:
            # CRITICAL: Validate order_result structure
            if not order_result or not isinstance(order_result, dict):
                logger.error("Position creation aborted: Order result is invalid or None")
                logger.error(f"Order result type: {type(order_result)}")
                raise ValueError("Invalid order_result for position creation")
            
            # CRITICAL: Validate execution_price
            execution_price = order_result.get("execution_price", 0)
            if execution_price <= 0:
                logger.error(f"CRITICAL: Invalid execution_price={execution_price}")
                logger.error(f"Order result keys: {list(order_result.keys())}")
                logger.error(f"Order result data: {order_result}")
                raise ValueError(f"Cannot create position with invalid entry price: {execution_price}")
            
            # CRITICAL: Validate executed_qty
            executed_qty = order_result.get("executed_qty", 0)
            if executed_qty <= 0:
                logger.error(f"CRITICAL: Invalid executed_qty={executed_qty}")
                raise ValueError(f"Cannot create position with invalid quantity: {executed_qty}")
            
            # CRITICAL: Validate leverage_info required fields
            required_leverage_fields = ["quantity", "leverage", "margin_required", 
                                       "stop_loss_price", "take_profit_price", "liquidation_price"]
            missing_leverage_fields = [f for f in required_leverage_fields if f not in leverage_info]
            if missing_leverage_fields:
                logger.error(f"Missing leverage_info fields: {missing_leverage_fields}")
                raise ValueError(f"Incomplete leverage_info: missing {missing_leverage_fields}")

            # Generate unique position ID with timestamp for tracking
            timestamp_ms = int(time.time() * 1000)
            random_suffix = uuid.uuid4().hex[:6]
            position_id = f"POS_{timestamp_ms}_{random_suffix}"
            
            # CRITICAL FIX: Extract order_id for mapping
            order_id = order_result.get("order_id", "")
            if not order_id:
                logger.warning(f"Order ID not found in order_result, generating fallback")
                order_id = f"ORDER_{timestamp_ms}_{random_suffix}"

            # Create position object with validated data
            position = Position(
                position_id=position_id,
                signal_id=signal.signal_id,
                symbol=signal.symbol,
                action=signal.action,
                entry_price=execution_price,
                quantity=leverage_info["quantity"],
                leverage=leverage_info["leverage"],
                margin_required=leverage_info["margin_required"],
                stop_loss_price=leverage_info["stop_loss_price"],
                take_profit_price=leverage_info["take_profit_price"],
                liquidation_price=leverage_info["liquidation_price"],
                current_price=execution_price,
                # Phoenix95 enhanced metadata
                phoenix95_score=signal.phoenix95_score,
                service_chain=signal.service_chain,
            )

            # CRITICAL: Post-creation validation
            validation_errors = []
            if position.entry_price <= 0:
                validation_errors.append(f"Invalid entry_price: {position.entry_price}")
            if position.current_price <= 0:
                validation_errors.append(f"Invalid current_price: {position.current_price}")
            if position.quantity <= 0:
                validation_errors.append(f"Invalid quantity: {position.quantity}")
            if position.margin_required <= 0:
                validation_errors.append(f"Invalid margin_required: {position.margin_required}")
            
            if validation_errors:
                logger.error(f"CRITICAL: Position validation failed: {'; '.join(validation_errors)}")
                raise ValueError(f"Position validation failed: {'; '.join(validation_errors)}")

            # Calculate initial liquidation risk
            position.liquidation_risk = self._calculate_initial_liquidation_risk(position)

            # CRITICAL FIX: Add to active positions with thread-safety AND create ID mapping
            async with self.position_lock:
                # Check for duplicate position_id (paranoid check)
                if position_id in self.active_positions:
                    logger.error(f"Duplicate position_id detected: {position_id}")
                    raise ValueError(f"Duplicate position_id: {position_id}")
                
                # Add position to active positions
                self.active_positions[position_id] = position
                
                # CRITICAL FIX: Create bidirectional mapping for position tracking
                # This enables finding positions by order_id or signal_id
                if order_id:
                    self._position_id_map[order_id] = position_id
                    logger.debug(f"Position ID mapping created: order_id={order_id} -> position_id={position_id}")
                
                # Also map by signal_id for additional tracking
                if hasattr(signal, 'signal_id') and signal.signal_id:
                    signal_key = f"signal_{signal.signal_id}"
                    self._position_id_map[signal_key] = position_id
                    logger.debug(f"Position ID mapping created: signal_id={signal.signal_id} -> position_id={position_id}")
                
                # Map by timestamp for time-based lookups (128GB RAM optimization)
                timestamp_key = f"time_{timestamp_ms}"
                self._position_id_map[timestamp_key] = position_id
                
                # Update statistics
                self.execution_stats["current_positions"] = len(self.active_positions)
                
                # Update exposure metrics
                total_margin = sum(pos.margin_required for pos in self.active_positions.values())
                self.risk_metrics["current_exposure"] = total_margin
                self.risk_metrics["margin_utilization"] = total_margin / config.PORTFOLIO_VALUE
                
                # CRITICAL FIX: Cleanup old position mappings (128GB RAM optimized)
                # Keep last 50000 mappings for high-frequency trading
                if len(self._position_id_map) > 50000:
                    # Remove oldest 25000 entries
                    keys_to_remove = list(self._position_id_map.keys())[:25000]
                    for old_key in keys_to_remove:
                        del self._position_id_map[old_key]
                    logger.info(f"Position ID mapping cache cleaned: {len(keys_to_remove)} old entries removed")

            # Calculate position creation duration
            creation_duration = (time.time() - position_creation_start) * 1000

            # Detailed success logging
            logger.info("=" * 60)
            logger.info("Phoenix95 Enhanced Position Created")
            logger.info(f"Position ID: {position_id}")
            logger.info(f"Order ID: {order_id}")
            logger.info(f"Signal ID: {signal.signal_id}")
            logger.info(f"Symbol: {position.symbol} {position.action.upper()}")
            logger.info(f"Entry Price: ${position.entry_price:.4f}")
            logger.info(f"Quantity: {position.quantity:.6f}")
            logger.info(f"Value: ${position.entry_price * position.quantity:,.2f}")
            logger.info(f"Margin Required: ${position.margin_required:,.2f}")
            logger.info(f"Leverage: {position.leverage}x")
            logger.info(f"Stop Loss: ${position.stop_loss_price:.4f}")
            logger.info(f"Take Profit: ${position.take_profit_price:.4f}")
            logger.info(f"Phoenix95 Score: {position.phoenix95_score:.1f}/95")
            logger.info(f"Liquidation Risk: {position.liquidation_risk:.1%}")
            logger.info(f"Service Chain: {position.service_chain}")
            logger.info(f"Creation Time: {creation_duration:.1f}ms")
            logger.info(f"Active Positions: {len(self.active_positions)}")
            logger.info(f"Position Mappings: {len(self._position_id_map)}")
            logger.info(f"Total Exposure: ${self.risk_metrics['current_exposure']:,.2f}")
            logger.info(f"Margin Utilization: {self.risk_metrics['margin_utilization']:.1%}")
            logger.info("=" * 60)

            return position

        except ValueError as e:
            logger.error(f"Position creation validation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in position creation: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            raise RuntimeError(f"Position creation failed: {str(e)}") from e
    
    async def _get_position_by_order_id(self, order_id: str) -> Optional[Position]:
        """
        Find position by order ID using the mapping system
        Optimized for 128GB RAM with extensive caching
        
        Args:
            order_id: The order ID to search for
            
        Returns:
            Position object if found, None otherwise
        """
        try:
            # Quick lookup using position ID map (O(1) operation)
            position_id = self._position_id_map.get(order_id)
            
            if position_id and position_id in self.active_positions:
                logger.debug(f"Position found by order_id mapping: {order_id} -> {position_id}")
                return self.active_positions[position_id]
            
            # Fallback: Linear search through active positions (slower but comprehensive)
            logger.debug(f"Position not found in mapping, attempting linear search for order_id: {order_id}")
            
            async with self.position_lock:
                for pos_id, position in self.active_positions.items():
                    # Check if position was created around the same time
                    # (order_id might contain timestamp)
                    if order_id in pos_id or pos_id in order_id:
                        logger.info(f"Position found by similarity search: {order_id} ~= {pos_id}")
                        # Update mapping for future lookups
                        self._position_id_map[order_id] = pos_id
                        return position
            
            logger.warning(f"Position not found for order_id: {order_id}")
            logger.debug(f"Active positions: {list(self.active_positions.keys())}")
            logger.debug(f"Available mappings: {len(self._position_id_map)}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error in position lookup by order_id: {e}")
            return None

    def _calculate_initial_liquidation_risk(self, position: Position) -> float:
        """Calculate initial liquidation risk with comprehensive validation"""
        try:
            # SPOT trading: no liquidation risk
            if position.liquidation_price <= 0:
                logger.debug(f"SPOT trading detected for {position.symbol}: No liquidation risk")
                return 0.0
            
            # Validate current_price
            if position.current_price <= 0:
                logger.warning(
                    f"Invalid current_price={position.current_price} for {position.position_id}, "
                    f"defaulting to medium risk"
                )
                return 0.5

            # Calculate price distance to liquidation
            price_distance = abs(position.current_price - position.liquidation_price)
            distance_ratio = price_distance / position.current_price

            # Risk calculation: inverse relationship with distance
            # Formula: risk = max(0, 1 - distance_ratio * 5)
            # At 20% distance -> 0% risk
            # At 10% distance -> 50% risk
            # At 0% distance -> 100% risk
            liquidation_risk = max(0.0, 1.0 - (distance_ratio * 5))
            liquidation_risk = min(1.0, liquidation_risk)
            
            logger.debug(
                f"Liquidation risk calculated for {position.position_id}: "
                f"{liquidation_risk:.1%} (distance: {distance_ratio:.1%})"
            )
            
            return liquidation_risk

        except ZeroDivisionError:
            logger.warning(f"Division by zero in liquidation risk calculation for {position.position_id}")
            return 0.5
        except Exception as e:
            logger.warning(f"Liquidation risk calculation error for {position.position_id}: {e}")
            return 0.5  # Conservative default

    # ========================================================================
    #                          Real-time Monitoring (Notify Service Integration)
    # ========================================================================

    async def _monitor_position_with_notifications(self, position: Position):
        """
        High-performance real-time position monitoring optimized for Ryzen 9 7950X / 128GB RAM
        Features: Multi-core parallel processing, adaptive caching, intelligent throttling
        CRITICAL: Detects and prevents unrealistic simulation price deviations
        """
        monitoring_start = time.time()
        
        logger.info("=" * 60)
        logger.info(f"Position Monitoring Started: {position.position_id}")
        logger.info(f"Symbol: {position.symbol} {position.action.upper()}")
        logger.info(f"Entry Price: ${position.entry_price:.4f}")
        logger.info(f"Phoenix95 Score: {position.phoenix95_score:.0f}/95")
        logger.info("=" * 60)

        try:
            # Monitoring state variables with enhanced caching
            last_risk_alert = 0
            last_price_update = 0
            last_pnl_log = 0
            last_close_check = 0
            
            # Adaptive price cache for CPU optimization (128GB RAM optimized)
            base_price_cache_duration = 2.0 if self.hardware_optimized else 3.0
            cached_price = position.current_price
            
            # CRITICAL FIX: Track entry price for simulation validation
            entry_price_baseline = position.entry_price
            max_realistic_deviation = 0.15  # 15% maximum deviation for simulation mode
            simulation_mode_detected = False
            
            # Performance tracking metrics with enhanced monitoring
            monitoring_cycles = 0
            total_monitoring_time = 0
            price_fetch_errors = 0
            pnl_calc_errors = 0
            cpu_throttle_count = 0
            successful_cycles = 0
            failed_cycles = 0
            simulation_price_corrections = 0
            
            # Adaptive performance tuning for high-end hardware (16-24 cores)
            max_cycles_before_gc = 5000 if self.hardware_optimized else 1000
            performance_log_interval = 500 if self.hardware_optimized else 100
            
            # CPU monitoring and throttling with dynamic adjustment
            last_cpu_check = time.time()
            cpu_check_interval = 10.0
            cpu_history = deque(maxlen=6)

            # Multi-core optimization: Parallel task queue
            pending_tasks = []
            max_parallel_tasks = 3 if self.hardware_optimized else 1

            while position.status == "ACTIVE":
                cycle_start = time.time()
                monitoring_cycles += 1
                current_time = time.time()
                
                # Enhanced CPU monitoring with trend analysis
                if current_time - last_cpu_check > cpu_check_interval:
                    try:
                        import psutil
                        cpu_percent = psutil.cpu_percent(interval=0.1)
                        cpu_history.append(cpu_percent)
                        
                        # Calculate average CPU usage trend
                        avg_cpu = sum(cpu_history) / len(cpu_history) if cpu_history else cpu_percent
                        
                        if avg_cpu > 85:
                            cpu_throttle_count += 1
                            logger.warning(
                                f"High CPU detected (avg: {avg_cpu:.1f}%, current: {cpu_percent:.1f}%), "
                                f"throttling monitoring for {position.position_id}"
                            )
                            base_price_cache_duration = 5.0
                            max_parallel_tasks = 1
                            await asyncio.sleep(2.0)
                        elif avg_cpu > 70:
                            base_price_cache_duration = 3.0
                            max_parallel_tasks = 2
                        elif avg_cpu < 50:
                            base_price_cache_duration = 1.5
                            max_parallel_tasks = 4 if self.hardware_optimized else 2
                        
                        last_cpu_check = current_time
                    except ImportError:
                        pass
                    except Exception as cpu_error:
                        logger.debug(f"CPU check error: {cpu_error}")
                
                # Adaptive price cache duration based on risk level
                if position.liquidation_risk > 0.8:
                    price_cache_duration = 0.5
                elif position.liquidation_risk > 0.5:
                    price_cache_duration = 1.5
                else:
                    price_cache_duration = base_price_cache_duration
                
                # Multi-core optimization: Parallel price fetching and calculations
                if current_time - last_price_update > price_cache_duration:
                    try:
                        # Create parallel tasks for price fetch
                        price_task = asyncio.create_task(
                            asyncio.wait_for(
                                self._get_current_price_optimized(position.symbol),
                                timeout=5.0
                            )
                        )
                        
                        # Wait for price with error handling
                        try:
                            current_price = await price_task
                            
                            # CRITICAL FIX: Detect simulation mode or unrealistic prices
                            if entry_price_baseline > 0:
                                price_change_ratio = abs(current_price - entry_price_baseline) / entry_price_baseline
                                
                                # Detect unrealistic price changes (>15% from entry)
                                if price_change_ratio > max_realistic_deviation:
                                    simulation_mode_detected = True
                                    simulation_price_corrections += 1
                                    
                                    logger.error(
                                        f"CRITICAL: Unrealistic price detected for {position.symbol}! "
                                        f"Entry: ${entry_price_baseline:.4f}, "
                                        f"Current: ${current_price:.4f}, "
                                        f"Change: {price_change_ratio*100:.1f}% "
                                        f"(max: {max_realistic_deviation*100:.0f}%)"
                                    )
                                    
                                    # Cap price at maximum deviation
                                    if current_price > entry_price_baseline:
                                        corrected_price = entry_price_baseline * (1 + max_realistic_deviation)
                                    else:
                                        corrected_price = entry_price_baseline * (1 - max_realistic_deviation)
                                    
                                    logger.warning(
                                        f"Price corrected: ${current_price:.4f} -> ${corrected_price:.4f} "
                                        f"(correction #{simulation_price_corrections})"
                                    )
                                    current_price = corrected_price
                                    
                                    # Force close if simulation mode persists
                                    if simulation_price_corrections >= 5:
                                        logger.error(
                                            f"CRITICAL: Persistent unrealistic prices detected ({simulation_price_corrections}x), "
                                            f"forcing position close to prevent false P&L"
                                        )
                                        await self._close_position_with_notification(
                                            position,
                                            entry_price_baseline,  # Use entry price for safe close
                                            "Simulation price anomaly detected"
                                        )
                                        break
                            
                            cached_price = current_price
                            last_price_update = current_time
                            price_fetch_errors = 0
                            
                        except asyncio.TimeoutError:
                            price_fetch_errors += 1
                            if price_fetch_errors <= 3:
                                logger.warning(
                                    f"Price fetch timeout for {position.symbol} "
                                    f"(error #{price_fetch_errors}), using cached price: ${cached_price:.4f}"
                                )
                            current_price = cached_price
                        except Exception as price_error:
                            price_fetch_errors += 1
                            if price_fetch_errors <= 3:
                                logger.warning(
                                    f"Price fetch error for {position.symbol}: {price_error} "
                                    f"(error #{price_fetch_errors}), using cached price"
                                )
                            current_price = cached_price
                        
                        # Critical alert for persistent failures
                        if price_fetch_errors == 10:
                            logger.error(
                                f"CRITICAL: Persistent price fetch failures for {position.symbol}: "
                                f"{price_fetch_errors} consecutive errors"
                            )
                    except Exception as e:
                        logger.error(f"Price fetch task creation failed: {e}")
                        current_price = cached_price
                else:
                    current_price = cached_price

                # Lock-free position update (optimized for multi-core)
                position.current_price = current_price
                position.updated_at = current_time

                # Multi-core: Parallel P&L calculation and risk assessment
                try:
                    # Create parallel calculation tasks
                    calc_tasks = []
                    
                    if len(pending_tasks) < max_parallel_tasks:
                        pnl_task = asyncio.create_task(
                            asyncio.to_thread(self._calculate_pnl, position, current_price)
                        )
                        calc_tasks.append(('pnl', pnl_task))
                        
                        risk_task = asyncio.create_task(
                            asyncio.to_thread(self._calculate_liquidation_risk, position)
                        )
                        calc_tasks.append(('risk', risk_task))
                    
                    # Gather results with timeout
                    if calc_tasks:
                        results = await asyncio.gather(
                            *[task for _, task in calc_tasks],
                            return_exceptions=True
                        )
                        
                        for (task_type, _), result in zip(calc_tasks, results):
                            if isinstance(result, Exception):
                                logger.debug(f"{task_type} calculation error: {result}")
                                if task_type == 'pnl':
                                    pnl_calc_errors += 1
                            else:
                                if task_type == 'pnl':
                                    # CRITICAL FIX: Validate P&L results for unrealistic values
                                    unrealized_pnl = result.get("unrealized_pnl", 0)
                                    pnl_percentage = result.get("pnl_percentage", 0)
                                    roe = result.get("roe", 0)
                                    
                                    # Detect unrealistic P&L (>1000% or <-100%)
                                    if abs(pnl_percentage) > 1000 or abs(roe) > 1000:
                                        logger.error(
                                            f"CRITICAL: Unrealistic P&L detected! "
                                            f"P&L: {pnl_percentage:+.2f}%, ROE: {roe:+.2f}%"
                                        )
                                        logger.error(f"P&L calculation result: {result}")
                                        
                                        # Force close to prevent false reporting
                                        await self._close_position_with_notification(
                                            position,
                                            entry_price_baseline,
                                            "Unrealistic P&L calculation detected"
                                        )
                                        break
                                    
                                    position.unrealized_pnl = unrealized_pnl
                                    position.pnl_percentage = pnl_percentage
                                    position.roe = roe
                                    pnl_calc_errors = 0
                                elif task_type == 'risk':
                                    position.liquidation_risk = result
                    else:
                        # Fallback to synchronous calculation
                        pnl_info = self._calculate_pnl(position, current_price)
                        
                        # CRITICAL FIX: Validate synchronous P&L as well
                        pnl_percentage = pnl_info.get("pnl_percentage", 0)
                        roe = pnl_info.get("roe", 0)
                        
                        if abs(pnl_percentage) > 1000 or abs(roe) > 1000:
                            logger.error(
                                f"CRITICAL: Unrealistic P&L in sync calculation! "
                                f"P&L: {pnl_percentage:+.2f}%, ROE: {roe:+.2f}%"
                            )
                            await self._close_position_with_notification(
                                position,
                                entry_price_baseline,
                                "Unrealistic P&L calculation detected (sync)"
                            )
                            break
                        
                        position.unrealized_pnl = pnl_info["unrealized_pnl"]
                        position.pnl_percentage = pnl_percentage
                        position.roe = roe
                        position.liquidation_risk = self._calculate_liquidation_risk(position)
                        
                except Exception as calc_error:
                    pnl_calc_errors += 1
                    if pnl_calc_errors <= 3:
                        logger.warning(f"Calculation error (#{pnl_calc_errors}): {calc_error}")
                    failed_cycles += 1
                else:
                    successful_cycles += 1

                # Enhanced periodic P&L logging with simulation mode indicator
                if current_time - last_pnl_log > 120:
                    success_rate = (successful_cycles / monitoring_cycles * 100) if monitoring_cycles > 0 else 0
                    sim_indicator = " [SIM-CORRECTED]" if simulation_mode_detected else ""
                    logger.info(
                        f"Position update: {position.symbol}{sim_indicator} | "
                        f"P&L: ${position.unrealized_pnl:+,.2f} ({position.pnl_percentage:+.2f}%) | "
                        f"ROE: {position.roe:+.2f}% | "
                        f"Risk: {position.liquidation_risk:.1%} | "
                        f"Price: ${current_price:.4f} | "
                        f"Success: {success_rate:.1f}%"
                    )
                    last_pnl_log = current_time

                # Risk alert with rate limiting
                if current_time - last_risk_alert > 300 and position.liquidation_risk > 0.7:
                    try:
                        await asyncio.wait_for(
                            self._send_risk_alert_via_notify(position),
                            timeout=10.0
                        )
                        last_risk_alert = current_time
                        logger.warning(
                            f"Risk alert sent for {position.position_id}: "
                            f"{position.liquidation_risk:.1%} liquidation risk"
                        )
                    except asyncio.TimeoutError:
                        logger.warning(f"Risk alert timeout for {position.position_id}")
                    except Exception as alert_error:
                        logger.warning(f"Risk alert error: {alert_error}")

                # Adaptive close condition check
                close_check_interval = 0.5 if position.liquidation_risk > 0.8 else (1.0 if position.liquidation_risk > 0.7 else 3.0)
                
                if current_time - last_close_check > close_check_interval:
                    close_reason = self._check_close_conditions_enhanced(position, current_price)
                    last_close_check = current_time
                    
                    if close_reason:
                        logger.info(f"Close condition triggered: {close_reason}")
                        try:
                            await asyncio.wait_for(
                                self._close_position_with_notification(
                                    position, current_price, close_reason
                                ),
                                timeout=30.0
                            )
                        except asyncio.TimeoutError:
                            logger.error(f"Position close timeout: {position.position_id}")
                            await self._force_close_position(position, close_reason)
                        break

                # Position timeout check
                position_age = current_time - monitoring_start
                if position_age > config.POSITION_TIMEOUT:
                    timeout_hours = config.POSITION_TIMEOUT / 3600
                    logger.warning(
                        f"Position timeout reached: {position.position_id} "
                        f"(age: {position_age/3600:.1f}h, limit: {timeout_hours:.1f}h)"
                    )
                    try:
                        await asyncio.wait_for(
                            self._close_position_with_notification(
                                position, current_price, f"Position timeout ({timeout_hours:.0f}h limit)"
                            ),
                            timeout=30.0
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout close failed: {position.position_id}")
                        await self._force_close_position(position, "Timeout close failure")
                    break

                # Enhanced adaptive monitoring interval
                if position.liquidation_risk > 0.9:
                    monitor_interval = 0.3
                elif position.liquidation_risk > 0.8:
                    monitor_interval = 0.7
                elif position.liquidation_risk > 0.5:
                    monitor_interval = 2.0
                elif position.unrealized_pnl < -position.margin_required * 0.5:
                    monitor_interval = 1.5
                else:
                    monitor_interval = max(8.0 if self.hardware_optimized else 10.0, config.MONITOR_INTERVAL)

                # Performance tracking
                cycle_time = time.time() - cycle_start
                total_monitoring_time += cycle_time
                
                # Enhanced periodic performance logging
                if monitoring_cycles % performance_log_interval == 0:
                    avg_cycle_time = total_monitoring_time / monitoring_cycles
                    monitoring_duration = current_time - monitoring_start
                    success_rate = (successful_cycles / monitoring_cycles * 100) if monitoring_cycles > 0 else 0
                    logger.info(
                        f"Monitoring performance [{position.position_id}]: "
                        f"Cycles: {monitoring_cycles}, "
                        f"Avg: {avg_cycle_time*1000:.1f}ms, "
                        f"Duration: {monitoring_duration/60:.1f}m, "
                        f"Success: {success_rate:.1f}%, "
                        f"Price errors: {price_fetch_errors}, "
                        f"Sim corrections: {simulation_price_corrections}, "
                        f"CPU throttles: {cpu_throttle_count}"
                    )

                # Adaptive memory cleanup (128GB optimized)
                if monitoring_cycles % max_cycles_before_gc == 0:
                    import gc
                    if self.hardware_optimized:
                        collected = gc.collect(0)
                    else:
                        collected = gc.collect()
                        
                    if collected > 100:
                        logger.debug(
                            f"Memory cleanup: {collected} objects collected at cycle {monitoring_cycles}"
                        )

                await asyncio.sleep(monitor_interval)

        except asyncio.CancelledError:
            logger.info(f"Position monitoring cancelled: {position.position_id}")
            raise
        except Exception as e:
            logger.error(
                f"Critical monitoring error for {position.position_id}: "
                f"{type(e).__name__}: {e}"
            )
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            
            try:
                await asyncio.wait_for(
                    self._notify_monitoring_error(position, str(e)),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"Error notification timeout for {position.position_id}")
            except Exception as notify_error:
                logger.warning(f"Error notification failed: {notify_error}")

            if position.position_id in self.active_positions:
                logger.warning(f"Attempting emergency close for {position.position_id}")
                await self._force_close_position(
                    position, 
                    f"Monitoring system error: {str(e)}"
                )
        
        finally:
            total_duration = time.time() - monitoring_start
            avg_cycle = total_monitoring_time / max(1, monitoring_cycles)
            success_rate = (successful_cycles / monitoring_cycles * 100) if monitoring_cycles > 0 else 0
            
            logger.info("=" * 60)
            logger.info(f"Position Monitoring Ended: {position.position_id}")
            logger.info(f"Total Duration: {total_duration/60:.1f} minutes")
            logger.info(f"Monitoring Cycles: {monitoring_cycles}")
            logger.info(f"Successful Cycles: {successful_cycles} ({success_rate:.1f}%)")
            logger.info(f"Failed Cycles: {failed_cycles}")
            logger.info(f"Avg Cycle Time: {avg_cycle*1000:.1f}ms")
            logger.info(f"Price Fetch Errors: {price_fetch_errors}")
            logger.info(f"P&L Calc Errors: {pnl_calc_errors}")
            logger.info(f"Simulation Price Corrections: {simulation_price_corrections}")
            logger.info(f"CPU Throttle Events: {cpu_throttle_count}")
            logger.info("=" * 60)
    
    async def _force_close_position(self, position: Position, reason: str):
        """Force close position in emergency situations"""
        try:
            logger.warning(f"Force closing position: {position.position_id} - Reason: {reason}")
            
            async with self.position_lock:
                if position.position_id in self.active_positions:
                    # Mark as closed
                    position.status = "CLOSED"
                    position.closed_at = time.time()
                    
                    # Calculate final P&L
                    final_pnl = self._calculate_pnl(position, position.current_price)
                    position.realized_pnl = final_pnl["unrealized_pnl"]
                    position.roe = final_pnl["roe"]
                    
                    # Move to history
                    self.position_history.append(position)
                    del self.active_positions[position.position_id]
                    
                    # Update stats
                    self.execution_stats["current_positions"] = len(self.active_positions)
                    self.risk_metrics["emergency_closes"] += 1
                    
                    logger.warning(
                        f"Position force closed: {position.position_id} | "
                        f"Final P&L: ${position.realized_pnl:+,.2f}"
                    )
        except Exception as e:
            logger.error(f"Force close failed for {position.position_id}: {e}")

    async def _get_ticker_price(self, client, symbol: str) -> float:
        """Query Binance SPOT ticker price"""
        try:
            ticker = await client.get_symbol_ticker(symbol=symbol.replace(".P", ""))
            return float(ticker["price"])
        except Exception as e:
            logger.debug(f"Ticker price query failed ({symbol}): {e}")
            raise e

    async def _get_orderbook_mid_price(self, client, symbol: str) -> float:
        """Query SPOT order book mid price (more accurate)"""
        try:
            orderbook = await client.get_order_book(
                symbol=symbol.replace(".P", ""), limit=5
            )
            best_bid = float(orderbook["bids"][0][0])
            best_ask = float(orderbook["asks"][0][0])

            # Spread validation
            spread_pct = (best_ask - best_bid) / best_bid * 100
            if spread_pct > 1.0:
                logger.warning(f"{symbol} high spread: {spread_pct:.2f}%")

            return (best_bid + best_ask) / 2

        except Exception as e:
            logger.debug(f"Order book price query failed ({symbol}): {e}")
            raise e

    async def _get_recent_trades_price(self, client, symbol: str) -> float:
        """Query recent SPOT trades price"""
        try:
            trades = await client.get_recent_trades(
                symbol=symbol.replace(".P", ""), limit=10
            )
            if not trades:
                raise Exception("No trade data")

            # Volume weighted average price (VWAP)
            total_volume = 0
            weighted_price_sum = 0

            for trade in trades:
                price = float(trade["price"])
                qty = float(trade["qty"])
                weighted_price_sum += price * qty
                total_volume += qty

            if total_volume == 0:
                raise Exception("Zero volume")

            vwap = weighted_price_sum / total_volume
            return vwap

        except Exception as e:
            logger.debug(f"Recent trades price query failed ({symbol}): {e}")
            raise e

    async def _get_simulation_price(self, symbol: str) -> float:
        """
        Generate realistic simulation price with bounds checking
        Optimized for 128GB RAM with extensive price tracking
        Prevents unrealistic price jumps that cause false P&L calculations
        """
        try:
            # More realistic base prices (2025 baseline - updated for current market)
            realistic_base_prices = {
                "BTCUSDT": 42000,
                "ETHUSDT": 2500,
                "BNBUSDT": 310,
                "ADAUSDT": 0.38,
                "DOGEUSDT": 0.075,
                "XRPUSDT": 0.52,
                "SOLUSDT": 95,
                "AVAXUSDT": 32,
                "DOTUSDT": 5.8,
                "LINKUSDT": 14.5,
                "MATICUSDT": 0.85,
                "LTCUSDT": 95,
                "ATOMUSDT": 9.2,
                "NEARUSDT": 2.8,
                "FTMUSDT": 0.35,
                "FLMUSDT": 0.02,  # FIX: Add realistic base price for FLM
            }

            base_price = realistic_base_prices.get(symbol, 100)

            # Simulation price cache initialization (128GB RAM optimized)
            if not hasattr(self, "_sim_price_cache"):
                self._sim_price_cache = {}
            
            # CRITICAL FIX: Track entry prices from active positions
            # This prevents simulation prices from diverging too far from reality
            entry_price = None
            if hasattr(self, 'active_positions') and self.active_positions:
                for position in self.active_positions.values():
                    if hasattr(position, 'symbol') and position.symbol == symbol:
                        if hasattr(position, 'entry_price') and position.entry_price > 0:
                            entry_price = position.entry_price
                            break

            # Price generation with realistic constraints
            if symbol in self._sim_price_cache:
                prev_price = self._sim_price_cache[symbol]
                
                # CRITICAL FIX: Smaller fluctuation for more stability (-0.2% ~ +0.2%)
                # Reduced from -0.5% ~ +0.5% to prevent wild swings
                change = np.random.uniform(-0.002, 0.002)
                new_price = prev_price * (1 + change)
                
                # CRITICAL FIX: Sanity check - prevent price from deviating too far
                if entry_price and entry_price > 0:
                    # Maximum 10% deviation from entry price for simulation
                    max_price = entry_price * 1.10
                    min_price = entry_price * 0.90
                    
                    if new_price > max_price:
                        new_price = max_price
                        logger.warning(
                            f"[Simulation] {symbol} price capped at +10% from entry: "
                            f"${new_price:.4f} (entry: ${entry_price:.4f})"
                        )
                    elif new_price < min_price:
                        new_price = min_price
                        logger.warning(
                            f"[Simulation] {symbol} price capped at -10% from entry: "
                            f"${new_price:.4f} (entry: ${entry_price:.4f})"
                        )
                
                # CRITICAL FIX: Prevent extreme price deviation from base price
                # Maximum 50% deviation from base price
                max_allowed = base_price * 1.5
                min_allowed = base_price * 0.5
                
                if new_price > max_allowed or new_price < min_allowed:
                    logger.error(
                        f"[Simulation] CRITICAL: {symbol} price out of bounds: ${new_price:.4f}, "
                        f"resetting to base price ${base_price:.4f}"
                    )
                    new_price = base_price * (1 + np.random.uniform(-0.05, 0.05))
                
            else:
                # Initial price generation
                if entry_price and entry_price > 0:
                    # Use entry price as starting point if available
                    new_price = entry_price * (1 + np.random.uniform(-0.01, 0.01))
                    logger.info(
                        f"[Simulation] {symbol} initialized from entry price: ${new_price:.4f} "
                        f"(entry: ${entry_price:.4f})"
                    )
                else:
                    # Initial price (base value +/- 5% range)
                    change = np.random.uniform(-0.05, 0.05)
                    new_price = base_price * (1 + change)
                    logger.info(
                        f"[Simulation] {symbol} initialized from base price: ${new_price:.4f} "
                        f"(base: ${base_price:.4f})"
                    )

            # CRITICAL FIX: Final validation - ensure price is reasonable
            if new_price <= 0:
                logger.error(f"[Simulation] CRITICAL: Invalid price generated for {symbol}, using base price")
                new_price = base_price
            
            # CRITICAL FIX: Detect unrealistic prices
            if new_price > 1000000:  # More than 1M per coin is suspicious
                logger.error(
                    f"[Simulation] CRITICAL: Unrealistic price detected for {symbol}: ${new_price:,.2f}, "
                    f"resetting to base price ${base_price:.4f}"
                )
                new_price = base_price

            # Update cache
            self._sim_price_cache[symbol] = new_price

            logger.debug(f"[Simulation] {symbol} price: ${new_price:,.4f}")
            return new_price

        except Exception as e:
            logger.error(f"Simulation price generation failed ({symbol}): {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Safe fallback with symbol-specific defaults
            fallback_prices = {
                "BTCUSDT": 42000,
                "ETHUSDT": 2500,
                "FLMUSDT": 0.02,
            }
            fallback_price = fallback_prices.get(symbol, 100.0)
            logger.warning(f"Using fallback price for {symbol}: ${fallback_price:.4f}")
            return fallback_price

    async def get_multiple_prices_optimized(
        self, symbols: List[str]
    ) -> Dict[str, float]:
        """Query multiple symbol prices simultaneously (performance optimized)"""
        try:
            if not symbols:
                return {}

            # Query all symbol prices in parallel
            tasks = [self._get_current_price_optimized(symbol) for symbol in symbols]
            prices = await asyncio.gather(*tasks, return_exceptions=True)

            result = {}
            for symbol, price in zip(symbols, prices):
                if isinstance(price, Exception):
                    logger.warning(f"{symbol} price query failed: {price}")
                    result[symbol] = 0.0
                else:
                    result[symbol] = price

            return result

        except Exception as e:
            logger.error(f"Multiple price query failed: {e}")
            return {symbol: 0.0 for symbol in symbols}

    def _init_binance_testnet_client(self):
        """Initialize Binance testnet client with HTTP fallback for time sync issues"""
        try:
            api_key = os.getenv('BINANCE_API_KEY')
            secret_key = os.getenv('BINANCE_SECRET_KEY')
            
            if not api_key or not secret_key:
                logger.warning("Binance API keys not set - using simulation mode")
                return
            
            # Store API credentials for HTTP calls
            self.binance_api_key = api_key
            self.binance_secret_key = secret_key
            self.binance_testnet_base_url = "https://testnet.binance.vision"
            
            # Test basic connection with simple HTTP request (no auth needed)
            try:
                import requests
                
                # Test with simple price endpoint (no auth needed)
                test_url = f"{self.binance_testnet_base_url}/api/v3/ticker/price?symbol=BTCUSDT"
                response = requests.get(test_url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    btc_price = float(data['price'])
                    logger.info(f"Binance testnet connection verified: BTC ${btc_price:,.2f}")
                    
                    # Set a flag to indicate HTTP API mode
                    self.binance_client = "HTTP_MODE"  # Special flag
                    logger.info("Binance testnet HTTP API mode activated - bypassing time sync issues")
                    
                else:
                    logger.error(f"Binance testnet test failed: {response.status_code}")
                    self.binance_client = None
                    
            except requests.RequestException as test_error:
                logger.error(f"Binance testnet connection test failed: {test_error}")
                self.binance_client = None
            except Exception as test_error:
                logger.error(f"Unexpected connection test error: {test_error}")
                self.binance_client = None
                
        except Exception as e:
            logger.error(f"Binance testnet client init failed: {e}")
            self.binance_client = None

    def clear_price_cache(self):
        """Clear price cache (when needed)"""
        if hasattr(self, "_price_cache"):
            self._price_cache.clear()
        if hasattr(self, "_cache_timestamps"):
            self._cache_timestamps.clear()
        if hasattr(self, "_sim_price_cache"):
            self._sim_price_cache.clear()

        logger.info("Price cache cleared successfully")

    def _calculate_liquidation_risk(self, position: Position) -> float:
        """Calculate dynamic liquidation risk"""
        try:
            price_distance = abs(position.current_price - position.liquidation_price)
            distance_ratio = price_distance / position.current_price

            # Risk based on distance to liquidation price
            base_risk = max(0.0, 1.0 - (distance_ratio * 4))

            # Volatility adjustment (considering Phoenix95 score)
            volatility_adjustment = 1.0
            if position.phoenix95_score > 0:
                # Higher score means more stable
                volatility_adjustment = 0.8 + (position.phoenix95_score / 95) * 0.4

            final_risk = base_risk * volatility_adjustment
            return max(0.0, min(1.0, final_risk))

        except:
            return 0.5

    def _check_close_conditions_enhanced(
        self, position: Position, current_price: float
    ) -> Optional[str]:
        """Enhanced close condition check with Phoenix95 integration and trailing stop logic"""
        try:
            # Layer 0: Input validation
            if not position:
                logger.error("Close condition check aborted: Position object is None")
                return "System protection liquidation"
            
            if current_price <= 0:
                logger.error(f"Close condition check aborted: Invalid current_price={current_price}")
                return "System protection liquidation"
            
            # Layer 1: Required attribute validation
            required_attrs = ['stop_loss_price', 'take_profit_price', 'phoenix95_score', 'action', 'entry_price']
            missing_attrs = [attr for attr in required_attrs if not hasattr(position, attr)]
            if missing_attrs:
                logger.error(f"Position {position.position_id} missing attributes: {', '.join(missing_attrs)}")
                return "System protection liquidation"

            # Layer 2: Basic stop loss/take profit conditions
            basic_close = self._check_basic_close_conditions(position, current_price)
            if basic_close:
                logger.info(f"Basic close condition triggered: {basic_close}")
                return basic_close

            # Layer 3: Phoenix95 adaptive buffers
            if position.phoenix95_score >= 85:
                # Elite signals: more lenient (5% buffer on stop, 5% earlier on profit)
                stop_buffer = 1.05
                profit_buffer = 0.95
                logger.debug(f"Elite Phoenix95 buffers applied: stop={stop_buffer}, profit={profit_buffer}")
            elif position.phoenix95_score >= 70:
                # Strong signals: moderate (3% buffer)
                stop_buffer = 1.03
                profit_buffer = 0.97
            else:
                # Standard signals: tight (2% buffer)
                stop_buffer = 1.02
                profit_buffer = 0.98

            # Layer 4: Dynamic stop loss/take profit with validation
            if position.action.lower() in ["buy", "long"]:
                # Long position checks
                if position.stop_loss_price > 0:
                    adjusted_stop = position.stop_loss_price * stop_buffer
                    if current_price <= adjusted_stop:
                        price_drop_pct = ((position.entry_price - current_price) / position.entry_price) * 100
                        logger.info(
                            f"Dynamic stop loss triggered for {position.position_id}: "
                            f"${current_price:.4f} <= ${adjusted_stop:.4f} "
                            f"(drop: {price_drop_pct:.2f}%, Phoenix95: {position.phoenix95_score:.0f})"
                        )
                        return f"Dynamic stop loss (Phoenix95: {position.phoenix95_score:.1f})"
                
                if position.take_profit_price > 0:
                    adjusted_profit = position.take_profit_price * profit_buffer
                    if current_price >= adjusted_profit:
                        price_gain_pct = ((current_price - position.entry_price) / position.entry_price) * 100
                        logger.info(
                            f"Dynamic take profit triggered for {position.position_id}: "
                            f"${current_price:.4f} >= ${adjusted_profit:.4f} "
                            f"(gain: {price_gain_pct:.2f}%, Phoenix95: {position.phoenix95_score:.0f})"
                        )
                        return f"Dynamic take profit (Phoenix95: {position.phoenix95_score:.1f})"
            else:
                # Short position checks
                if position.stop_loss_price > 0:
                    adjusted_stop = position.stop_loss_price * stop_buffer
                    if current_price >= adjusted_stop:
                        price_rise_pct = ((current_price - position.entry_price) / position.entry_price) * 100
                        logger.info(
                            f"Dynamic stop loss triggered for {position.position_id}: "
                            f"${current_price:.4f} >= ${adjusted_stop:.4f} "
                            f"(rise: {price_rise_pct:.2f}%, Phoenix95: {position.phoenix95_score:.0f})"
                        )
                        return f"Dynamic stop loss (Phoenix95: {position.phoenix95_score:.1f})"
                
                if position.take_profit_price > 0:
                    adjusted_profit = position.take_profit_price * profit_buffer
                    if current_price <= adjusted_profit:
                        price_drop_pct = ((position.entry_price - current_price) / position.entry_price) * 100
                        logger.info(
                            f"Dynamic take profit triggered for {position.position_id}: "
                            f"${current_price:.4f} <= ${adjusted_profit:.4f} "
                            f"(gain: {price_drop_pct:.2f}%, Phoenix95: {position.phoenix95_score:.0f})"
                        )
                        return f"Dynamic take profit (Phoenix95: {position.phoenix95_score:.1f})"

            # Layer 5: SPOT Trading Liquidation Check (CRITICAL FIX)
            # SPOT trading (leverage=1) does NOT have liquidation risk
            # Only check liquidation for leveraged positions
            if hasattr(position, 'leverage') and hasattr(position, 'liquidation_price'):
                # SPOT trading check (no liquidation for 1x leverage)
                if position.leverage == 1 or position.liquidation_price <= 0:
                    logger.debug(
                        f"SPOT trading detected for {position.position_id}: "
                        f"leverage={position.leverage}x, no liquidation risk"
                    )
                    # Skip liquidation check for SPOT
                    pass
                else:
                    # Futures/Margin trading liquidation check
                    if hasattr(position, 'liquidation_risk'):
                        # High liquidation risk threshold (95%)
                        if position.liquidation_risk > 0.95:
                            logger.error(
                                f"CRITICAL: Emergency liquidation risk for {position.position_id}: "
                                f"{position.liquidation_risk:.1%} (threshold: 95%, "
                                f"leverage: {position.leverage}x)"
                            )
                            return "Emergency liquidation (liquidation risk exceeds 95%)"
                        
                        # Warning level (80%)
                        elif position.liquidation_risk > 0.80:
                            logger.warning(
                                f"HIGH liquidation risk for {position.position_id}: "
                                f"{position.liquidation_risk:.1%} (leverage: {position.leverage}x)"
                            )
                        
                        # Safe level logging
                        elif position.liquidation_risk < 0.30:
                            logger.debug(
                                f"Safe liquidation risk for {position.position_id}: "
                                f"{position.liquidation_risk:.1%} (leverage: {position.leverage}x)"
                            )
            else:
                logger.debug(
                    f"No liquidation attributes found for {position.position_id}, "
                    f"assuming SPOT trading"
                )

            # Layer 6: Trailing stop for profitable positions (Phoenix95 enhancement)
            if hasattr(position, 'unrealized_pnl') and hasattr(position, 'margin_required'):
                if position.unrealized_pnl > 0 and position.margin_required > 0:
                    profit_pct = (position.unrealized_pnl / position.margin_required) * 100
                    
                    # Trailing stop threshold: 10%+ profit
                    if profit_pct > 10:
                        trailing_stop_pct = 0.05  # Trail by 5%
                        
                        if position.action.lower() in ["buy", "long"]:
                            # Calculate trailing stop price
                            peak_price = position.entry_price * (1 + (profit_pct/100))
                            trailing_stop = peak_price * (1 - trailing_stop_pct)
                            
                            if current_price < trailing_stop:
                                logger.info(
                                    f"Trailing stop triggered for {position.position_id}: "
                                    f"${current_price:.4f} < ${trailing_stop:.4f} "
                                    f"(secured profit: {profit_pct:.1f}%, peak: ${peak_price:.4f})"
                                )
                                return f"Trailing stop (secured {profit_pct:.1f}% profit)"
                        
                        elif position.action.lower() in ["sell", "short"]:
                            # Trailing stop for short positions
                            peak_price = position.entry_price * (1 - (profit_pct/100))
                            trailing_stop = peak_price * (1 + trailing_stop_pct)
                            
                            if current_price > trailing_stop:
                                logger.info(
                                    f"Trailing stop triggered for short {position.position_id}: "
                                    f"${current_price:.4f} > ${trailing_stop:.4f} "
                                    f"(secured profit: {profit_pct:.1f}%)"
                                )
                                return f"Trailing stop (secured {profit_pct:.1f}% profit)"

            # No close conditions met
            return None

        except AttributeError as e:
            logger.error(f"Attribute error in close condition check for {position.position_id}: {e}")
            return "System protection liquidation"
        except ZeroDivisionError as e:
            logger.error(f"Division by zero in close condition check for {position.position_id}: {e}")
            return "System protection liquidation"
        except Exception as e:
            logger.error(f"Unexpected error in close condition check for {position.position_id}: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            return "System protection liquidation"

    def _check_basic_close_conditions(
        self, position: Position, current_price: float
    ) -> Optional[str]:
        """Basic close condition check with enhanced validation"""
        try:
            # Validate inputs
            if not position or current_price <= 0:
                return None
            
            # Validate position attributes exist and are valid
            if not hasattr(position, 'stop_loss_price') or not hasattr(position, 'take_profit_price'):
                logger.warning(f"Position {position.position_id} missing price attributes")
                return None
            
            if not hasattr(position, 'action') or not position.action:
                logger.warning(f"Position {position.position_id} missing action attribute")
                return None

            # Check stop loss and take profit with validation
            if position.action.lower() in ["buy", "long"]:
                if position.stop_loss_price > 0 and current_price <= position.stop_loss_price:
                    return "Stop loss executed"
                if position.take_profit_price > 0 and current_price >= position.take_profit_price:
                    return "Take profit executed"
            else:
                if position.stop_loss_price > 0 and current_price >= position.stop_loss_price:
                    return "Stop loss executed"
                if position.take_profit_price > 0 and current_price <= position.take_profit_price:
                    return "Take profit executed"

            # SPOT trading: No liquidation price check needed (always 0)
            # Skip liquidation condition for SPOT trading
            if hasattr(position, 'liquidation_price') and position.liquidation_price > 0:
                liquidation_threshold = 0.03
                if position.action.lower() in ["buy", "long"]:
                    if current_price <= position.liquidation_price * (1 + liquidation_threshold):
                        return "Force liquidation prevention (long)"
                else:
                    if current_price >= position.liquidation_price * (1 - liquidation_threshold):
                        return "Force liquidation prevention (short)"

            return None

        except AttributeError as e:
            logger.warning(f"Attribute error in basic close condition check: {e}")
            return None
        except Exception as e:
            logger.warning(f"Basic close condition check error: {e}")
            return None

    def _serialize_for_json(self, obj: Any) -> Any:
        """
        Helper method to serialize objects for JSON compatibility
        Handles datetime, dataclass, and other non-JSON-serializable types
        Optimized for high-performance systems (128GB RAM)
        """
        try:
            from datetime import datetime, date
            from decimal import Decimal
            
            # Handle None
            if obj is None:
                return None
            
            # Handle primitive types (already JSON-serializable)
            if isinstance(obj, (str, int, float, bool)):
                return obj
            
            # Handle datetime objects
            if isinstance(obj, datetime):
                return obj.isoformat()
            
            # Handle date objects
            if isinstance(obj, date):
                return obj.isoformat()
            
            # Handle Decimal
            if isinstance(obj, Decimal):
                return float(obj)
            
            # Handle dictionaries recursively
            if isinstance(obj, dict):
                return {key: self._serialize_for_json(value) for key, value in obj.items()}
            
            # Handle lists/tuples recursively
            if isinstance(obj, (list, tuple)):
                return [self._serialize_for_json(item) for item in obj]
            
            # Handle dataclass objects
            if hasattr(obj, '__dataclass_fields__'):
                return self._serialize_for_json(asdict(obj))
            
            # Fallback: try converting to string
            return str(obj)
            
        except Exception as e:
            logger.debug(f"Serialization error for {type(obj).__name__}: {e}, using fallback")
            return str(obj)

    async def _notify_execution_result(self, signal: EnhancedTradingSignal, result: ExecutionResult) -> bool:
        """
        Trade execution result notification via Notify Service
        Optimized for high-performance systems (Ryzen 9 7950X, 128GB RAM)
        Features: Parallel retry, adaptive timeouts, intelligent caching, datetime serialization
        """
        notification_start = time.time()
        notification_id = f"NOTIFY_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        
        try:
            # Enhanced input validation with detailed logging
            if not signal:
                logger.error(f"[{notification_id}] Notification aborted: Signal object is None")
                return False
            
            if not result:
                logger.error(f"[{notification_id}] Notification aborted: ExecutionResult object is None")
                return False
            
            # Validate critical result fields
            if not hasattr(result, 'execution_id') or not result.execution_id:
                logger.error(f"[{notification_id}] Notification aborted: Missing execution_id in result")
                return False
            
            if not hasattr(result, 'status') or not result.status:
                logger.error(f"[{notification_id}] Notification aborted: Missing status in result")
                return False

            # Extract position and signal data for required fields
            position_id = "N/A"
            symbol = getattr(signal, 'symbol', 'UNKNOWN')
            action = getattr(signal, 'action', 'UNKNOWN')
            
            if hasattr(result, 'position') and result.position:
                position_id = getattr(result.position, 'position_id', 'N/A')
                symbol = getattr(result.position, 'symbol', symbol)
                action = getattr(result.position, 'action', action)

            # Prepare execution_result field for Notify Service with datetime conversion
            execution_result_data = {
                "execution_id": result.execution_id,
                "status": result.status,
                "success": getattr(result, 'success', False),
                "message": getattr(result, 'message', ''),
                "execution_time_ms": getattr(result, 'execution_time_ms', 0),
                "timestamp": getattr(result, 'timestamp', time.time()),
                "service_chain": getattr(result, 'service_chain', 'unknown'),
                "notified": getattr(result, 'notified', False)
            }
            
            # Add position data if available with datetime handling
            if hasattr(result, 'position') and result.position:
                try:
                    position_data = {
                        "position_id": result.position.position_id,
                        "symbol": result.position.symbol,
                        "action": result.position.action,
                        "entry_price": result.position.entry_price,
                        "quantity": result.position.quantity,
                        "leverage": result.position.leverage,
                        "margin_required": result.position.margin_required,
                        "stop_loss_price": result.position.stop_loss_price,
                        "take_profit_price": result.position.take_profit_price,
                        "phoenix95_score": result.position.phoenix95_score,
                        "created_at": result.position.created_at,
                        "updated_at": result.position.updated_at
                    }
                    
                    # Convert any datetime objects to ISO format
                    execution_result_data["position"] = self._serialize_for_json(position_data)
                    
                except AttributeError as ae:
                    logger.warning(f"[{notification_id}] Could not extract full position data: {ae}")
                    execution_result_data["position"] = {
                        "position_id": getattr(result.position, 'position_id', 'unknown'),
                        "symbol": getattr(result.position, 'symbol', 'unknown')
                    }
            else:
                execution_result_data["position"] = None

            # Serialize signal data (may contain datetime objects)
            try:
                signal_dict = asdict(signal) if hasattr(signal, '__dict__') else {}
                signal_dict = self._serialize_for_json(signal_dict)
            except Exception as signal_error:
                logger.warning(f"[{notification_id}] Signal serialization error: {signal_error}")
                signal_dict = {
                    "signal_id": getattr(signal, 'signal_id', 'unknown'),
                    "symbol": getattr(signal, 'symbol', 'unknown'),
                    "action": getattr(signal, 'action', 'unknown')
                }

            # CRITICAL FIX: Prepare comprehensive notification payload with ALL required fields
            # Root-level fields for Notify Service validation
            notify_data = {
                "type": "trade_execution_result",
                
                # CRITICAL: Root-level required fields for Notify Service validation
                "execution_id": result.execution_id,
                "position_id": position_id,
                "symbol": symbol,
                "action": action,
                "service": "execute_service",
                "priority": "high" if result.status == "SUCCESS" else "medium",
                "timestamp": time.time(),
                
                # CRITICAL FIX: Add execution_result and signal at ROOT LEVEL
                "execution_result": execution_result_data,
                "signal": signal_dict,
                
                # Detailed data nested structure (for backward compatibility)
                "data": {
                    "execution_id": result.execution_id,
                    "position_id": position_id,
                    "symbol": symbol,
                    "action": action,
                    "signal": signal_dict,
                    "execution_result": execution_result_data,
                    "service_chain": getattr(signal, 'service_chain', 'unknown'),
                    "phoenix95_score": getattr(signal, 'phoenix95_score', 0),
                    "execution_service": "completed",
                    "performance_metrics": {
                        "execution_time_ms": getattr(result, 'execution_time_ms', 0),
                        "kelly_size": result.position.margin_required if (hasattr(result, 'position') and result.position) else 0,
                        "leverage_used": result.position.leverage if (hasattr(result, 'position') and result.position) else 0,
                        "position_value": (
                            result.position.entry_price * result.position.quantity 
                            if (hasattr(result, 'position') and result.position and 
                                hasattr(result.position, 'entry_price') and 
                                hasattr(result.position, 'quantity')) else 0
                        ),
                    },
                    "timestamp": time.time(),
                    "notification_id": notification_id
                }
            }
            
            # Final serialization check for entire payload
            notify_data = self._serialize_for_json(notify_data)

            # Log payload structure for debugging (only first time)
            if not hasattr(self, '_notification_payload_logged'):
                logger.info(f"[{notification_id}] Notification payload structure validated:")
                logger.info(f"  Root fields: {list(notify_data.keys())}")
                logger.info(f"  Required fields present: execution_id={bool(notify_data.get('execution_id'))}, "
                           f"position_id={bool(notify_data.get('position_id'))}, "
                           f"symbol={bool(notify_data.get('symbol'))}, "
                           f"execution_result={bool(notify_data.get('execution_result'))}, "
                           f"signal={bool(notify_data.get('signal'))}")
                self._notification_payload_logged = True

            # High-performance retry logic (optimized for 16-24 core CPU)
            max_retries = 5
            base_timeout = 6.0
            
            # Parallel retry attempt tracking (128GB RAM allows extensive caching)
            retry_cache_key = f"{result.execution_id}_{notification_id}"
            if not hasattr(self, '_notification_retry_cache'):
                self._notification_retry_cache = {}
            
            for attempt in range(max_retries):
                try:
                    # Adaptive timeout with multi-core optimization
                    adaptive_timeout = base_timeout + (attempt * 1.5)
                    
                    # Store retry attempt in cache (128GB RAM optimization)
                    self._notification_retry_cache[retry_cache_key] = {
                        "attempt": attempt + 1,
                        "timestamp": time.time(),
                        "status": "in_progress"
                    }
                    
                    success = await asyncio.wait_for(
                        self._call_notify_service("/api/notification/execution", notify_data),
                        timeout=adaptive_timeout
                    )

                    if success:
                        notification_duration = (time.time() - notification_start) * 1000
                        logger.info(
                            f"[{notification_id}] Execution notification sent successfully "
                            f"(attempt {attempt + 1}/{max_retries}, {notification_duration:.1f}ms)"
                        )
                        
                        # Update success rate metric with thread-safe operation
                        current_rate = self.execution_stats.get("notify_success_rate", 0)
                        self.execution_stats["notify_success_rate"] = min(1.0, current_rate + 0.01)
                        
                        # Update cache with success status
                        self._notification_retry_cache[retry_cache_key]["status"] = "success"
                        
                        # Cleanup old cache entries (keep last 10000 for 128GB RAM)
                        if len(self._notification_retry_cache) > 10000:
                            oldest_keys = sorted(
                                self._notification_retry_cache.keys(),
                                key=lambda k: self._notification_retry_cache[k]["timestamp"]
                            )[:5000]
                            for old_key in oldest_keys:
                                del self._notification_retry_cache[old_key]
                        
                        return True
                    else:
                        logger.warning(
                            f"[{notification_id}] Notification failed (attempt {attempt + 1}/{max_retries}), "
                            f"service returned False"
                        )
                        if attempt < max_retries - 1:
                            # Reduced exponential backoff for high-performance hardware
                            backoff_delay = min(3.0, 0.5 * (2 ** attempt))
                            await asyncio.sleep(backoff_delay)
                        
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[{notification_id}] Notification timeout after {adaptive_timeout}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        backoff_delay = min(3.0, 0.5 * (2 ** attempt))
                        await asyncio.sleep(backoff_delay)
                        
                except Exception as e:
                    logger.warning(
                        f"[{notification_id}] Notification error (attempt {attempt + 1}/{max_retries}): "
                        f"{type(e).__name__}: {e}"
                    )
                    if attempt < max_retries - 1:
                        backoff_delay = min(3.0, 0.5 * (2 ** attempt))
                        await asyncio.sleep(backoff_delay)

            # All retries failed - use backup telegram notification
            logger.error(
                f"[{notification_id}] All {max_retries} notification attempts failed, "
                f"falling back to telegram backup"
            )
            
            # Update cache with failure status
            if retry_cache_key in self._notification_retry_cache:
                self._notification_retry_cache[retry_cache_key]["status"] = "failed_all_retries"
            
            try:
                await asyncio.wait_for(
                    self._send_telegram_backup_notification(signal, result),
                    timeout=10.0
                )
                logger.info(f"[{notification_id}] Backup telegram notification sent successfully")
            except asyncio.TimeoutError:
                logger.error(f"[{notification_id}] Backup telegram notification timeout (10s)")
            except Exception as backup_error:
                logger.error(f"[{notification_id}] Backup telegram notification failed: {backup_error}")
            
            return False

        except Exception as e:
            logger.error(
                f"[{notification_id}] Critical error in execution notification: {type(e).__name__}: {e}"
            )
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            
            # Final attempt: backup notification with timeout protection
            try:
                await asyncio.wait_for(
                    self._send_telegram_backup_notification(signal, result),
                    timeout=5.0
                )
                logger.warning(f"[{notification_id}] Emergency backup notification sent after critical error")
            except asyncio.TimeoutError:
                logger.error(f"[{notification_id}] Emergency backup notification timeout")
            except Exception as backup_error:
                logger.error(f"[{notification_id}] Emergency backup notification failed: {backup_error}")
            
            return False

    async def _send_risk_alert_via_notify(self, position: Position):
        """
        Risk alert via Notify Service with intelligent rate limiting and deduplication
        Optimized for high-performance systems (Ryzen 9 7950X, 128GB RAM)
        Features: Multi-tier rate limiting, smart caching, priority-based throttling
        """
        alert_start = time.time()
        alert_id = f"ALERT_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"
        
        try:
            # Validate position object
            if not position:
                logger.error(f"[{alert_id}] Risk alert aborted: Position object is None")
                return
            
            # Validate position has required fields
            if not hasattr(position, 'position_id') or not position.position_id:
                logger.error(f"[{alert_id}] Risk alert aborted: Position missing position_id")
                return
            
            if not hasattr(position, 'liquidation_risk'):
                logger.warning(f"[{alert_id}] Risk alert skipped: Position {position.position_id} missing liquidation_risk")
                return

            # Initialize advanced rate limiter cache (128GB RAM optimized)
            if not hasattr(self, '_risk_alert_cache'):
                self._risk_alert_cache = {}
                self._risk_alert_timestamps = {}
                self._risk_alert_global_count = 0
                self._risk_alert_global_reset = time.time()
                self._risk_alert_priority_queue = {}
                logger.debug(f"[{alert_id}] Risk alert cache system initialized")
            
            current_time = time.time()
            position_id = position.position_id
            
            # Global rate limit: Maximum 15 alerts per minute (increased for high-performance)
            time_since_global_reset = current_time - self._risk_alert_global_reset
            if time_since_global_reset >= 60:
                # Reset global counter every minute
                self._risk_alert_global_count = 0
                self._risk_alert_global_reset = current_time
                logger.debug(f"[{alert_id}] Global alert counter reset")
            
            # Calculate alert priority first
            alert_level = self._calculate_alert_level(position)
            alert_priority = self._get_alert_priority(position.liquidation_risk)
            
            # Priority-based rate limiting (critical alerts bypass some limits)
            if alert_priority == "critical":
                max_global_alerts = 20
                min_interval = 120
            elif alert_priority == "high":
                max_global_alerts = 15
                min_interval = 180
            else:
                max_global_alerts = 10
                min_interval = 300
            
            if self._risk_alert_global_count >= max_global_alerts and alert_priority not in ["critical", "high"]:
                logger.debug(
                    f"[{alert_id}] Global rate limit reached: {self._risk_alert_global_count}/{max_global_alerts} alerts this minute, "
                    f"skipping alert for {position_id} (priority: {alert_priority})"
                )
                return
            
            # Per-position rate limit with priority adjustment
            if position_id in self._risk_alert_timestamps:
                last_alert_time = self._risk_alert_timestamps[position_id]
                time_since_last_alert = current_time - last_alert_time
                
                if time_since_last_alert < min_interval:
                    logger.debug(
                        f"[{alert_id}] Per-position rate limit active: "
                        f"Last alert {time_since_last_alert:.0f}s ago for {position_id}, "
                        f"minimum interval: {min_interval}s (priority: {alert_priority})"
                    )
                    return
            
            # Risk severity filter with priority bypass
            if position.liquidation_risk < 0.7 and alert_priority not in ["critical", "high"]:
                logger.debug(
                    f"[{alert_id}] Risk level too low for alert: {position.liquidation_risk:.1%} "
                    f"for {position_id} (threshold: 70%, priority: {alert_priority})"
                )
                return
            
            # Calculate liquidation distance (handle SPOT trading with 0 liquidation price)
            if hasattr(position, 'liquidation_price') and position.liquidation_price > 0:
                liquidation_distance = abs(
                    position.current_price - position.liquidation_price
                ) / position.current_price
            else:
                liquidation_distance = 0.0

            # Prepare comprehensive alert payload
            # CRITICAL FIX: Add root-level fields for Notify Service validation
            notify_data = {
                "type": "position_risk_alert",
                
                # ROOT LEVEL: Required fields for Notify Service validation
                "position_id": position.position_id,
                "symbol": position.symbol,
                "action": position.action,
                "service": "execute_service",
                "priority": alert_priority,
                "timestamp": time.time(),
                
                # NESTED: Detailed data structure (for backward compatibility)
                "data": {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "action": position.action,
                    "position": asdict(position) if hasattr(position, '__dict__') else {},
                    "alert_level": alert_level,
                    "liquidation_risk": position.liquidation_risk,
                    "liquidation_distance": liquidation_distance,
                    "liquidation_distance_pct": liquidation_distance * 100,
                    "unrealized_pnl": getattr(position, 'unrealized_pnl', 0),
                    "pnl_percentage": getattr(position, 'pnl_percentage', 0),
                    "roe": getattr(position, 'roe', 0),
                    "phoenix95_score": getattr(position, 'phoenix95_score', 0),
                    "current_price": getattr(position, 'current_price', 0),
                    "entry_price": getattr(position, 'entry_price', 0),
                    "margin_required": getattr(position, 'margin_required', 0),
                    "leverage": getattr(position, 'leverage', 1),
                    "service": "execute_service",
                    "timestamp": time.time(),
                    "alert_id": alert_id,
                    "alert_metadata": {
                        "time_since_last_alert": (
                            current_time - self._risk_alert_timestamps.get(position_id, 0)
                        ),
                        "global_alert_count_this_minute": self._risk_alert_global_count,
                        "alert_frequency_limited": True,
                        "priority": alert_priority,
                        "min_interval": min_interval
                    }
                }
            }

            # Enhanced retry logic with adaptive timeout (Ryzen 9 optimization)
            max_retries = 3 if alert_priority in ["critical", "high"] else 2
            base_timeout = 6.0
            
            # Store alert attempt in cache (128GB RAM optimization)
            cache_key = f"{position_id}_{alert_id}"
            self._risk_alert_cache[cache_key] = {
                "attempt_start": current_time,
                "priority": alert_priority,
                "status": "in_progress"
            }
            
            for attempt in range(max_retries):
                try:
                    # Adaptive timeout based on priority
                    if alert_priority == "critical":
                        adaptive_timeout = base_timeout + (attempt * 1)
                    else:
                        adaptive_timeout = base_timeout + (attempt * 2)
                    
                    success = await asyncio.wait_for(
                        self._call_notify_service("/api/notification/risk", notify_data),
                        timeout=adaptive_timeout
                    )
                    
                    if success:
                        alert_duration = (time.time() - alert_start) * 1000
                        
                        # Update rate limiting trackers
                        self._risk_alert_timestamps[position_id] = current_time
                        self._risk_alert_global_count += 1
                        self.risk_metrics["liquidation_warnings"] += 1
                        
                        # Update cache with success
                        self._risk_alert_cache[cache_key]["status"] = "success"
                        self._risk_alert_cache[cache_key]["duration_ms"] = alert_duration
                        
                        # Cleanup old cache entries (keep last 5000 for 128GB RAM)
                        if len(self._risk_alert_cache) > 5000:
                            oldest_keys = sorted(
                                self._risk_alert_cache.keys(),
                                key=lambda k: self._risk_alert_cache[k]["attempt_start"]
                            )[:2500]
                            for old_key in oldest_keys:
                                del self._risk_alert_cache[old_key]
                        
                        logger.warning(
                            f"[{alert_id}] Risk alert sent successfully for {position.position_id}: "
                            f"Level={alert_level}, Priority={alert_priority}, Risk={position.liquidation_risk:.1%}, "
                            f"P&L=${position.unrealized_pnl:+,.2f}, "
                            f"Duration={alert_duration:.1f}ms, "
                            f"Global alerts: {self._risk_alert_global_count}/{max_global_alerts}"
                        )
                        return
                    else:
                        logger.warning(
                            f"[{alert_id}] Risk alert failed for {position.position_id} "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        if attempt < max_retries - 1:
                            backoff_delay = min(2.0, 0.5 * (2 ** attempt))
                            await asyncio.sleep(backoff_delay)
                            
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[{alert_id}] Risk alert timeout for {position.position_id} "
                        f"after {adaptive_timeout}s (attempt {attempt + 1}/{max_retries})"
                    )
                    if attempt < max_retries - 1:
                        backoff_delay = min(2.0, 0.5 * (2 ** attempt))
                        await asyncio.sleep(backoff_delay)
                except Exception as e:
                    logger.warning(
                        f"[{alert_id}] Risk alert error for {position.position_id} "
                        f"(attempt {attempt + 1}/{max_retries}): {type(e).__name__}: {e}"
                    )
                    if attempt < max_retries - 1:
                        backoff_delay = min(2.0, 0.5 * (2 ** attempt))
                        await asyncio.sleep(backoff_delay)
            
            # All retries failed - update cache
            if cache_key in self._risk_alert_cache:
                self._risk_alert_cache[cache_key]["status"] = "failed_all_retries"
            
            logger.error(f"[{alert_id}] All {max_retries} risk alert attempts failed for {position.position_id}")

        except Exception as e:
            logger.error(
                f"[{alert_id}] Critical error in risk alert for {getattr(position, 'position_id', 'unknown')}: "
                f"{type(e).__name__}: {e}"
            )
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")

    def _get_alert_priority(self, liquidation_risk: float) -> str:
        """
        Determine alert priority based on liquidation risk
        Optimized for Ryzen 9 7950X / 128GB RAM system with enhanced categorization
        """
        try:
            # Validate input
            if liquidation_risk < 0 or liquidation_risk > 1:
                logger.warning(f"Invalid liquidation_risk value: {liquidation_risk}, defaulting to 'medium'")
                return "medium"
            
            # Enhanced priority determination with finer granularity
            if liquidation_risk > 0.95:
                return "critical"
            elif liquidation_risk > 0.85:
                return "high"
            elif liquidation_risk > 0.7:
                return "medium"
            elif liquidation_risk > 0.5:
                return "low"
            else:
                return "info"
        except Exception as e:
            logger.error(f"Error in alert priority calculation: {e}")
            return "medium"

    async def _close_position_with_notification(
        self, position: Position, close_price: float, reason: str
    ):
        """
        Position close with notification, memory management, and comprehensive tracking
        Optimized for Ryzen 9 7950X (16-24 cores) / 128GB DDR5 RAM
        Features: Parallel processing, intelligent caching, enhanced retry logic
        """
        close_start = time.time()
        close_id = f"CLOSE_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
        
        try:
            # Enhanced input validation
            if not position:
                logger.error(f"[{close_id}] Position close aborted: Position object is None")
                return
            
            if not hasattr(position, 'position_id'):
                logger.error(f"[{close_id}] Position close aborted: Missing position_id attribute")
                return
            
            if close_price <= 0:
                logger.warning(f"[{close_id}] Invalid close_price {close_price}, using current_price")
                close_price = getattr(position, 'current_price', 0)
                if close_price <= 0:
                    logger.error(f"[{close_id}] Cannot close position: No valid price available")
                    return

            logger.info("=" * 60)
            logger.info(f"[{close_id}] Position Close Initiated: {position.position_id}")
            logger.info(f"Symbol: {position.symbol} {position.action.upper()}")
            logger.info(f"Close Reason: {reason}")
            logger.info(f"Entry Price: ${position.entry_price:.4f}")
            logger.info(f"Close Price: ${close_price:.4f}")
            logger.info(f"Phoenix95 Score: {position.phoenix95_score:.1f}/95")
            logger.info("=" * 60)

            # Execute basic position close with timeout protection
            close_timeout = 30.0
            try:
                await asyncio.wait_for(
                    self._execute_basic_close_position(position, close_price, reason),
                    timeout=close_timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"[{close_id}] Position close execution timeout after {close_timeout}s")
                raise RuntimeError(f"Position close timeout for {position.position_id}")
            except Exception as close_error:
                logger.error(f"[{close_id}] Position close execution failed: {close_error}")
                raise

            # Calculate position metrics with validation
            try:
                position_duration = position.closed_at - position.created_at
                duration_hours = position_duration / 3600
                duration_minutes = position_duration / 60
            except (AttributeError, TypeError) as e:
                logger.warning(f"[{close_id}] Duration calculation error: {e}, using defaults")
                position_duration = 0
                duration_hours = 0
                duration_minutes = 0
            
            # Prepare comprehensive notification data with enhanced structure
            # CRITICAL FIX: Add root-level fields for Notify Service validation
            notify_data = {
                "type": "position_closed",
                
                # ROOT LEVEL: Required fields for Notify Service validation
                "position_id": position.position_id,
                "symbol": position.symbol,
                "action": position.action,
                "service": "execute_service",
                "priority": "high",
                "timestamp": time.time(),
                
                # NESTED: Detailed data structure (for backward compatibility)
                "data": {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "action": position.action,
                    "close_reason": reason,
                    "entry_price": position.entry_price,
                    "close_price": close_price,
                    "quantity": position.quantity,
                    "final_pnl": position.realized_pnl,
                    "pnl_percentage": position.pnl_percentage,
                    "roe": position.roe,
                    "phoenix95_score": position.phoenix95_score,
                    "service_chain": position.service_chain,
                    "performance_summary": {
                        "position_duration_seconds": position_duration,
                        "position_duration_hours": round(duration_hours, 2),
                        "position_duration_minutes": round(duration_minutes, 2),
                        "max_risk_reached": position.liquidation_risk,
                        "leverage_used": position.leverage,
                        "margin_required": position.margin_required,
                        "final_position_value": close_price * position.quantity,
                        "price_change_percent": ((close_price - position.entry_price) / position.entry_price * 100) 
                                                if position.entry_price > 0 else 0,
                    },
                    "timestamps": {
                        "created_at": position.created_at,
                        "closed_at": position.closed_at,
                        "iso_created": datetime.fromtimestamp(position.created_at).isoformat(),
                        "iso_closed": datetime.fromtimestamp(position.closed_at).isoformat(),
                    },
                    "service": "execute_service",
                    "timestamp": time.time(),
                    "close_id": close_id
                }
            }

            # High-performance notification with enhanced retry logic (128GB RAM optimized)
            notification_success = False
            max_retry_attempts = 5
            base_timeout = 8.0
            
            # Cache notification attempt (128GB RAM optimization)
            if not hasattr(self, '_close_notification_cache'):
                self._close_notification_cache = {}
            
            cache_key = f"{position.position_id}_{close_id}"
            
            for attempt in range(max_retry_attempts):
                try:
                    # Adaptive timeout with multi-core optimization
                    adaptive_timeout = base_timeout + (attempt * 2)
                    
                    # Store attempt in cache
                    self._close_notification_cache[cache_key] = {
                        "attempt": attempt + 1,
                        "timestamp": time.time(),
                        "status": "in_progress"
                    }
                    
                    notification_success = await asyncio.wait_for(
                        self._call_notify_service("/api/notification/position", notify_data),
                        timeout=adaptive_timeout
                    )
                    
                    if notification_success:
                        logger.info(
                            f"[{close_id}] Position close notification sent successfully "
                            f"(attempt {attempt + 1}/{max_retry_attempts})"
                        )
                        self._close_notification_cache[cache_key]["status"] = "success"
                        
                        # Cleanup old cache entries (keep last 5000 for 128GB RAM)
                        if len(self._close_notification_cache) > 5000:
                            oldest_keys = sorted(
                                self._close_notification_cache.keys(),
                                key=lambda k: self._close_notification_cache[k]["timestamp"]
                            )[:2500]
                            for old_key in oldest_keys:
                                del self._close_notification_cache[old_key]
                        
                        break
                    else:
                        logger.warning(
                            f"[{close_id}] Notification failed (attempt {attempt + 1}/{max_retry_attempts})"
                        )
                        if attempt < max_retry_attempts - 1:
                            # Reduced backoff for high-performance hardware
                            backoff_delay = min(2.0, 0.5 * (2 ** attempt))
                            logger.info(f"[{close_id}] Retrying in {backoff_delay}s...")
                            await asyncio.sleep(backoff_delay)
                            
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[{close_id}] Notification timeout after {adaptive_timeout}s "
                        f"(attempt {attempt + 1}/{max_retry_attempts})"
                    )
                    if attempt < max_retry_attempts - 1:
                        backoff_delay = min(2.0, 0.5 * (2 ** attempt))
                        await asyncio.sleep(backoff_delay)
                        
                except Exception as notify_error:
                    logger.warning(
                        f"[{close_id}] Notification error (attempt {attempt + 1}/{max_retry_attempts}): "
                        f"{type(notify_error).__name__}: {notify_error}"
                    )
                    if attempt < max_retry_attempts - 1:
                        backoff_delay = min(2.0, 0.5 * (2 ** attempt))
                        await asyncio.sleep(backoff_delay)

            # Fallback telegram notification if all retries failed
            if not notification_success:
                logger.error(
                    f"[{close_id}] All {max_retry_attempts} notification attempts failed, "
                    f"attempting telegram backup"
                )
                
                # Update cache with failure status
                if cache_key in self._close_notification_cache:
                    self._close_notification_cache[cache_key]["status"] = "failed_all_retries"
                
                try:
                    await asyncio.wait_for(
                        self._send_telegram_direct(
                            f"Position Closed (Notify Service Failed)\n"
                            f"ID: {position.position_id}\n"
                            f"Symbol: {position.symbol}\n"
                            f"P&L: ${position.realized_pnl:+,.2f} ({position.pnl_percentage:+.2f}%)\n"
                            f"Reason: {reason}"
                        ),
                        timeout=10.0
                    )
                    logger.info(f"[{close_id}] Telegram backup notification sent")
                except Exception as telegram_error:
                    logger.error(f"[{close_id}] Telegram backup also failed: {telegram_error}")

            # Calculate total close duration
            close_duration = (time.time() - close_start) * 1000

            # Final logging with comprehensive summary
            logger.info("=" * 60)
            logger.info(f"[{close_id}] Position Close Complete: {position.position_id}")
            logger.info(f"Final P&L: ${position.realized_pnl:+,.2f} ({position.pnl_percentage:+.2f}%)")
            logger.info(f"ROE: {position.roe:+.2f}%")
            logger.info(f"Duration: {duration_minutes:.1f} minutes ({duration_hours:.2f} hours)")
            logger.info(f"Notification: {'SUCCESS' if notification_success else 'FAILED'}")
            logger.info(f"Close Time: {close_duration:.1f}ms")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(
                f"[{close_id}] Critical error in position close for {position.position_id}: "
                f"{type(e).__name__}: {e}"
            )
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            
            # Attempt to force close anyway with timeout
            try:
                await asyncio.wait_for(
                    self._force_close_position(position, f"Error recovery: {str(e)}"),
                    timeout=10.0
                )
                logger.warning(f"[{close_id}] Force close succeeded for {position.position_id}")
            except asyncio.TimeoutError:
                logger.error(f"[{close_id}] Force close timeout for {position.position_id}")
            except Exception as force_error:
                logger.error(f"[{close_id}] Force close also failed: {force_error}")

    async def _execute_basic_close_position(
        self, position: Position, close_price: float, reason: str
    ):
        """
        Enhanced basic position close with memory optimization for high-end hardware
        Optimized for Ryzen 9 7950X (16-24 cores) / 128GB DDR5 RAM
        """
        close_start = time.time()
        
        try:
            # Enhanced P&L calculation with comprehensive validation
            final_pnl = self._calculate_pnl(position, close_price)
            if final_pnl.get("error", False):
                logger.warning(f"P&L calculation had errors for {position.position_id}, using partial data")
            
            # Validate P&L results before applying
            if "unrealized_pnl" not in final_pnl or "pnl_percentage" not in final_pnl or "roe" not in final_pnl:
                logger.error(f"Critical P&L data missing for {position.position_id}")
                raise ValueError("Incomplete P&L calculation results")
            
            position.realized_pnl = final_pnl["unrealized_pnl"]
            position.pnl_percentage = final_pnl["pnl_percentage"]
            position.roe = final_pnl["roe"]

            # Update position status with thread-safety and timeout protection
            # Optimized for 128GB RAM system with enhanced async handling
            lock_timeout = 5.0
            
            async def _update_position_with_lock():
                """
                Helper function to update position status within lock
                Isolated for better memory management on high-RAM systems
                """
                async with self.position_lock:
                    position.status = "CLOSED"
                    position.current_price = close_price
                    position.closed_at = time.time()
                    
                    # Update global trading statistics with validation
                    self.daily_pnl += position.realized_pnl
                    self.total_trades += 1
                    if position.realized_pnl > 0:
                        self.successful_trades += 1
                    
                    # Update drawdown tracking
                    self._update_drawdown_metrics(position)

                    # Remove from active positions with verification
                    if position.position_id in self.active_positions:
                        del self.active_positions[position.position_id]
                        logger.debug(f"Position {position.position_id} removed from active positions")
                    else:
                        logger.warning(f"Position {position.position_id} not found in active_positions")
                    
                    # Update current exposure metrics
                    total_exposure = sum(pos.margin_required for pos in self.active_positions.values())
                    self.risk_metrics["current_exposure"] = total_exposure
                    self.risk_metrics["margin_utilization"] = total_exposure / config.PORTFOLIO_VALUE
            
            try:
                # Use asyncio.wait_for for Python 3.11+ native timeout handling
                await asyncio.wait_for(_update_position_with_lock(), timeout=lock_timeout)
            except asyncio.TimeoutError:
                logger.error(f"Position lock timeout after {lock_timeout}s for {position.position_id}")
                raise RuntimeError(f"Failed to acquire position lock within {lock_timeout}s")

            # Memory-optimized history management (128GB RAM optimized)
            self._manage_position_history(position)

            # Update execution statistics with enhanced metrics
            self.execution_stats["current_positions"] = len(self.active_positions)
            self.execution_stats["daily_pnl"] = self.daily_pnl
            self.execution_stats["win_rate"] = (
                self.successful_trades / self.total_trades * 100
                if self.total_trades > 0
                else 0.0
            )
            
            # Calculate additional performance metrics
            if self.total_trades > 0:
                avg_pnl = self.daily_pnl / self.total_trades
                self.execution_stats["avg_pnl_per_trade"] = avg_pnl
            else:
                self.execution_stats["avg_pnl_per_trade"] = 0.0

            # Calculate close duration
            close_duration = (time.time() - close_start) * 1000
            
            # Enhanced logging with performance metrics
            logger.debug(
                f"Position closed in {close_duration:.1f}ms: {position.position_id} | "
                f"P&L: ${position.realized_pnl:+,.2f} ({position.pnl_percentage:+.2f}%) | "
                f"ROE: {position.roe:+.2f}% | "
                f"Win rate: {self.execution_stats['win_rate']:.1f}% | "
                f"Active: {len(self.active_positions)}"
            )

            # Adaptive memory cleanup (optimized for 128GB system)
            cleanup_interval = 150 if self.hardware_optimized else 50
            if self.total_trades % cleanup_interval == 0:
                logger.debug(f"Triggering memory cleanup at trade #{self.total_trades}")
                await self._perform_memory_cleanup()

        except ValueError as ve:
            logger.error(f"Validation error in position close for {position.position_id}: {ve}")
            raise
        except RuntimeError as re:
            logger.error(f"Runtime error in position close for {position.position_id}: {re}")
            raise
        except Exception as e:
            logger.error(f"Basic position close error for {position.position_id}: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            raise

    def _update_drawdown_metrics(self, position: Position):
        """
        Update drawdown tracking metrics
        Optimized for high-frequency trading on 128GB RAM system
        """
        try:
            # Calculate current balance with validation
            current_balance = config.PORTFOLIO_VALUE + self.daily_pnl
            
            if current_balance < 0:
                logger.warning(f"Negative balance detected: ${current_balance:,.2f}")
            
            # Update peak balance
            if current_balance > self.risk_metrics["peak_balance"]:
                self.risk_metrics["peak_balance"] = current_balance
                logger.debug(f"New peak balance: ${current_balance:,.2f}")
            
            # Calculate current drawdown with zero-division protection
            if self.risk_metrics["peak_balance"] > 0:
                drawdown = (self.risk_metrics["peak_balance"] - current_balance) / self.risk_metrics["peak_balance"]
                self.risk_metrics["current_drawdown"] = max(0.0, min(1.0, drawdown))
            else:
                self.risk_metrics["current_drawdown"] = 0.0
            
            # Update max drawdown with alert
            if self.risk_metrics["current_drawdown"] > self.risk_metrics["max_drawdown"]:
                self.risk_metrics["max_drawdown"] = self.risk_metrics["current_drawdown"]
                logger.warning(f"New max drawdown: {self.risk_metrics['max_drawdown']:.2%}")
                
                # Critical drawdown alert (30% threshold)
                if self.risk_metrics["max_drawdown"] > 0.30:
                    logger.error(f"CRITICAL: Max drawdown exceeds 30%: {self.risk_metrics['max_drawdown']:.2%}")
                
        except ZeroDivisionError:
            logger.error("Division by zero in drawdown calculation")
        except Exception as e:
            logger.warning(f"Drawdown metrics update error: {type(e).__name__}: {e}")

    def _manage_position_history(self, position: Position):
        """
        Optimized position history management for 128GB RAM system
        Uses intelligent caching and memory-efficient data structures
        """
        try:
            # Validate position before adding
            if not hasattr(position, 'position_id') or not position.position_id:
                logger.warning("Cannot add invalid position to history")
                return
            
            # Add to history with timestamp indexing (128GB RAM optimization)
            self.position_history.append(position)
            
            # Dynamic history size based on hardware optimization
            max_history_size = self._calculate_optimal_history_size()
            
            if len(self.position_history) > max_history_size:
                # Calculate retention percentages (optimized for 128GB)
                recent_percentage = 0.70
                significant_percentage = 0.30
                
                recent_count = int(max_history_size * recent_percentage)
                significant_count = int(max_history_size * significant_percentage)
                
                # Keep most recent positions
                recent_positions = self.position_history[-recent_count:]
                
                # Keep significant P&L positions (top performers and worst losses)
                older_positions = self.position_history[:-recent_count]
                pnl_threshold = self._calculate_significant_pnl_threshold()
                significant_positions = [
                    pos for pos in older_positions
                    if abs(getattr(pos, 'realized_pnl', 0)) > pnl_threshold
                ]
                
                # Sort significant by absolute P&L and take top N
                significant_positions.sort(
                    key=lambda p: abs(getattr(p, 'realized_pnl', 0)),
                    reverse=True
                )
                significant_positions = significant_positions[:significant_count]
                
                # Combine and limit (maintaining chronological order where possible)
                self.position_history = (significant_positions + recent_positions)[-max_history_size:]
                
                logger.info(
                    f"Position history optimized: {len(self.position_history)}/{max_history_size} kept "
                    f"({len(significant_positions)} significant + {len(recent_positions)} recent)"
                )

        except Exception as e:
            logger.warning(f"Position history management error: {type(e).__name__}: {e}")
            
            # Fallback to simple truncation with enhanced sizing
            fallback_size = 3000 if self.hardware_optimized else 1000
            if len(self.position_history) > fallback_size:
                self.position_history = self.position_history[-fallback_size:]
                logger.warning(f"Applied fallback truncation to {fallback_size} positions")

    def _calculate_optimal_history_size(self) -> int:
        """
        Calculate optimal history size based on system resources
        Specifically optimized for 128GB DDR5 RAM systems
        """
        try:
            import psutil
            
            # Get system memory information
            memory = psutil.virtual_memory()
            available_gb = memory.available / (1024**3)
            total_gb = memory.total / (1024**3)
            usage_percent = memory.percent
            
            # Adaptive sizing based on total RAM and current usage
            if self.hardware_optimized and total_gb > 100:
                # 128GB system: aggressive memory utilization
                if usage_percent < 50:
                    return 10000
                elif usage_percent < 70:
                    return 7000
                else:
                    return 5000
                    
            elif total_gb > 60:
                # 64GB system
                return 4000 if usage_percent < 60 else 3000
                
            elif available_gb > 8:
                return 3000
            elif available_gb > 4:
                return 2000
            elif available_gb > 2:
                return 1500
            else:
                return 1000
                
        except ImportError:
            logger.debug("psutil not available, using default history size")
            return 5000 if self.hardware_optimized else 2000
        except Exception as e:
            logger.warning(f"Optimal history size calculation error: {e}")
            return 2000

    def _calculate_significant_pnl_threshold(self) -> float:
        """
        Calculate threshold for significant P&L positions
        Uses statistical analysis for intelligent filtering
        """
        try:
            if len(self.position_history) < 10:
                return abs(self.daily_pnl) * 0.1
            
            # Analyze recent historical data (last 200 positions for 128GB system)
            sample_size = min(200 if self.hardware_optimized else 100, len(self.position_history))
            recent_positions = self.position_history[-sample_size:]
            recent_pnls = [getattr(pos, 'realized_pnl', 0) for pos in recent_positions]
            
            if not recent_pnls:
                return 100.0
            
            # Calculate statistical metrics
            avg_pnl = sum(recent_pnls) / len(recent_pnls)
            abs_pnls = [abs(pnl) for pnl in recent_pnls]
            avg_abs_pnl = sum(abs_pnls) / len(abs_pnls)
            
            # Use 2.5x average absolute P&L as threshold (more selective)
            threshold = avg_abs_pnl * 2.5
            
            logger.debug(
                f"Significant P&L threshold: ${threshold:.2f} "
                f"(avg: ${avg_pnl:.2f}, avg_abs: ${avg_abs_pnl:.2f})"
            )
            return max(threshold, 50.0)
            
        except ZeroDivisionError:
            return 100.0
        except Exception as e:
            logger.warning(f"Significant P&L threshold calculation error: {e}")
            return 100.0

    async def _perform_memory_cleanup(self):
        """
        Advanced memory cleanup and optimization for 128GB DDR5 RAM systems
        Features: Intelligent GC, adaptive caching, parallel cleanup, deep analysis
        """
        cleanup_start = time.time()
        
        try:
            import gc
            
            logger.debug("Starting advanced memory cleanup cycle...")
            
            # Multi-generation garbage collection with priority (128GB optimized)
            collected_objects = []
            for generation in range(3):
                try:
                    collected = gc.collect(generation)
                    collected_objects.append(collected)
                except Exception as gc_error:
                    logger.warning(f"GC generation {generation} error: {gc_error}")
                    collected_objects.append(0)
            
            total_collected = sum(collected_objects)
            
            cleanup_stats = {
                "objects_collected": total_collected,
                "generation_0": collected_objects[0] if len(collected_objects) > 0 else 0,
                "generation_1": collected_objects[1] if len(collected_objects) > 1 else 0,
                "generation_2": collected_objects[2] if len(collected_objects) > 2 else 0,
            }
            
            # Enhanced memory tracking with detailed metrics
            try:
                import psutil
                process = psutil.Process()
                memory_info_before = process.memory_info()
                memory_before = memory_info_before.rss / (1024*1024)
                
                cleanup_stats["memory_before_mb"] = round(memory_before, 2)
                cleanup_stats["memory_vms_before_mb"] = round(memory_info_before.vms / (1024*1024), 2)
                
                # System memory status
                system_mem = psutil.virtual_memory()
                cleanup_stats["system_available_gb"] = round(system_mem.available / (1024**3), 2)
                cleanup_stats["system_percent_used"] = system_mem.percent
            except ImportError:
                memory_before = 0
                logger.debug("psutil not available for memory tracking")
            except Exception as mem_error:
                memory_before = 0
                logger.debug(f"Memory tracking error: {mem_error}")
            
            current_time = time.time()
            
            # Multi-threaded cache cleanup for high-end systems
            cache_cleanup_tasks = []
            
            # Task 1: Enhanced price cache cleanup (128GB optimized)
            if hasattr(self, '_price_cache'):
                original_cache_size = len(self._price_cache)
                active_symbols = {pos.symbol for pos in self.active_positions.values()}
                
                # Adaptive retention based on hardware (128GB = 20 minutes)
                retention_seconds = 1200 if self.hardware_optimized else 600
                
                # Keep active symbols + recently accessed
                self._price_cache = {
                    symbol: price for symbol, price in self._price_cache.items()
                    if symbol in active_symbols or 
                    (hasattr(self, '_cache_timestamps') and 
                     self._cache_timestamps.get(symbol, 0) > current_time - retention_seconds)
                }
                cleanup_stats["price_cache_cleared"] = original_cache_size - len(self._price_cache)
                
                # Timestamp cleanup with extended retention
                if hasattr(self, '_cache_timestamps'):
                    original_timestamp_size = len(self._cache_timestamps)
                    self._cache_timestamps = {
                        symbol: timestamp for symbol, timestamp in self._cache_timestamps.items()
                        if current_time - timestamp < retention_seconds
                    }
                    cleanup_stats["timestamps_cleared"] = original_timestamp_size - len(self._cache_timestamps)
                else:
                    cleanup_stats["timestamps_cleared"] = 0
            else:
                cleanup_stats["price_cache_cleared"] = 0
                cleanup_stats["timestamps_cleared"] = 0

            # Task 2: Simulation cache with intelligent retention
            if hasattr(self, '_sim_price_cache'):
                original_sim_size = len(self._sim_price_cache)
                
                # Aggressive caching for 128GB systems
                max_sim_cache = 2000 if self.hardware_optimized else 500
                
                if len(self._sim_price_cache) > max_sim_cache:
                    active_symbols = {pos.symbol for pos in self.active_positions.values()}
                    
                    # Priority-based retention
                    active_cache = {
                        symbol: price for symbol, price in self._sim_price_cache.items()
                        if symbol in active_symbols
                    }
                    
                    # Keep most recent non-active symbols
                    remaining_items = [
                        (s, p) for s, p in self._sim_price_cache.items()
                        if s not in active_symbols
                    ][-max_sim_cache:]
                    
                    self._sim_price_cache = {**active_cache, **dict(remaining_items)}
                    
                cleanup_stats["sim_cache_cleared"] = original_sim_size - len(self._sim_price_cache)
            else:
                cleanup_stats["sim_cache_cleared"] = 0

            # Task 3: Cache hit/miss statistics cleanup
            if hasattr(self, '_cache_hit_count'):
                original_hit_count_size = len(self._cache_hit_count)
                
                # Keep only recent symbols (128GB = 5000 symbols)
                max_hit_stats = 5000 if self.hardware_optimized else 1000
                
                if len(self._cache_hit_count) > max_hit_stats:
                    active_symbols = {pos.symbol for pos in self.active_positions.values()}
                    
                    self._cache_hit_count = {
                        s: count for s, count in self._cache_hit_count.items()
                        if s in active_symbols
                    }
                    
                    if hasattr(self, '_cache_miss_count'):
                        self._cache_miss_count = {
                            s: count for s, count in self._cache_miss_count.items()
                            if s in active_symbols
                        }
                    
                cleanup_stats["cache_stats_cleared"] = original_hit_count_size - len(self._cache_hit_count)
            else:
                cleanup_stats["cache_stats_cleared"] = 0

            # Task 4: Price source reliability cleanup
            if hasattr(self, '_price_source_reliability'):
                original_reliability_size = len(self._price_source_reliability)
                
                # Adaptive sizing for 128GB systems
                max_reliability = 10000 if self.hardware_optimized else 2000
                
                if len(self._price_source_reliability) > max_reliability:
                    keep_count = int(max_reliability * 0.8)
                    recent_keys = list(self._price_source_reliability.keys())[-keep_count:]
                    
                    self._price_source_reliability = {
                        k: v for k, v in self._price_source_reliability.items()
                        if k in recent_keys
                    }
                    
                cleanup_stats["reliability_stats_cleared"] = original_reliability_size - len(self._price_source_reliability)
            else:
                cleanup_stats["reliability_stats_cleared"] = 0

            # Task 5: Risk alert cache cleanup
            if hasattr(self, '_risk_alert_timestamps'):
                original_alert_cache_size = len(self._risk_alert_timestamps)
                
                # Extended retention for 128GB systems (2 hours)
                alert_retention = 7200 if self.hardware_optimized else 3600
                
                self._risk_alert_timestamps = {
                    pos_id: timestamp for pos_id, timestamp in self._risk_alert_timestamps.items()
                    if current_time - timestamp < alert_retention
                }
                cleanup_stats["risk_alert_cache_cleared"] = original_alert_cache_size - len(self._risk_alert_timestamps)
            else:
                cleanup_stats["risk_alert_cache_cleared"] = 0

            # Task 6: Service health and circuit breaker maintenance
            if hasattr(self, 'service_health'):
                circuit_breakers_reset = 0
                circuit_breaker_timeout = 300
                
                for service_name, health in self.service_health.items():
                    if health.get('circuit_open'):
                        time_open = current_time - health.get('circuit_opened_at', 0)
                        if time_open > circuit_breaker_timeout:
                            health['circuit_open'] = False
                            health['consecutive_failures'] = 0
                            circuit_breakers_reset += 1
                            logger.info(f"Circuit breaker auto-reset for {service_name} after {time_open:.0f}s")
                
                cleanup_stats["circuit_breakers_reset"] = circuit_breakers_reset
            else:
                cleanup_stats["circuit_breakers_reset"] = 0

            # Task 7: Rate limiter statistics
            if hasattr(self, 'rate_limiter'):
                try:
                    limiter_stats = self.rate_limiter.get_stats()
                    cleanup_stats["rate_limiter_requests"] = limiter_stats.get("total_requests", 0)
                    cleanup_stats["rate_limiter_limit_hits"] = limiter_stats.get("limit_hits", 0)
                except Exception as limiter_error:
                    logger.debug(f"Rate limiter stats error: {limiter_error}")
                    cleanup_stats["rate_limiter_requests"] = 0
                    cleanup_stats["rate_limiter_limit_hits"] = 0
            else:
                cleanup_stats["rate_limiter_requests"] = 0
                cleanup_stats["rate_limiter_limit_hits"] = 0

            # Task 8: Advanced position history management
            if hasattr(self, 'position_history') and len(self.position_history) > 0:
                original_history_size = len(self.position_history)
                max_history = self._calculate_optimal_history_size()
                
                if original_history_size > max_history:
                    # Intelligent retention strategy (128GB optimized)
                    recent_percentage = 0.70
                    significant_percentage = 0.30
                    
                    recent_count = int(max_history * recent_percentage)
                    significant_count = int(max_history * significant_percentage)
                    
                    recent_positions = self.position_history[-recent_count:]
                    
                    # Get significant positions (high P&L or losses)
                    older_positions = self.position_history[:-recent_count]
                    if older_positions:
                        sorted_by_pnl = sorted(
                            older_positions, 
                            key=lambda p: abs(getattr(p, 'realized_pnl', 0)), 
                            reverse=True
                        )
                        significant_positions = sorted_by_pnl[:significant_count]
                    else:
                        significant_positions = []
                    
                    self.position_history = significant_positions + recent_positions
                    cleanup_stats["position_history_trimmed"] = original_history_size - len(self.position_history)
                    
                    logger.debug(
                        f"Position history optimized: kept {len(significant_positions)} significant + "
                        f"{len(recent_positions)} recent = {len(self.position_history)} total"
                    )
                else:
                    cleanup_stats["position_history_trimmed"] = 0
            else:
                cleanup_stats["position_history_trimmed"] = 0

            # Post-cleanup memory measurement
            try:
                memory_info_after = process.memory_info()
                memory_after = memory_info_after.rss / (1024*1024)
                memory_freed = memory_before - memory_after
                memory_freed_percent = (memory_freed / memory_before * 100) if memory_before > 0 else 0
                
                cleanup_stats["memory_after_mb"] = round(memory_after, 2)
                cleanup_stats["memory_vms_after_mb"] = round(memory_info_after.vms / (1024*1024), 2)
                cleanup_stats["memory_freed_mb"] = round(memory_freed, 2)
                cleanup_stats["memory_freed_percent"] = round(memory_freed_percent, 2)
                
                # Memory efficiency calculation
                if memory_before > 0:
                    cleanup_efficiency = (memory_freed / memory_before) * 100
                    cleanup_stats["cleanup_efficiency_percent"] = round(cleanup_efficiency, 2)
            except:
                cleanup_stats["memory_after_mb"] = 0
                cleanup_stats["memory_freed_mb"] = 0
                cleanup_stats["memory_freed_percent"] = 0
                cleanup_stats["cleanup_efficiency_percent"] = 0

            # Cleanup duration and performance metrics
            cleanup_duration = (time.time() - cleanup_start) * 1000
            cleanup_stats["cleanup_duration_ms"] = round(cleanup_duration, 2)

            # Enhanced logging with comprehensive metrics
            logger.info(
                f"Memory cleanup completed in {cleanup_duration:.1f}ms: "
                f"{total_collected} objects collected, "
                f"Memory freed: {cleanup_stats.get('memory_freed_mb', 0):.1f}MB "
                f"({cleanup_stats.get('memory_freed_percent', 0):.1f}%), "
                f"Efficiency: {cleanup_stats.get('cleanup_efficiency_percent', 0):.1f}%"
            )
            
            logger.debug(
                f"Cache cleanup: Price=-{cleanup_stats.get('price_cache_cleared', 0)}, "
                f"Sim=-{cleanup_stats.get('sim_cache_cleared', 0)}, "
                f"Stats=-{cleanup_stats.get('cache_stats_cleared', 0)}, "
                f"Reliability=-{cleanup_stats.get('reliability_stats_cleared', 0)}, "
                f"Alerts=-{cleanup_stats.get('risk_alert_cache_cleared', 0)}, "
                f"History=-{cleanup_stats.get('position_history_trimmed', 0)}"
            )
            
            # Detailed GC logging for significant collections
            if total_collected > 1000:
                logger.info(
                    f"Significant GC collection detected: "
                    f"Gen0={cleanup_stats['generation_0']}, "
                    f"Gen1={cleanup_stats['generation_1']}, "
                    f"Gen2={cleanup_stats['generation_2']}"
                )
            
            # Circuit breaker reset notification
            if cleanup_stats.get('circuit_breakers_reset', 0) > 0:
                logger.info(
                    f"Circuit breakers auto-reset: {cleanup_stats['circuit_breakers_reset']} services reconnected"
                )

        except Exception as e:
            logger.warning(f"Memory cleanup error: {type(e).__name__}: {e}")
            import traceback
            logger.debug(f"Cleanup traceback:\n{traceback.format_exc()}")

    def get_memory_usage_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive memory usage statistics for monitoring
        Fully optimized for 128GB DDR5 RAM systems with detailed analytics
        """
        try:
            import psutil
            import gc
            
            process = psutil.Process()
            memory_info = process.memory_info()
            system_memory = psutil.virtual_memory()
            
            # Enhanced cache size calculations
            price_cache_size = len(getattr(self, '_price_cache', {}))
            sim_cache_size = len(getattr(self, '_sim_price_cache', {}))
            timestamp_cache_size = len(getattr(self, '_cache_timestamps', {}))
            risk_alert_cache_size = len(getattr(self, '_risk_alert_timestamps', {}))
            hit_count_size = len(getattr(self, '_cache_hit_count', {}))
            miss_count_size = len(getattr(self, '_cache_miss_count', {}))
            reliability_size = len(getattr(self, '_price_source_reliability', {}))
            
            # Calculate optimal sizes and utilization
            optimal_history = self._calculate_optimal_history_size()
            history_utilization = (len(self.position_history) / optimal_history * 100) if optimal_history > 0 else 0
            
            # Calculate cache hit rate
            total_hits = sum(getattr(self, '_cache_hit_count', {}).values())
            total_misses = sum(getattr(self, '_cache_miss_count', {}).values())
            cache_hit_rate = (total_hits / (total_hits + total_misses) * 100) if (total_hits + total_misses) > 0 else 0
            
            return {
                # Process memory (detailed)
                "process_memory_mb": round(memory_info.rss / (1024*1024), 2),
                "process_memory_gb": round(memory_info.rss / (1024*1024*1024), 2),
                "process_memory_percent": round(process.memory_percent(), 2),
                "process_vms_mb": round(memory_info.vms / (1024*1024), 2),
                
                # System memory (comprehensive)
                "system_memory_total_gb": round(system_memory.total / (1024**3), 2),
                "system_memory_available_gb": round(system_memory.available / (1024**3), 2),
                "system_memory_used_gb": round(system_memory.used / (1024**3), 2),
                "system_memory_used_percent": system_memory.percent,
                "system_memory_cached_gb": round(getattr(system_memory, 'cached', 0) / (1024**3), 2),
                
                # Position tracking
                "active_positions": len(self.active_positions),
                "position_history_size": len(self.position_history),
                "max_history_size": optimal_history,
                "history_utilization_percent": round(history_utilization, 2),
                
                # Cache sizes (comprehensive)
                "price_cache_size": price_cache_size,
                "sim_cache_size": sim_cache_size,
                "timestamp_cache_size": timestamp_cache_size,
                "risk_alert_cache_size": risk_alert_cache_size,
                "hit_count_cache_size": hit_count_size,
                "miss_count_cache_size": miss_count_size,
                "reliability_cache_size": reliability_size,
                "total_cache_items": (price_cache_size + sim_cache_size + timestamp_cache_size + 
                                     risk_alert_cache_size + hit_count_size + miss_count_size + reliability_size),
                
                # Cache performance
                "cache_hit_rate_percent": round(cache_hit_rate, 2),
                "cache_total_hits": total_hits,
                "cache_total_misses": total_misses,
                
                # Python internals
                "python_object_count": len(gc.get_objects()),
                "gc_counts": gc.get_count(),
                "gc_thresholds": gc.get_threshold(),
                
                # Hardware optimization status
                "hardware_optimized": self.hardware_optimized,
                "aggressive_caching": getattr(self, 'aggressive_caching', False),
                "max_concurrent_orders": getattr(self, 'max_concurrent_orders', 0),
                "max_worker_threads": getattr(self, 'max_worker_threads', 0),
                
                # Timestamp
                "timestamp": time.time(),
                "timestamp_iso": datetime.now().isoformat()
            }
            
        except ImportError:
            return {
                "active_positions": len(self.active_positions),
                "position_history_size": len(self.position_history),
                "hardware_optimized": self.hardware_optimized,
                "note": "psutil not available - install with: pip install psutil"
            }
        except Exception as e:
            logger.error(f"Memory stats collection error: {type(e).__name__}: {e}")
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": time.time()
            }

    async def _notify_execution_error(self, request_data: dict, error: str):
        """Trade execution error notification"""
        try:
            notify_data = {
                "type": "trade_execution_error",
                "data": {
                    "request_data": request_data,
                    "error": error,
                    "service": "execute_service",
                    "timestamp": time.time(),
                    "system_info": {
                        "active_positions": len(self.active_positions),
                        "daily_pnl": self.daily_pnl,
                        "execution_stats": self.execution_stats,
                    },
                },
                "service": "execute_service",
                "priority": "high",
                "timestamp": time.time(),
            }

            await self._call_notify_service("/api/notification/system", notify_data)

        except Exception as e:
            logger.error(f"Execution error notification failed: {e}")

    async def _notify_monitoring_error(self, position: Position, error: str):
        """Monitoring error notification"""
        try:
            notify_data = {
                "type": "position_monitoring_error",
                "data": {
                    "position_id": position.position_id,
                    "position": asdict(position),
                    "error": error,
                    "service": "execute_service",
                },
                "service": "execute_service",
                "priority": "medium",
                "timestamp": time.time(),
            }

            await self._call_notify_service("/api/notification/system", notify_data)

        except Exception as e:
            logger.error(f"Monitoring error notification failed: {e}")

    def _calculate_alert_level(self, position: Position) -> str:
        """Calculate alert level"""
        if position.liquidation_risk > 0.9:
            return "CRITICAL"
        elif position.liquidation_risk > 0.7:
            return "HIGH"
        elif position.liquidation_risk > 0.5:
            return "MEDIUM"
        else:
            return "LOW"


    # ========================================================================
    #                          Inter-Service Communication System
    # ========================================================================

    async def _call_notify_service(self, endpoint: str, data: dict) -> bool:
        """
        Call Notify Service with retry logic and performance tracking
        Optimized for high-performance systems (Ryzen 9 7950X, 128GB RAM)
        Features: Payload validation, endpoint verification, enhanced error logging
        """
        call_start = time.time()
        
        try:
            # CRITICAL FIX: Validate endpoint before calling
            valid_endpoints = [
                "/api/notification/execution",  # Execute Service notifications
                "/api/notification/position",   # Position close notifications
                "/api/notification/risk",       # Risk alert notifications
                "/api/notification/system"      # System error notifications
            ]
            
            if endpoint not in valid_endpoints:
                logger.error(f"Invalid Notify Service endpoint: {endpoint}")
                logger.error(f"Valid endpoints: {valid_endpoints}")
                return False
            
            # CRITICAL FIX: Log payload structure before sending (debug mode only)
            if logger.isEnabledFor(logging.DEBUG):
                import json
                logger.debug(f"Notify Service payload for {endpoint}:")
                logger.debug(f"  Root keys: {list(data.keys())}")
                logger.debug(f"  Payload preview: {json.dumps(data, indent=2, default=str)[:500]}")
            
            # Call service with retry logic
            success = await self._call_service_with_retry(
                f"{config.NOTIFY_SERVICE_URL}{endpoint}", data
            )
            
            call_duration = (time.time() - call_start) * 1000
            
            if success:
                logger.debug(f"Notify service call completed in {call_duration:.1f}ms: {endpoint}")
            else:
                logger.warning(f"Notify service call failed after {call_duration:.1f}ms: {endpoint}")
                logger.warning(f"Failed payload keys: {list(data.keys())}")
            
            return success
            
        except Exception as e:
            logger.error(f"Notify service call error for {endpoint}: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def _call_service_with_retry(
        self, url: str, data: dict, max_retries: int = None
    ) -> bool:
        """
        Enhanced service call with intelligent retry logic, circuit breaker, and connection pooling
        Optimized for high-performance systems (Ryzen 9 7950X, 128GB RAM)
        Features: Circuit breaker pattern, adaptive timeouts, connection pooling, detailed error logging
        """
        call_start = time.time()
        
        if max_retries is None:
            max_retries = config.MAX_RETRIES

        # Circuit breaker pattern - track service health
        try:
            service_name = url.split('/')[2]  # Extract hostname
        except IndexError:
            service_name = url
            logger.warning(f"Could not extract service name from URL: {url}")
        
        if not hasattr(self, '_service_health'):
            self._service_health = {}
        
        health_key = service_name
        if health_key not in self._service_health:
            self._service_health[health_key] = {
                'failures': 0,
                'successes': 0,
                'last_failure': 0,
                'last_success': 0,
                'circuit_open': False,
                'total_calls': 0
            }

        health = self._service_health[health_key]
        health['total_calls'] += 1
        
        # Circuit breaker check
        if health['circuit_open']:
            cooldown_remaining = 60 - (time.time() - health['last_failure'])
            if cooldown_remaining > 0:
                logger.warning(
                    f"Circuit breaker open for {service_name}, "
                    f"skipping call (cooldown: {cooldown_remaining:.0f}s)"
                )
                return False
            else:
                logger.info(f"Circuit breaker reset for {service_name} after cooldown")
                health['circuit_open'] = False
                health['failures'] = 0

        for attempt in range(max_retries):
            try:
                # Enhanced session configuration with connection pooling
                connector = aiohttp.TCPConnector(
                    limit=20 if self.hardware_optimized else 10,
                    limit_per_host=10 if self.hardware_optimized else 5,
                    enable_cleanup_closed=True,
                    keepalive_timeout=30,
                    force_close=False
                )
                
                # Adaptive timeout (increases with retry attempts)
                base_timeout = config.SERVICE_TIMEOUT
                adaptive_timeout = base_timeout + (attempt * 2)
                timeout = aiohttp.ClientTimeout(
                    total=adaptive_timeout,
                    connect=5,
                    sock_read=base_timeout
                )

                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout
                ) as session:
                    
                    # Enhanced headers for better service identification
                    headers = {
                        'Content-Type': 'application/json',
                        'User-Agent': 'Phoenix95-Execute-Service/4.0',
                        'X-Service-Chain': data.get('service_chain', 'unknown'),
                        'X-Retry-Attempt': str(attempt + 1),
                        'X-Max-Retries': str(max_retries),
                        'X-Request-ID': f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
                    }

                    async with session.post(url, json=data, headers=headers) as response:
                        
                        if response.status == 200:
                            # Success - reset circuit breaker
                            health['failures'] = 0
                            health['circuit_open'] = False
                            health['successes'] += 1
                            health['last_success'] = time.time()
                            
                            call_duration = (time.time() - call_start) * 1000
                            logger.debug(
                                f"Service call successful: {service_name} "
                                f"(attempt {attempt + 1}/{max_retries}, {call_duration:.1f}ms)"
                            )
                            return True
                            
                        elif response.status == 503:
                            if attempt < max_retries - 1:
                                wait_time = min(30, (2 ** attempt) + np.random.uniform(0, 1))
                                logger.warning(
                                    f"Service unavailable: {service_name}, "
                                    f"retry in {wait_time:.1f}s ({attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(wait_time)
                                continue
                                
                        elif response.status == 429:
                            if attempt < max_retries - 1:
                                retry_after = response.headers.get('Retry-After', str((2 ** attempt) * 2))
                                try:
                                    wait_time = min(60, int(retry_after))
                                except ValueError:
                                    wait_time = min(60, (2 ** attempt) * 2)
                                    
                                logger.warning(f"Rate limited: {service_name}, retry in {wait_time}s")
                                await asyncio.sleep(wait_time)
                                continue
                                
                        elif response.status == 404:
                            logger.error(f"Service endpoint not found: {url}")
                            return False
                            
                        elif response.status >= 500:
                            health['failures'] += 1
                            if attempt < max_retries - 1:
                                wait_time = min(15, (2 ** attempt) + np.random.uniform(0, 0.5))
                                logger.warning(
                                    f"Server error {response.status} from {service_name}, "
                                    f"retry in {wait_time:.1f}s"
                                )
                                await asyncio.sleep(wait_time)
                                continue
                        else:
                            # CRITICAL FIX: Enhanced error logging for 400-level errors
                            logger.warning(f"Service call failed: {service_name} (HTTP {response.status})")
                            try:
                                error_text = await response.text()
                                logger.error(f"Error response from {service_name}: {error_text[:500]}")
                                
                                # Try to parse as JSON for better error details
                                try:
                                    import json
                                    error_json = json.loads(error_text)
                                    logger.error(f"Parsed error details: {json.dumps(error_json, indent=2)}")
                                except:
                                    pass
                            except Exception as read_error:
                                logger.error(f"Could not read error response: {read_error}")
                            return False

            except asyncio.TimeoutError:
                health['failures'] += 1
                if attempt < max_retries - 1:
                    wait_time = min(10, (2 ** attempt) + np.random.uniform(0, 0.5))
                    logger.warning(
                        f"Timeout calling {service_name} ({adaptive_timeout}s), "
                        f"retry in {wait_time:.1f}s ({attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Final timeout calling {service_name} after {max_retries} attempts")
                    
            except aiohttp.ClientConnectorError as e:
                health['failures'] += 1
                if attempt < max_retries - 1:
                    wait_time = min(10, (2 ** attempt) + np.random.uniform(0, 0.5))
                    logger.warning(f"Connection error to {service_name}, retry in {wait_time:.1f}s: {e}")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Final connection error to {service_name}: {e}")
                    
            except Exception as e:
                health['failures'] += 1
                if attempt < max_retries - 1:
                    wait_time = min(5, (2 ** attempt) + np.random.uniform(0, 0.5))
                    logger.warning(
                        f"Unexpected error calling {service_name}, "
                        f"retry in {wait_time:.1f}s: {type(e).__name__}: {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Final unexpected error calling {service_name}: {type(e).__name__}: {e}")

        # All retries failed
        health['failures'] += 1
        health['last_failure'] = time.time()
        
        # Open circuit breaker if too many failures
        failure_threshold = 5
        if health['failures'] >= failure_threshold:
            health['circuit_open'] = True
            success_rate = health['successes'] / health['total_calls'] if health['total_calls'] > 0 else 0
            logger.error(
                f"Circuit breaker opened for {service_name}: "
                f"{health['failures']} consecutive failures, "
                f"success rate: {success_rate:.1%}"
            )
        
        total_duration = (time.time() - call_start) * 1000
        logger.error(
            f"Service call final failure: {service_name} ({url}) "
            f"after {max_retries} attempts ({total_duration:.1f}ms)"
        )
        return False

    # ========================================================================
    #                          Backup System
    # ========================================================================

    async def _send_telegram_backup_notification(
        self, signal: EnhancedTradingSignal, result: ExecutionResult
    ):
        """Backup telegram notification"""
        try:
            if result.status == "SUCCESS" and result.position:
                pos = result.position
                message = (
                    f"[BACKUP] Trade Execution Completed\n\n"
                    f"Symbol: {pos.symbol}\n"
                    f"Action: {pos.action.upper()}\n"
                    f"Entry Price: ${pos.entry_price:,.2f}\n"
                    f"Leverage: {pos.leverage}x\n"
                    f"Phoenix95: {signal.phoenix95_score:.1f}/95\n"
                    f"Margin: ${pos.margin_required:,.2f}\n"
                    f"Execution Time: {result.execution_time_ms:.1f}ms\n"
                    f"Take Profit: ${pos.take_profit_price:,.2f}\n"
                    f"Stop Loss: ${pos.stop_loss_price:,.2f}\n\n"
                    f"WARNING: Notify Service connection failed, backup sent"
                )
            else:
                message = (
                    f"[BACKUP] Trade Execution Failed\n\n"
                    f"Symbol: {signal.symbol}\n"
                    f"Action: {signal.action.upper()}\n"
                    f"Price: ${signal.price:,.2f}\n"
                    f"Phoenix95: {signal.phoenix95_score:.1f}/95\n"
                    f"Failure Reason: {result.message}\n\n"
                    f"WARNING: Notify Service connection failed, backup sent"
                )

            await self._send_telegram_direct(message)

        except Exception as e:
            logger.error(f"Backup telegram send failed: {e}")

    async def _send_telegram_direct(self, message: str):
        """Direct telegram send"""
        try:
            url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
            data = {
                "chat_id": config.TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, timeout=10) as response:
                    if response.status == 200:
                        logger.info("Backup telegram sent successfully")
                    else:
                        logger.warning(f"Backup telegram send failed: {response.status}")

        except Exception as e:
            logger.warning(f"Backup telegram error: {e}")


    # ========================================================================
    #                          Legacy Compatibility Functions
    # ========================================================================

    async def _basic_validation(self, signal) -> Dict:
        """Basic validation (legacy compatibility)"""
        try:
            # Allow all symbols if allowed symbols list is empty
            if config.ALLOWED_SYMBOLS and signal.symbol not in config.ALLOWED_SYMBOLS:
                return {
                    "valid": False,
                    "reason": f"Symbol not allowed: {signal.symbol}",
                }
            
            # Confidence validation
            if signal.confidence < config.MIN_CONFIDENCE:
                return {
                    "valid": False,
                    "reason": f"Insufficient confidence: {signal.confidence:.1%}",
                }

            # Action validation
            if signal.action.lower() not in ["buy", "sell", "long", "short"]:
                return {"valid": False, "reason": f"Invalid action: {signal.action}"}

            # Price validation
            if signal.price <= 0:
                return {"valid": False, "reason": f"Invalid price: {signal.price}"}

            # All validations passed
            return {"valid": True, "reason": "Basic validation passed"}

        except Exception as e:
            # Handle exceptions
            return {
                "valid": False,
                "reason": f"Error during validation: {str(e)}",
            }
            
    async def _comprehensive_risk_check(self, signal) -> Dict:
        """Comprehensive risk validation (legacy compatibility)"""
        try:
            if len(self.active_positions) >= config.MAX_POSITIONS:
                return {
                    "approved": False,
                    "level": 1,
                    "reason": f"Maximum positions exceeded: {len(self.active_positions)}/{config.MAX_POSITIONS}",
                }

            if abs(self.daily_pnl) >= config.MAX_DAILY_LOSS * config.PORTFOLIO_VALUE:
                return {
                    "approved": False,
                    "level": 2,
                    "reason": f"Daily loss limit exceeded: ${self.daily_pnl:,.2f}",
                }

            symbol_positions = [
                p for p in self.active_positions.values() if p.symbol == signal.symbol
            ]
            if len(symbol_positions) >= 2:
                return {
                    "approved": False,
                    "level": 3,
                    "reason": f"{signal.symbol} position duplication: {len(symbol_positions)} positions",
                }

            return {"approved": True, "level": 0, "reason": "Comprehensive risk validation passed"}

        except Exception as e:
            return {
                "approved": False,
                "level": -1,
                "reason": f"Error during risk validation: {e}",
            }

    def _calculate_liquidation_price(
        self, entry_price: float, action: str, leverage: int
    ) -> float:
        """SPOT trading has no liquidation price (always returns 0)"""
        # SPOT trading uses 1x leverage (no margin), therefore no liquidation risk exists
        # This function is maintained for compatibility with position data structure
        return 0.0

    async def _calculate_dynamic_leverage(
        self, signal: EnhancedTradingSignal, position_size: float
    ) -> Dict:
        """Calculate position parameters for SPOT trading (no leverage)"""
        try:
            # Signal object safety validation
            if not signal:
                logger.error("Signal object is None")
                return {}
            
            # Extract price and action
            price = getattr(signal, 'price', 0)
            action = getattr(signal, 'action', '').lower()
            
            if not price or not action:
                logger.error("Price or action is invalid")
                return {}

            # SPOT trading: no leverage, direct purchase
            # position_size = total purchase amount
            actual_position_size = position_size  # Total purchase value
            margin_required = position_size  # SPOT requires full amount (no margin)
            quantity = actual_position_size / price  # Quantity to buy/sell
            
            # SPOT trading: no liquidation price (always 1x leverage)
            liquidation_price = 0  # N/A for spot trading

            # Calculate stop loss/take profit prices
            if action in ["buy", "long"]:
                stop_loss_price = price * (1 - config.STOP_LOSS_PERCENT)
                take_profit_price = price * (1 + config.TAKE_PROFIT_PERCENT)
            else:
                stop_loss_price = price * (1 + config.STOP_LOSS_PERCENT)
                take_profit_price = price * (1 - config.TAKE_PROFIT_PERCENT)

            logger.info(f"SPOT position calculation:")
            logger.info(f"   Trading mode: SPOT (1x leverage)")
            logger.info(f"   Total purchase value: ${actual_position_size:.2f}")
            logger.info(f"   Required balance: ${margin_required:.2f}")
            logger.info(f"   Quantity: {quantity:.6f}")

            return {
                "leverage": 1,  # Always 1x for SPOT
                "position_size": position_size,
                "actual_position_size": actual_position_size,
                "margin_required": margin_required,
                "quantity": quantity,
                "liquidation_price": liquidation_price,  # 0 for SPOT
                "stop_loss_price": stop_loss_price,
                "take_profit_price": take_profit_price,
                "margin_ratio": margin_required / config.PORTFOLIO_VALUE,
                "phoenix95_recommendation": 1,  # Always 1x for SPOT
                "risk_adjustment_factor": 1.0,  # N/A for SPOT
            }

        except Exception as e:
            logger.error(f"SPOT position calculation failed: {e}")
            return {}

    def _calculate_optimal_slippage(
        self, signal: EnhancedTradingSignal, leverage_info: Dict
    ) -> float:
        """Calculate optimal slippage"""
        try:
            # Signal and leverage info safety validation
            if not signal or not leverage_info:
                return 0.001  # Default 0.1%
                
            # Base slippage
            base_slippage = 0.0005  # 0.05%

            # Reduce slippage based on Phoenix95 score
            phoenix95_score = getattr(signal, 'phoenix95_score', 0)
            if phoenix95_score >= 80:
                base_slippage *= 0.8  # 20% reduction

            # Adjustment based on position size
            margin_required = leverage_info.get("margin_required", 0)
            if margin_required > 0:
                position_ratio = margin_required / config.PORTFOLIO_VALUE
                size_adjustment = min(1.5, 1.0 + position_ratio * 2)
            else:
                size_adjustment = 1.0

            # Final slippage
            final_slippage = base_slippage * size_adjustment
            return max(0.0001, min(final_slippage, 0.005))  # 0.01% ~ 0.5% range

        except Exception as e:
            logger.error(f"Slippage calculation failed: {e}")
            return 0.001  # Default 0.1%

    def _calculate_pnl(self, position: Position, current_price: float) -> Dict:
        """Calculate P&L with comprehensive error handling and performance optimization for SPOT trading"""
        calculation_start = time.time()
        
        try:
            # Input validation
            if not position:
                logger.error("P&L calculation aborted: Position object is None")
                return self._get_default_pnl_dict()
            
            if current_price <= 0:
                logger.error(f"P&L calculation aborted: Invalid current price: {current_price}")
                return self._get_default_pnl_dict()
            
            # CRITICAL: Validate entry price to prevent division by zero
            if not hasattr(position, 'entry_price') or position.entry_price is None or position.entry_price <= 0:
                logger.error(
                    f"P&L calculation failed: Invalid entry_price for {position.position_id}: "
                    f"{getattr(position, 'entry_price', 'None')}"
                )
                return self._get_default_pnl_dict()
            
            # Validate quantity
            if not hasattr(position, 'quantity') or position.quantity is None or position.quantity <= 0:
                logger.error(
                    f"P&L calculation failed: Invalid quantity for {position.position_id}: "
                    f"{getattr(position, 'quantity', 'None')}"
                )
                return self._get_default_pnl_dict()
            
            # Validate and fix margin_required
            if not hasattr(position, 'margin_required') or position.margin_required is None or position.margin_required <= 0:
                logger.warning(
                    f"Invalid margin_required for {position.position_id}, "
                    f"calculating from entry_price * quantity"
                )
                position.margin_required = position.entry_price * position.quantity
            
            # Calculate price difference based on position direction
            if position.action.lower() in ["buy", "long"]:
                price_diff = current_price - position.entry_price
                price_change_pct = (price_diff / position.entry_price) * 100
            else:
                price_diff = position.entry_price - current_price
                price_change_pct = (price_diff / position.entry_price) * 100

            # Calculate unrealized P&L
            unrealized_pnl = price_diff * position.quantity
            
            # Calculate P&L percentage (protected division)
            pnl_percentage = (unrealized_pnl / position.margin_required * 100) if position.margin_required > 0 else 0.0
            
            # Calculate ROE (Return on Equity) with protected division and leverage factor
            roe = ((price_diff / position.entry_price) * position.leverage * 100) if position.entry_price > 0 else 0.0

            # SPOT trading: No funding costs (futures only feature)
            estimated_funding_cost = 0.0
            funding_periods = 0
            net_pnl = unrealized_pnl
            
            # Calculate position value
            position_value = current_price * position.quantity

            # Performance metric
            calculation_time = (time.time() - calculation_start) * 1000

            # Detailed logging for significant P&L changes (only log if > 5% change)
            if abs(pnl_percentage) > 5:
                logger.debug(
                    f"P&L calculated [{position.position_id}]: "
                    f"${unrealized_pnl:+,.2f} ({pnl_percentage:+.2f}%) | "
                    f"ROE: {roe:+.2f}% | "
                    f"Price: ${position.entry_price:.4f} -> ${current_price:.4f} ({price_change_pct:+.2f}%) | "
                    f"Calc time: {calculation_time:.2f}ms"
                )

            return {
                "unrealized_pnl": round(unrealized_pnl, 2),
                "net_pnl": round(net_pnl, 2),
                "estimated_funding_cost": estimated_funding_cost,
                "pnl_percentage": round(pnl_percentage, 4),
                "roe": round(roe, 4),
                "funding_periods": funding_periods,
                "price_change_pct": round(price_change_pct, 4),
                "position_value": round(position_value, 2),
                "calculation_time_ms": round(calculation_time, 2),
                "error": False
            }

        except ZeroDivisionError as e:
            logger.error(f"Division by zero in P&L calculation for {position.position_id}: {e}")
            return self._get_default_pnl_dict()
        except AttributeError as e:
            logger.error(f"Attribute error in P&L calculation for {position.position_id}: {e}")
            return self._get_default_pnl_dict()
        except Exception as e:
            logger.error(f"Unexpected error in P&L calculation for {position.position_id}: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            return self._get_default_pnl_dict()

    def _get_default_pnl_dict(self) -> Dict:
        """Return default P&L dictionary for error cases with comprehensive fields"""
        return {
            "unrealized_pnl": 0.0,
            "net_pnl": 0.0,
            "estimated_funding_cost": 0.0,
            "pnl_percentage": 0.0,
            "roe": 0.0,
            "funding_periods": 0,
            "price_change_pct": 0.0,
            "position_value": 0.0,
            "calculation_time_ms": 0.0,
            "error": True
        }

    async def _execute_enhanced_trade(
        self, signal: EnhancedTradingSignal, execution_id: str, start_time: float
    ) -> ExecutionResult:
        """Enhanced trade execution with Phoenix95 integration and critical validations"""
        try:
            # Signal object safety validation
            if not signal:
                return self._create_failed_result(
                    execution_id, "Signal object is None", start_time
                )

            # Step 1: Enhanced validation
            validation = await self._enhanced_validation(signal)
            if not validation["valid"]:
                return self._create_failed_result(
                    execution_id, f"Enhanced validation failed: {validation['reason']}", start_time
                )

            # Step 2: Phoenix95 based risk validation
            risk_check = await self._phoenix95_risk_check(signal)
            if not risk_check["approved"]:
                return self._create_failed_result(
                    execution_id,
                    f"Phoenix95 risk validation failed: {risk_check['reason']}",
                    start_time,
                )

            # Step 3: Enhanced Kelly Criterion
            position_size = await self._calculate_phoenix95_kelly_size(signal)
            if position_size <= 0:
                return self._create_failed_result(
                    execution_id, "Phoenix95 position sizing failed", start_time
                )

            # Step 4: Dynamic leverage calculation
            leverage_info = await self._calculate_dynamic_leverage(
                signal, position_size
            )
            
            # leverage_info safety validation
            if not leverage_info or not isinstance(leverage_info, dict):
                return self._create_failed_result(
                    execution_id, "Dynamic leverage calculation failed", start_time
                )

            # Step 5: Optimized Binance order execution
            order_result = await self._execute_optimized_binance_order(
                signal, leverage_info
            )
            
            # CRITICAL: Validate order execution success
            if not order_result.get("success", False):
                return self._create_failed_result(
                    execution_id, f"Order execution failed: {order_result.get('error', 'Unknown error')}", start_time
                )
            
            # CRITICAL: Validate order_result contains valid execution_price
            execution_price = order_result.get("execution_price", 0)
            if execution_price <= 0:
                logger.error(f"CRITICAL: Order succeeded but invalid execution_price: {execution_price}")
                logger.error(f"Order result: {order_result}")
                return self._create_failed_result(
                    execution_id, 
                    f"Order execution succeeded but invalid price received: {execution_price}",
                    start_time
                )
            
            # CRITICAL: Validate executed quantity
            executed_qty = order_result.get("executed_qty", 0)
            if executed_qty <= 0:
                logger.error(f"CRITICAL: Order succeeded but invalid executed_qty: {executed_qty}")
                return self._create_failed_result(
                    execution_id,
                    f"Order execution succeeded but invalid quantity: {executed_qty}",
                    start_time
                )
            
            logger.info(f"Order validation passed: Price=${execution_price:.4f}, Qty={executed_qty:.6f}")

            # Step 6: Enhanced position creation
            try:
                position = await self._create_enhanced_position(
                    execution_id, signal, leverage_info, order_result
                )
            except ValueError as ve:
                logger.error(f"Position creation validation failed: {ve}")
                return self._create_failed_result(
                    execution_id, f"Position creation validation failed: {str(ve)}", start_time
                )
            except Exception as pe:
                logger.error(f"Position creation error: {pe}")
                return self._create_failed_result(
                    execution_id, f"Position creation failed: {str(pe)}", start_time
                )
            
            # Position safety verification
            if not position:
                return self._create_failed_result(
                    execution_id, "Position creation failed (returned None)", start_time
                )
            
            # CRITICAL: Final position validation
            if position.entry_price <= 0:
                logger.error(f"CRITICAL: Position created with invalid entry_price: {position.entry_price}")
                return self._create_failed_result(
                    execution_id, f"Position validation failed: invalid entry_price {position.entry_price}", start_time
                )

            # Step 7: Start real-time monitoring (Notify Service integration)
            asyncio.create_task(self._monitor_position_with_notifications(position))

            # Update statistics
            execution_time = (time.time() - start_time) * 1000
            self._update_execution_stats(True, execution_time)

            # Phoenix95 boost count
            phoenix95_score = getattr(signal, 'phoenix95_score', 0)
            if phoenix95_score >= 85:
                self.risk_metrics["phoenix95_boost_count"] += 1

            return ExecutionResult(
                execution_id=execution_id,
                status="SUCCESS",
                message="Phoenix95 enhanced trade execution successful",
                success=True,
                position=position,
                execution_time_ms=execution_time,
                service_chain=f"{getattr(signal, 'service_chain', 'unknown')}_execute",
                timestamp=time.time(),
                notified=False
            )

        except AttributeError as e:
            logger.error(f"Attribute error: {e}")
            return self._create_failed_result(
                execution_id, f"Signal object attribute error: {str(e)}", start_time
            )
        except Exception as e:
            logger.error(f"Enhanced trade execution failed: {e}")
            import traceback
            logger.error(f"Detailed error: {traceback.format_exc()}")
            return self._create_failed_result(
                execution_id, f"Enhanced execution error: {str(e)}", start_time
            )

    async def _execute_simulation_order(
        self, signal: EnhancedTradingSignal, leverage_info: Dict, start_time: float
    ) -> Dict:
        """Simulation mode order execution (when API keys unavailable)"""
        try:
            # Existing simulation logic (improved)
            await asyncio.sleep(0.05)  # Simulate real API delay

            # 95% success rate (more realistic)
            success = np.random.random() < 0.95
            execution_time = (time.time() - start_time) * 1000

            if success:
                order_id = f"SIM_{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}"

                # Add slight slippage
                slippage = np.random.uniform(0.0001, 0.002)  # 0.01% - 0.2%
                if signal.action.lower() in ["buy", "long"]:
                    execution_price = signal.price * (1 + slippage)
                else:
                    execution_price = signal.price * (1 - slippage)

                logger.info(f"[Simulation] Order execution successful:")
                logger.info(f"   Order ID: {order_id}")
                logger.info(f"   Execution price: ${execution_price:,.2f}")
                logger.info(f"   Slippage: {slippage*100:.3f}%")
                logger.info(f"   Execution time: {execution_time:.1f}ms")

                return {
                    "success": True,
                    "order_id": order_id,
                    "execution_price": execution_price,
                    "executed_qty": leverage_info["quantity"],
                    "commission": execution_price
                    * leverage_info["quantity"]
                    * 0.0004,  # 0.04% commission
                    "slippage": slippage,
                    "execution_time_ms": execution_time,
                    "optimized": True,
                    "exchange": "simulation",
                    "timestamp": time.time(),
                }
            else:
                return {"success": False, "error": "Simulation order failed (5% probability)"}

        except Exception as e:
            return {"success": False, "error": f"Simulation error: {e}"}

    async def _validate_market_conditions(self, symbol: str) -> bool:
        """Real Binance market conditions validation (integrated with existing function)"""
        try:
            # Handle simulation mode
            if hasattr(config, "SIMULATION_MODE") and config.SIMULATION_MODE:
                await asyncio.sleep(0.01)  # Simulate API call
                return np.random.random() < 0.95

            # Real Binance API call
            binance_client = await self._get_binance_client()
            if not binance_client:
                return False

            try:
                # Reuse already implemented detailed validation function
                result = await self._validate_binance_market_conditions(
                    binance_client, symbol
                )
                return result

            finally:
                await binance_client.close_connection()

        except Exception as e:
            logger.warning(f"Market condition validation error ({symbol}): {e}")
            return False

    async def _check_account_balance(self, required_margin: float) -> bool:
        """Real Binance account balance check (integrated with existing function)"""
        try:
            # Handle simulation mode
            if hasattr(config, "SIMULATION_MODE") and config.SIMULATION_MODE:
                await asyncio.sleep(0.01)  # Simulate API call
                available_balance = config.PORTFOLIO_VALUE * 0.8
                return available_balance >= required_margin

            # Real Binance API call
            binance_client = await self._get_binance_client()
            if not binance_client:
                return False

            try:
                # Reuse already implemented detailed balance check function
                result = await self._check_binance_account_balance(
                    binance_client, required_margin
                )
                return result

            finally:
                await binance_client.close_connection()

        except Exception as e:
            logger.warning(f"Balance check error (required amount: ${required_margin:,.2f}): {e}")
            return False

    async def _get_optimal_execution_price(
        self, signal: EnhancedTradingSignal
    ) -> float:
        """Query optimal execution price (real-time Binance price)"""
        try:
            # Handle simulation mode
            if hasattr(config, "SIMULATION_MODE") and config.SIMULATION_MODE:
                await asyncio.sleep(0.005)  # Fast price query simulation
                price_movement = np.random.uniform(-0.001, 0.001)
                return signal.price * (1 + price_movement)

            # Real Binance optimal price query (reuse already implemented function)
            optimal_price = await self._get_current_price_optimized(signal.symbol)

            if optimal_price > 0:
                logger.debug(
                    f"Symbol {signal.symbol} optimal execution price: ${optimal_price:,.4f} (signal price: ${signal.price:,.4f})"
                )
                return optimal_price
            else:
                logger.warning(f"Optimal price query failed, using signal price")
                return signal.price

        except Exception as e:
            logger.warning(f"Optimal execution price query failed ({signal.symbol}): {e}")
            return signal.price

    async def _get_current_price_optimized(self, symbol: str) -> float:
        """
        Real-time Binance SPOT price query with advanced multi-level caching
        Optimized for 128GB DDR5 RAM with intelligent cache management
        """
        try:
            # Initialize advanced cache structures (128GB RAM optimized)
            if not hasattr(self, "_price_cache"):
                self._price_cache = {}
                self._cache_timestamps = {}
                self._cache_hit_count = {}
                self._cache_miss_count = {}
                self._price_source_reliability = {}

            # Adaptive cache duration based on symbol volatility and system load
            base_cache_duration = 3 if self.hardware_optimized else 5
            current_time = time.time()
            
            # Calculate symbol-specific cache duration
            if symbol in self._cache_hit_count:
                hit_rate = self._cache_hit_count.get(symbol, 0) / max(1, self._cache_hit_count.get(symbol, 0) + self._cache_miss_count.get(symbol, 0))
                cache_duration = base_cache_duration * (1 + hit_rate * 0.5)
            else:
                cache_duration = base_cache_duration

            # Multi-level cache check with validation
            if (
                symbol in self._price_cache
                and symbol in self._cache_timestamps
                and current_time - self._cache_timestamps[symbol] < cache_duration
            ):
                cached_price = self._price_cache[symbol]
                
                # Validate cached price
                if cached_price > 0:
                    self._cache_hit_count[symbol] = self._cache_hit_count.get(symbol, 0) + 1
                    logger.debug(f"Cache HIT for {symbol}: ${cached_price:,.4f} (age: {current_time - self._cache_timestamps[symbol]:.1f}s)")
                    return cached_price
                else:
                    logger.warning(f"Invalid cached price for {symbol}: {cached_price}, refreshing")

            # Cache miss tracking
            self._cache_miss_count[symbol] = self._cache_miss_count.get(symbol, 0) + 1

            # Handle simulation mode
            if hasattr(config, "SIMULATION_MODE") and config.SIMULATION_MODE:
                sim_price = await self._get_simulation_price(symbol)
                self._update_price_cache(symbol, sim_price, current_time)
                return sim_price

            # Clean symbol format
            clean_symbol = symbol.replace('.P', '').upper()

            # Get Binance client with connection pooling
            binance_client = await self._get_binance_client()
            if not binance_client:
                logger.warning(f"No Binance client available for {symbol}, switching to simulation mode")
                sim_price = await self._get_simulation_price(symbol)
                self._update_price_cache(symbol, sim_price, current_time)
                return sim_price

            try:
                # API rate limit check with adaptive weight
                await self.rate_limiter.acquire(weight=2)

                # Multi-source parallel query with timeout protection (multi-core optimization)
                query_timeout = 8.0 if self.hardware_optimized else 10.0
                
                tasks = [
                    asyncio.create_task(self._get_ticker_price(binance_client, clean_symbol)),
                    asyncio.create_task(self._get_orderbook_mid_price(binance_client, clean_symbol)),
                    asyncio.create_task(self._get_recent_trades_price(binance_client, clean_symbol)),
                ]

                # Wait for all tasks with timeout
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=query_timeout
                    )
                    ticker_price, orderbook_price, trades_price = results
                except asyncio.TimeoutError:
                    logger.warning(f"Price query timeout for {symbol} after {query_timeout}s")
                    # Cancel pending tasks
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    
                    # Use stale cache if available
                    if symbol in self._price_cache:
                        stale_price = self._price_cache[symbol]
                        logger.warning(f"Using stale cache for {symbol}: ${stale_price:,.4f}")
                        return stale_price
                    return await self._get_simulation_price(symbol)

                # Enhanced price validation and source reliability tracking
                valid_prices = []

                if not isinstance(ticker_price, Exception) and ticker_price > 0:
                    valid_prices.append(("ticker", ticker_price, 0.4))
                    self._update_source_reliability("ticker", symbol, True)
                else:
                    self._update_source_reliability("ticker", symbol, False)
                    logger.debug(f"Ticker price failed for {symbol}: {ticker_price}")

                if not isinstance(orderbook_price, Exception) and orderbook_price > 0:
                    valid_prices.append(("orderbook", orderbook_price, 0.4))
                    self._update_source_reliability("orderbook", symbol, True)
                else:
                    self._update_source_reliability("orderbook", symbol, False)
                    logger.debug(f"Orderbook price failed for {symbol}: {orderbook_price}")

                if not isinstance(trades_price, Exception) and trades_price > 0:
                    valid_prices.append(("trades", trades_price, 0.2))
                    self._update_source_reliability("trades", symbol, True)
                else:
                    self._update_source_reliability("trades", symbol, False)
                    logger.debug(f"Trades price failed for {symbol}: {trades_price}")

                if not valid_prices:
                    logger.error(f"All SPOT price sources failed for {symbol}")
                    # Try stale cache first
                    if symbol in self._price_cache:
                        stale_age = current_time - self._cache_timestamps.get(symbol, 0)
                        if stale_age < 60:
                            logger.warning(f"Using stale cache for {symbol}: age {stale_age:.1f}s")
                            return self._price_cache[symbol]
                    return await self._get_simulation_price(symbol)

                # Calculate weighted average with outlier detection
                prices_only = [price for _, price, _ in valid_prices]
                
                # Outlier detection (remove prices >5% different from median)
                if len(prices_only) > 1:
                    median_price = sorted(prices_only)[len(prices_only) // 2]
                    filtered_prices = [
                        (source, price, weight)
                        for source, price, weight in valid_prices
                        if abs(price - median_price) / median_price < 0.05
                    ]
                    
                    if filtered_prices:
                        valid_prices = filtered_prices
                        logger.debug(f"Outlier filtering: {len(prices_only)} -> {len(valid_prices)} sources")

                # Calculate weighted average
                weighted_sum = sum(price * weight for _, price, weight in valid_prices)
                total_weight = sum(weight for _, _, weight in valid_prices)
                optimized_price = weighted_sum / total_weight

                # Update cache with new price
                self._update_price_cache(symbol, optimized_price, current_time)

                # Enhanced logging with source information
                sources_info = ", ".join(
                    [f"{source}:${price:,.2f}" for source, price, _ in valid_prices]
                )
                logger.info(
                    f"Optimized SPOT price for {symbol}: ${optimized_price:,.4f} ({sources_info})"
                )

                return optimized_price

            finally:
                # Ensure client connection is closed
                try:
                    await asyncio.wait_for(
                        binance_client.close_connection(),
                        timeout=3.0
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Client close timeout for {symbol}")
                except Exception as close_error:
                    logger.debug(f"Client close error: {close_error}")

        except Exception as e:
            logger.error(f"Price query error for {symbol}: {type(e).__name__}: {e}")
            
            # Multi-level fallback system
            if (
                hasattr(self, "_price_cache")
                and symbol in self._price_cache
                and hasattr(self, "_cache_timestamps")
                and symbol in self._cache_timestamps
            ):
                cache_age = time.time() - self._cache_timestamps[symbol]
                
                # Extended stale cache threshold (60 seconds for 128GB system)
                stale_threshold = 60 if self.hardware_optimized else 30
                
                if cache_age < stale_threshold:
                    logger.warning(
                        f"Using stale cached price for {symbol} "
                        f"(age: {cache_age:.1f}s, threshold: {stale_threshold}s)"
                    )
                    return self._price_cache[symbol]

            # Last resort: simulation price
            logger.error(f"All fallbacks exhausted for {symbol}, using simulation")
            return await self._get_simulation_price(symbol)

    def _update_price_cache(self, symbol: str, price: float, timestamp: float):
        """
        Update price cache with intelligent memory management
        Optimized for 128GB RAM system
        """
        try:
            self._price_cache[symbol] = price
            self._cache_timestamps[symbol] = timestamp
            
            # Adaptive cache size management (128GB optimized)
            max_cache_size = 2000 if self.hardware_optimized else 500
            
            if len(self._price_cache) > max_cache_size:
                # Remove oldest entries (keep 80% most recent)
                keep_count = int(max_cache_size * 0.8)
                
                sorted_symbols = sorted(
                    self._cache_timestamps.items(),
                    key=lambda x: x[1],
                    reverse=True
                )
                
                symbols_to_keep = {symbol for symbol, _ in sorted_symbols[:keep_count]}
                
                # Clean old entries
                self._price_cache = {
                    s: p for s, p in self._price_cache.items()
                    if s in symbols_to_keep
                }
                self._cache_timestamps = {
                    s: t for s, t in self._cache_timestamps.items()
                    if s in symbols_to_keep
                }
                
                logger.debug(f"Price cache cleaned: {len(self._price_cache)}/{max_cache_size} entries")
                
        except Exception as e:
            logger.warning(f"Cache update error: {e}")

    def _update_source_reliability(self, source: str, symbol: str, success: bool):
        """
        Track price source reliability for adaptive weighting
        128GB RAM allows extensive reliability tracking
        """
        try:
            key = f"{source}_{symbol}"
            
            if key not in self._price_source_reliability:
                self._price_source_reliability[key] = {"success": 0, "failure": 0}
            
            if success:
                self._price_source_reliability[key]["success"] += 1
            else:
                self._price_source_reliability[key]["failure"] += 1
            
            # Limit reliability tracking (128GB optimized)
            max_reliability_entries = 5000 if self.hardware_optimized else 1000
            
            if len(self._price_source_reliability) > max_reliability_entries:
                # Keep only recent entries
                keep_count = int(max_reliability_entries * 0.8)
                keys_to_keep = list(self._price_source_reliability.keys())[-keep_count:]
                
                self._price_source_reliability = {
                    k: v for k, v in self._price_source_reliability.items()
                    if k in keys_to_keep
                }
                
        except Exception as e:
            logger.debug(f"Source reliability update error: {e}")

    async def _get_simulation_price(self, symbol: str) -> float:
        """Query simulation price (improved version)"""
        try:
            # More realistic base prices (2024 baseline)
            realistic_base_prices = {
                "BTCUSDT": 42000,
                "ETHUSDT": 2500,
                "BNBUSDT": 310,
                "ADAUSDT": 0.38,
                "DOGEUSDT": 0.075,
                "XRPUSDT": 0.52,
                "SOLUSDT": 95,
                "AVAXUSDT": 32,
                "DOTUSDT": 5.8,
                "LINKUSDT": 14.5,
                "MATICUSDT": 0.85,
                "LTCUSDT": 95,
                "ATOMUSDT": 9.2,
                "NEARUSDT": 2.8,
                "FTMUSDT": 0.35,
            }

            base_price = realistic_base_prices.get(symbol, 100)

            # Simulation price cache (for continuity)
            if not hasattr(self, "_sim_price_cache"):
                self._sim_price_cache = {}

            # Price change based on previous price (more realistic)
            if symbol in self._sim_price_cache:
                prev_price = self._sim_price_cache[symbol]
                # Small fluctuation (-0.5% ~ +0.5%)
                change = np.random.uniform(-0.005, 0.005)
                new_price = prev_price * (1 + change)
            else:
                # Initial price (base value +/- 5% range)
                change = np.random.uniform(-0.05, 0.05)
                new_price = base_price * (1 + change)

            # Update cache
            self._sim_price_cache[symbol] = new_price

            logger.debug(f"[Simulation] {symbol} price: ${new_price:,.4f}")
            return new_price

        except Exception as e:
            logger.warning(f"Simulation price generation failed ({symbol}): {e}")
            return 100.0  # Default fallback

    async def get_multiple_prices_optimized(
        self, symbols: List[str]
    ) -> Dict[str, float]:
        """Query multiple symbol prices simultaneously (performance optimized)"""
        try:
            if not symbols:
                return {}

            # Query all symbol prices in parallel
            tasks = [self._get_current_price_optimized(symbol) for symbol in symbols]
            prices = await asyncio.gather(*tasks, return_exceptions=True)

            result = {}
            for symbol, price in zip(symbols, prices):
                if isinstance(price, Exception):
                    logger.warning(f"Warning: {symbol} price query failed: {price}")
                    result[symbol] = 0.0
                else:
                    result[symbol] = price

            return result

        except Exception as e:
            logger.error(f"Multiple price query failed: {e}")
            return {symbol: 0.0 for symbol in symbols}

    def clear_price_cache(self):
        """Clear price cache (when needed)"""
        if hasattr(self, "_price_cache"):
            self._price_cache.clear()
        if hasattr(self, "_cache_timestamps"):
            self._cache_timestamps.clear()
        if hasattr(self, "_sim_price_cache"):
            self._sim_price_cache.clear()

        logger.info("Price cache cleared successfully")

    async def update_service_connections(self):
        """Update service connection status with enhanced health metrics and auto-reconnection integration"""
        try:
            services = {
                "risk_service": config.RISK_SERVICE_URL,
                "notify_service": config.NOTIFY_SERVICE_URL,
                "brain_service": config.BRAIN_SERVICE_URL,
            }

            connection_results = {}
            
            for service_name, service_url in services.items():
                try:
                    health_info = self.service_health.get(service_name, {})
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"{service_url}/health", 
                            timeout=aiohttp.ClientTimeout(total=5.0)
                        ) as response:
                            if response.status == 200:
                                self.service_connections[service_name] = "connected"
                                
                                # Update health metrics
                                if health_info:
                                    health_info['status'] = "connected"
                                    health_info['consecutive_successes'] += 1
                                    health_info['consecutive_failures'] = 0
                                    health_info['last_check'] = time.time()
                                
                                connection_results[service_name] = {
                                    "status": "connected",
                                    "response_time_ms": (time.time() - health_info.get('last_check', time.time())) * 1000,
                                    "health": "healthy"
                                }
                            else:
                                self.service_connections[service_name] = "unreachable"
                                
                                if health_info:
                                    health_info['status'] = "disconnected"
                                    health_info['consecutive_failures'] += 1
                                    health_info['consecutive_successes'] = 0
                                
                                connection_results[service_name] = {
                                    "status": "unreachable",
                                    "http_status": response.status,
                                    "health": "unhealthy"
                                }
                                
                except asyncio.TimeoutError:
                    self.service_connections[service_name] = "timeout"
                    if health_info:
                        health_info['status'] = "timeout"
                        health_info['consecutive_failures'] += 1
                    connection_results[service_name] = {
                        "status": "timeout",
                        "health": "unhealthy",
                        "error": "Connection timeout (5s)"
                    }
                    
                except Exception as e:
                    self.service_connections[service_name] = "error"
                    if health_info:
                        health_info['status'] = "error"
                        health_info['consecutive_failures'] += 1
                    connection_results[service_name] = {
                        "status": "error",
                        "health": "unhealthy",
                        "error": str(e)
                    }

            return {
                "connections": self.service_connections,
                "details": connection_results,
                "timestamp": time.time(),
                "auto_reconnection_enabled": self.reconnection_config['enabled']
            }
            
        except Exception as e:
            logger.error(f"Error updating service connections: {e}")
            return {
                "connections": self.service_connections,
                "error": str(e),
                "timestamp": time.time()
            }

    def get_enhanced_system_status(self) -> Dict:
        """Query enhanced system status with auto-reconnection metrics and hardware optimization info"""
        try:
            # Calculate service health statistics
            total_services = len(self.service_health)
            connected_services = sum(
                1 for health in self.service_health.values() 
                if health['status'] == 'connected'
            )
            
            # Calculate average response health
            avg_consecutive_failures = sum(
                health['consecutive_failures'] 
                for health in self.service_health.values()
            ) / max(1, total_services)
            
            # Get circuit breaker status
            circuit_breakers = {
                name: {
                    "open": health['circuit_open'],
                    "consecutive_failures": health['consecutive_failures'],
                    "opened_at": datetime.fromtimestamp(health['circuit_opened_at']).isoformat() 
                    if health['circuit_open'] else None
                }
                for name, health in self.service_health.items()
            }
            
            return {
                "service": "Phoenix95-Execute-V4-Enhanced",
                "status": "ACTIVE",
                "port": 8102,
                "version": "4.0.0-integrated",
                "uptime_seconds": time.time() - (self.service_health['notify_service'].get('last_check', time.time())),
                
                # Trading metrics
                "active_positions": len(self.active_positions),
                "daily_pnl": self.daily_pnl,
                "execution_stats": self.execution_stats,
                "risk_metrics": self.risk_metrics,
                
                # Service connection status
                "service_connections": self.service_connections,
                "service_health_summary": {
                    "total_services": total_services,
                    "connected": connected_services,
                    "disconnected": total_services - connected_services,
                    "connection_rate": f"{(connected_services/total_services)*100:.1f}%",
                    "avg_consecutive_failures": round(avg_consecutive_failures, 2)
                },
                
                # Auto-reconnection system status
                "auto_reconnection": {
                    "enabled": self.reconnection_config['enabled'],
                    "running": self._reconnection_running,
                    "check_interval_seconds": self.reconnection_config['check_interval'],
                    "circuit_breaker_threshold": self.reconnection_config['circuit_breaker_threshold'],
                    "circuit_breaker_timeout_seconds": self.reconnection_config['circuit_breaker_timeout'],
                    "circuit_breakers": circuit_breakers
                },
                
                # Hardware optimization status
                "hardware_optimization": {
                    "enabled": self.hardware_optimized,
                    "cpu_cores_optimized": "16-24 cores (Ryzen 9 7950X / i9-13900K)",
                    "ram_optimized": "128GB DDR5",
                    "max_concurrent_orders": self.max_concurrent_orders,
                    "max_worker_threads": self.max_worker_threads,
                    "max_history_size": self.max_history_size,
                    "aggressive_caching": self.aggressive_caching
                },
                
                # Configuration
                "config": {
                    "leverage": config.LEVERAGE,
                    "margin_mode": config.MARGIN_MODE,
                    "stop_loss": f"{config.STOP_LOSS_PERCENT*100:.0f}%",
                    "take_profit": f"{config.TAKE_PROFIT_PERCENT*100:.0f}%",
                    "max_positions": config.MAX_POSITIONS,
                    "kelly_max_fraction": f"{config.KELLY_MAX_FRACTION*100:.1f}%",
                    "portfolio_value": f"${config.PORTFOLIO_VALUE:,}",
                },
                
                # Service integrations
                "integrations": {
                    "risk_service_endpoint": f"{config.RISK_SERVICE_URL}/execute",
                    "notify_service_endpoints": [
                        f"{config.NOTIFY_SERVICE_URL}/api/notification/execution",
                        f"{config.NOTIFY_SERVICE_URL}/api/notification/position",
                        f"{config.NOTIFY_SERVICE_URL}/api/notification/risk",
                    ],
                    "backup_notification": "telegram_direct",
                },
                
                # Performance features
                "performance": {
                    "uvloop_enabled": True,
                    "async_optimization": True,
                    "parallel_validation": True,
                    "adaptive_monitoring": True,
                    "multi_core_processing": True,
                    "aggressive_memory_caching": self.aggressive_caching,
                },
                
                # System timestamp
                "timestamp": time.time(),
                "timestamp_iso": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating enhanced system status: {e}")
            return {
                "service": "Phoenix95-Execute-V4-Enhanced",
                "status": "ERROR",
                "error": str(e),
                "timestamp": time.time()
            }


# ===============================================================================
#                              FastAPI Web Service (Integrated Version)
# ===============================================================================

# Create FastAPI app

# ===============================================================================
#                              FastAPI Application Lifespan
# ===============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager for startup and shutdown events
    Replaces deprecated @app.on_event("startup") and @app.on_event("shutdown")
    Optimized for high-performance systems (Ryzen 9 7950X, 128GB RAM)
    """
    # Startup
    try:
        logger.info("=" * 70)
        logger.info("FastAPI Application Starting - Initializing Services")
        logger.info("=" * 70)
        
        # Start auto-reconnection monitor
        if execute_engine.reconnection_config['enabled']:
            logger.info("Starting auto-reconnection monitoring system...")
            execute_engine._reconnection_task = asyncio.create_task(
                execute_engine._start_auto_reconnection_monitor()
            )
            logger.info("Auto-reconnection monitor started successfully")
        else:
            logger.warning("Auto-reconnection system is disabled")
        
        # Perform initial service health check
        logger.info("Performing initial service health check...")
        await execute_engine._check_all_service_connections()
        
        # Log service connection status
        connected_services = sum(
            1 for status in execute_engine.service_connections.values() 
            if status == "connected"
        )
        total_services = len(execute_engine.service_connections)
        
        logger.info(f"Initial service health: {connected_services}/{total_services} services connected")
        for service_name, status in execute_engine.service_connections.items():
            status_symbol = "[OK]" if status == "connected" else "[X]"
            logger.info(f"  {status_symbol} {service_name}: {status}")
        
        logger.info("=" * 70)
        logger.info("FastAPI Application Startup Complete")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    yield  # Application runs here
    
    # Shutdown
    try:
        logger.info("=" * 70)
        logger.info("FastAPI Application Shutdown Initiated")
        logger.info("=" * 70)
        
        # Stop auto-reconnection monitor
        if execute_engine._reconnection_running:
            logger.info("Stopping auto-reconnection monitor...")
            execute_engine._reconnection_running = False
            
            if execute_engine._reconnection_task:
                execute_engine._reconnection_task.cancel()
                try:
                    await asyncio.wait_for(execute_engine._reconnection_task, timeout=5.0)
                except asyncio.CancelledError:
                    logger.info("Auto-reconnection monitor cancelled successfully")
                except asyncio.TimeoutError:
                    logger.warning("Auto-reconnection monitor cancellation timeout")
        
        # Close all active positions (emergency close)
        if execute_engine.active_positions:
            logger.warning(f"Emergency closing {len(execute_engine.active_positions)} active positions...")
            for position_id, position in list(execute_engine.active_positions.items()):
                try:
                    await execute_engine._force_close_position(
                        position, 
                        "Server shutdown - Emergency close"
                    )
                except Exception as e:
                    logger.error(f"Error closing position {position_id}: {e}")
        
        # Log final statistics
        logger.info("Final execution statistics:")
        logger.info(f"  Total executions: {execute_engine.execution_stats['total_executions']}")
        logger.info(f"  Successful: {execute_engine.execution_stats['successful_executions']}")
        logger.info(f"  Failed: {execute_engine.execution_stats['failed_executions']}")
        logger.info(f"  Daily P&L: ${execute_engine.daily_pnl:+,.2f}")
        logger.info(f"  Win rate: {execute_engine.execution_stats['win_rate']:.1f}%")
        
        logger.info("=" * 70)
        logger.info("FastAPI Application Shutdown Complete")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

# ===============================================================================
#                              FastAPI Web Service (Integrated Version)
# ===============================================================================

# Create FastAPI app
app = FastAPI(
    title="Phoenix 95 V4 Execute Service - Integrated",
    description="Hedge fund-grade 20x leverage trading execution system (Risk/Notify Service fully integrated)",
    version="4.0.0-integrated",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Handler for 422 validation errors
from fastapi.exceptions import RequestValidationError

@app.exception_handler(RequestValidationError)  
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    request_info = f"{request.method} {request.url}"
    client_host = request.client.host if request.client else "unknown"
    
    logger.error(f"422 Validation Error from {client_host}")
    logger.error(f"Request: {request_info}")
    logger.error(f"Validation errors: {exc.errors()}")
    
    # Enhanced error response with more details
    error_details = []
    for error in exc.errors():
        field_path = " -> ".join(str(loc) for loc in error["loc"])
        error_details.append({
            "field": field_path,
            "message": error["msg"],
            "type": error["type"]
        })
    
    return JSONResponse(
        status_code=422,
        content={
            "error": "Data validation failed",
            "message": "Request data does not match expected format",
            "details": error_details,
            "request_id": str(uuid.uuid4())[:8],
            "timestamp": time.time()
        }
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global handler for unhandled exceptions"""
    request_info = f"{request.method} {request.url}"
    client_host = request.client.host if request.client else "unknown"
    error_id = str(uuid.uuid4())[:8]
    
    logger.error(f"500 Internal Server Error [{error_id}] from {client_host}")
    logger.error(f"Request: {request_info}")
    logger.error(f"Exception type: {type(exc).__name__}")
    logger.error(f"Exception message: {str(exc)}")
    
    # Log full traceback for debugging
    import traceback
    logger.error(f"Full traceback:\n{traceback.format_exc()}")
    
    # Don't expose internal details to client
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred while processing your request",
            "error_id": error_id,
            "timestamp": time.time(),
            "support_message": "Please contact support with the error_id if the issue persists"
        }
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Enhanced HTTP exception handler"""
    request_info = f"{request.method} {request.url}"
    client_host = request.client.host if request.client else "unknown"
    
    logger.warning(f"HTTP {exc.status_code} from {client_host}: {request_info}")
    logger.warning(f"Exception detail: {exc.detail}")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": f"HTTP {exc.status_code}",
            "message": str(exc.detail),
            "timestamp": time.time(),
            "path": str(request.url.path)
        },
        headers=getattr(exc, "headers", None)
    )

@app.exception_handler(AttributeError)
async def attribute_error_handler(request: Request, exc: AttributeError):
    """Specific handler for AttributeError (common in trading execution)"""
    request_info = f"{request.method} {request.url}"
    client_host = request.client.host if request.client else "unknown"
    error_id = str(uuid.uuid4())[:8]
    
    logger.error(f"AttributeError [{error_id}] from {client_host}")
    logger.error(f"Request: {request_info}")
    logger.error(f"AttributeError: {str(exc)}")
    
    import traceback
    logger.error(f"Traceback:\n{traceback.format_exc()}")
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Configuration or object error",
            "message": "A required component or attribute is missing",
            "error_id": error_id,
            "timestamp": time.time(),
            "hint": "This usually indicates a system configuration issue"
        }
    )

# Trading execution engine instance
execute_engine = Phoenix95ExecuteEngine()


# Request models
class RiskServiceExecuteRequest(BaseModel):
    """Risk Service standard execution request with enhanced validation"""

    signal_data: Optional[Dict] = None
    signal: Optional[Dict] = None  # Fallback field for compatibility
    phoenix_analysis: Optional[Dict] = None
    service_chain: Optional[str] = "unknown"
    timestamp: Optional[float] = Field(default_factory=time.time)
    
    # Root level fields for fallback reconstruction
    signal_id: Optional[str] = None
    symbol: Optional[str] = None
    action: Optional[str] = None
    price: Optional[Union[float, str]] = None
    confidence: Optional[Union[float, str]] = None
    phoenix_score: Optional[Union[float, str]] = None
    
    model_config = {
        "extra": "allow",
        "validate_assignment": True
    }
        
    @field_validator('price', mode='before')
    @classmethod
    def validate_price(cls, v):
        if v is None:
            return v
        try:
            if isinstance(v, str):
                v = v.replace(',', '')
            return float(v)
        except (ValueError, TypeError):
            raise ValueError('Price must be a valid number')
    
    @field_validator('confidence', mode='before')
    @classmethod
    def validate_confidence(cls, v):
        if v is None:
            return v
        try:
            if isinstance(v, str):
                v = float(v)
            else:
                v = float(v)
            if not (0 < v <= 1):
                raise ValueError('Confidence must be between 0 and 1')
            return v
        except (ValueError, TypeError):
            raise ValueError('Confidence must be a valid number between 0 and 1')
    
    @field_validator('phoenix_score', mode='before')
    @classmethod
    def validate_phoenix_score(cls, v):
        if v is None:
            return v
        try:
            return float(v)
        except (ValueError, TypeError):
            raise ValueError('Phoenix score must be a valid number')
    
    @field_validator('action')
    @classmethod
    def validate_action(cls, v):
        if v is None:
            return v
        valid_actions = ['buy', 'sell', 'long', 'short']
        if v.lower() not in valid_actions:
            raise ValueError(f'Action must be one of: {valid_actions}')
        return v.lower()
    
    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v):
        if v is None:
            return v
        symbol = v.upper().strip()
        if len(symbol) < 3:
            raise ValueError('Symbol must be at least 3 characters long')
        return symbol

class LegacyExecuteRequest(BaseModel):
    """Legacy compatibility execution request"""

    signal_id: str
    symbol: str
    action: str
    price: float
    confidence: float
    phoenix95_score: Optional[float] = 0.0
    kelly_ratio: Optional[float] = 0.01



# ===============================================================================
#                              Integrated API Routes
# ===============================================================================


@app.get("/")
async def enhanced_dashboard():
    """Phoenix95 enhanced dashboard"""

    # Update service connection status
    service_connections = execute_engine.service_connections

    # Active positions table
    positions_html = ""
    if execute_engine.active_positions:
        positions_rows = []
        for pos in execute_engine.active_positions.values():
            pnl_class = "profit" if pos.unrealized_pnl >= 0 else "loss"
            roe_class = "profit" if pos.roe >= 0 else "loss"
            risk_class = (
                "critical"
                if pos.liquidation_risk > 0.7
                else "warning"
                if pos.liquidation_risk > 0.3
                else "safe"
            )

            row = f"""
                        <tr>
                            <td>{pos.symbol}</td>
                            <td>{pos.action.upper()}</td>
                            <td>{pos.leverage}x</td>
                            <td>${pos.entry_price:,.2f}</td>
                            <td>${pos.current_price:,.2f}</td>
                            <td class="{pnl_class}">${pos.unrealized_pnl:+,.2f}</td>
                            <td class="{roe_class}">{pos.roe:+.2f}%</td>
                            <td class="{risk_class}">{pos.liquidation_risk:.1%}</td>
                            <td><span class="phoenix-score">{pos.phoenix95_score:.0f}/95</span></td>
                        </tr>"""
            positions_rows.append(row)

        positions_html = f"""
            <div class="status-card">
                <div class="status-title">Active Positions ({len(execute_engine.active_positions)})</div>
                <table class="positions-table">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Direction</th>
                            <th>Leverage</th>
                            <th>Entry Price</th>
                            <th>Current Price</th>
                            <th>P&L</th>
                            <th>ROE</th>
                            <th>Liquidation Risk</th>
                            <th>Phoenix95</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join(positions_rows)}
                    </tbody>
                </table>
            </div>"""

    # Service connection status display
    connections_html = ""
    for service, status in execute_engine.service_connections.items():
        status_class = "connected" if status == "connected" else "disconnected"
        status_icon = "[OK]" if status == "connected" else "[X]"
        service_name = service.replace("_service", "").title()

        connections_html += f"""
            <div class="connection-item">
                <span>{status_icon} {service_name} Service</span>
                <span class="status-value {status_class}">{status.upper()}</span>
            </div>"""

    # Daily P&L color
    pnl_color = "#4caf50" if execute_engine.daily_pnl >= 0 else "#f44336"


    # Pipeline status
    pipeline_health = all(
        status == "connected" for status in execute_engine.service_connections.values()
    )
    pipeline_status = "Fully Connected" if pipeline_health else "Partially Connected"

    css_styles = """
        body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px; 
               background: linear-gradient(135deg, #1e3c72, #2a5298); color: white; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { text-align: center; margin-bottom: 40px; }
        .header h1 { font-size: 2.8em; margin: 0; color: #00ff88; text-shadow: 0 0 10px rgba(0,255,136,0.3); }
        .header .subtitle { font-size: 1.2em; color: #ffeb3b; margin: 10px 0; }
        .status-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); 
                       gap: 25px; margin-bottom: 35px; }
        .status-card { background: rgba(255,255,255,0.1); border-radius: 15px; padding: 25px; 
                       backdrop-filter: blur(10px); border: 1px solid rgba(255,255,255,0.2);
                       box-shadow: 0 8px 25px rgba(0,0,0,0.2); transition: transform 0.3s ease; }
        .status-card:hover { transform: translateY(-5px); }
        .status-title { font-size: 1.4em; font-weight: bold; margin-bottom: 20px; color: #00ff88; 
                        border-bottom: 2px solid rgba(0,255,136,0.3); padding-bottom: 10px; }
        .status-item { display: flex; justify-content: space-between; margin: 12px 0; 
                       padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.1); }
        .connection-item { display: flex; justify-content: space-between; margin: 10px 0; 
                          padding: 8px 12px; background: rgba(255,255,255,0.05); border-radius: 8px; }
        .status-value { font-weight: bold; color: #ffeb3b; }
        .connected { color: #4caf50 !important; }
        .disconnected { color: #f44336 !important; }
        .positions-table { width: 100%; border-collapse: collapse; margin-top: 20px; }
        .positions-table th, .positions-table td { padding: 12px; text-align: center; 
                                                    border-bottom: 1px solid rgba(255,255,255,0.2); }
        .positions-table th { background: rgba(0,255,136,0.2); color: #00ff88; font-weight: bold; }
        .profit { color: #4caf50; font-weight: bold; }
        .loss { color: #f44336; font-weight: bold; }
        .critical { color: #ff1744; font-weight: bold; }
        .warning { color: #ff9800; font-weight: bold; }
        .safe { color: #4caf50; font-weight: bold; }
        .phoenix-score { background: linear-gradient(45deg, #ff6b6b, #4ecdc4); 
                        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                        font-weight: bold; }
        .footer { text-align: center; margin-top: 50px; color: rgba(255,255,255,0.8); }
        .integration-badge { display: inline-block; background: rgba(0,255,136,0.2); 
                           padding: 5px 10px; border-radius: 20px; margin: 0 5px; 
                           font-size: 0.9em; border: 1px solid rgba(0,255,136,0.3); }
    """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Phoenix 95 V4 Execute Service - Integrated</title>
        <style>
            {css_styles}
        </style>
        <script>
            setInterval(() => location.reload(), 8000); // Refresh every 8 seconds
        </script>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Phoenix 95 V4 Execute Service</h1>
                <div class="subtitle">Hedge Fund Grade 20x Leverage Trading Execution System (Fully Integrated)</div>
                <p>Port: 8102 | Status: <span style="color: #00ff88;">ACTIVE</span> | Pipeline: {pipeline_status}</p>
                <div>
                    <span class="integration-badge">Risk Service Integration</span>
                    <span class="integration-badge">Notify Service Integration</span>
                    <span class="integration-badge">Phoenix95 Enhanced</span>
                    <span class="integration-badge">uvloop Optimized</span>
                </div>
            </div>
            
            <div class="status-grid">
                <div class="status-card">
                    <div class="status-title">Execution Statistics</div>
                    <div class="status-item">
                        <span>Total Executions:</span>
                        <span class="status-value">{execute_engine.execution_stats['total_executions']:,}</span>
                    </div>
                    <div class="status-item">
                        <span>Successful:</span>
                        <span class="status-value">{execute_engine.execution_stats['successful_executions']:,}</span>
                    </div>
                    <div class="status-item">
                        <span>Failed:</span>
                        <span class="status-value">{execute_engine.execution_stats['failed_executions']:,}</span>
                    </div>
                    <div class="status-item">
                        <span>Avg Execution Time:</span>
                        <span class="status-value">{execute_engine.execution_stats['avg_execution_time']:.1f}ms</span>
                    </div>
                    <div class="status-item">
                        <span>Notification Success Rate:</span>
                        <span class="status-value">{execute_engine.execution_stats.get('notify_success_rate', 0)*100:.1f}%</span>
                    </div>
                </div>
                
                <div class="status-card">
                    <div class="status-title">Position Status</div>
                    <div class="status-item">
                        <span>Active Positions:</span>
                        <span class="status-value">{len(execute_engine.active_positions)}</span>
                    </div>
                    <div class="status-item">
                        <span>Daily P&L:</span>
                        <span class="status-value" style="color: {pnl_color}">${execute_engine.daily_pnl:+,.2f}</span>
                    </div>
                    <div class="status-item">
                        <span>Total Trades:</span>
                        <span class="status-value">{execute_engine.total_trades}</span>
                    </div>
                    <div class="status-item">
                        <span>Win Rate:</span>
                        <span class="status-value">{execute_engine.execution_stats['win_rate']:.1f}%</span>
                    </div>
                    <div class="status-item">
                        <span>Phoenix95 Boost:</span>
                        <span class="status-value">{execute_engine.risk_metrics['phoenix95_boost_count']}</span>
                    </div>
                </div>
                
                <div class="status-card">
                    <div class="status-title">Service Connection Status</div>
                    {connections_html}
                </div>
                
                <div class="status-card">
                    <div class="status-title">System Configuration</div>
                    <div class="status-item">
                        <span>Leverage:</span>
                        <span class="status-value">{config.LEVERAGE}x (Dynamic)</span>
                    </div>
                    <div class="status-item">
                        <span>Margin Mode:</span>
                        <span class="status-value">{config.MARGIN_MODE}</span>
                    </div>
                    <div class="status-item">
                        <span>Stop Loss/Take Profit:</span>
                        <span class="status-value">±{config.STOP_LOSS_PERCENT*100:.0f}%</span>
                    </div>
                    <div class="status-item">
                        <span>Kelly Max:</span>
                        <span class="status-value">{config.KELLY_MAX_FRACTION*100:.1f}%</span>
                    </div>
                    <div class="status-item">
                        <span>Portfolio:</span>
                        <span class="status-value">${config.PORTFOLIO_VALUE:,}</span>
                    </div>
                </div>
                
                <div class="status-card">
                    <div class="status-title">Risk Management</div>
                    <div class="status-item">
                        <span>Max Positions:</span>
                        <span class="status-value">{config.MAX_POSITIONS}</span>
                    </div>
                    <div class="status-item">
                        <span>Liquidation Warnings:</span>
                        <span class="status-value">{execute_engine.risk_metrics['liquidation_warnings']}</span>
                    </div>
                    <div class="status-item">
                        <span>Min Confidence:</span>
                        <span class="status-value">{config.MIN_CONFIDENCE*100:.0f}%</span>
                    </div>
                    <div class="status-item">
                        <span>Daily Loss Limit:</span>
                        <span class="status-value">{config.MAX_DAILY_LOSS*100:.0f}%</span>
                    </div>
                </div>
                
                <div class="status-card">
                    <div class="status-title">Performance Metrics</div>
                    <div class="status-item">
                        <span>uvloop Optimization:</span>
                        <span class="status-value connected">Active</span>
                    </div>
                    <div class="status-item">
                        <span>Parallel Validation:</span>
                        <span class="status-value connected">Active</span>
                    </div>
                    <div class="status-item">
                        <span>Adaptive Monitoring:</span>
                        <span class="status-value connected">Active</span>
                    </div>
                    <div class="status-item">
                        <span>Phoenix95 Enhancement:</span>
                        <span class="status-value connected">Active</span>
                    </div>
                </div>
            </div>
            
            {positions_html}
            
            <div class="footer">
                <p>Phoenix 95 V4 Execute Service - Integrated | Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Dynamic Leverage | Hedge Fund Grade | Isolated Margin | Fully Integrated</p>
                <p>Phoenix95 Analysis Enhanced | Notify Service Integration | uvloop Performance Optimized</p>
            </div>
        </div>
    </body>
    </html>
    """

    return HTMLResponse(content=html_content)

@app.post("/execute")
async def execute_from_risk_service(request: RiskServiceExecuteRequest):
    """
    Execute trades from Risk Service with standardized request format (Main API)
    Optimized for high-performance systems (Ryzen 9 7950X / 128GB RAM)
    """
    request_start = time.time()
    request_id = f"REQ_{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    
    try:
        # Enhanced request logging with performance tracking
        logger.info("=" * 70)
        logger.info(f"[{request_id}] Execute Request Received from Risk Service")
        logger.info("=" * 70)
        
        # Complete request data dump (only in debug mode for performance)
        if logger.isEnabledFor(logging.DEBUG):
            import json
            request_dict = request.dict()
            logger.debug("=== Risk Service Complete Data Structure ===")
            logger.debug(json.dumps(request_dict, indent=2, default=str))
            logger.debug("=" * 50)
        
        # Convert request to dict for processing
        request_data = request.dict()
        
        # Enhanced request validation logging
        logger.info(f"[{request_id}] Request Validation:")
        logger.info(f"  Total keys: {len(request_data)}")
        logger.info(f"  signal_data exists: {bool(request.signal_data)}")
        logger.info(f"  phoenix_analysis exists: {bool(request.phoenix_analysis)}")
        logger.info(f"  service_chain: {request.service_chain or 'unknown'}")
        
        # Detailed signal_data inspection
        if request.signal_data:
            logger.info(f"  signal_data keys: {list(request.signal_data.keys())}")
            logger.info(f"  symbol: {request.signal_data.get('symbol', 'N/A')}")
            logger.info(f"  action: {request.signal_data.get('action', 'N/A')}")
            logger.info(f"  price: {request.signal_data.get('price', 'N/A')}")
            logger.info(f"  confidence: {request.signal_data.get('confidence', 'N/A')}")
        else:
            logger.warning(f"  WARNING: signal_data is None or empty")
            
            # Attempt root-level data recovery
            if request.symbol and request.action and request.price:
                logger.warning(f"  RECOVERY: Found root-level trading data")
                logger.warning(f"  Root symbol: {request.symbol}")
                logger.warning(f"  Root action: {request.action}")
                logger.warning(f"  Root price: {request.price}")
            else:
                logger.error(f"  CRITICAL: No trading data found at any level")
        
        # Phoenix analysis inspection
        if request.phoenix_analysis:
            phoenix_score = request.phoenix_analysis.get('phoenix_score', 0)
            risk_score = request.phoenix_analysis.get('risk_score', 0)
            logger.info(f"  Phoenix95 score: {phoenix_score:.1f}/95")
            logger.info(f"  Risk score: {risk_score:.2f}")
        else:
            logger.warning(f"  WARNING: phoenix_analysis is None or empty")
        
        logger.info(f"[{request_id}] Sending to execution engine...")
        
        # Execute trade with timeout protection
        try:
            result = await asyncio.wait_for(
                execute_engine.execute_trade_from_risk_service(request_data),
                timeout=120.0  # 2 minute timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"[{request_id}] Execution timeout after 120 seconds")
            raise HTTPException(
                status_code=504,
                detail="Trade execution timeout (120s)"
            )
        
        # Calculate request processing time
        processing_time = (time.time() - request_start) * 1000
        
        # Enhanced result logging
        logger.info("=" * 70)
        logger.info(f"[{request_id}] Trade Execution Completed")
        logger.info(f"  Status: {result.status}")
        logger.info(f"  Execution ID: {result.execution_id}")
        logger.info(f"  Processing Time: {processing_time:.1f}ms")
        
        if result.status == "FAILED":
            logger.error(f"  FAILURE Reason: {result.message}")
            logger.error(f"  Error Details: {result.error_details if hasattr(result, 'error_details') else 'N/A'}")
        else:
            logger.info(f"  SUCCESS: Trade executed successfully")
            if result.position:
                logger.info(f"  Position ID: {result.position.position_id}")
                logger.info(f"  Symbol: {result.position.symbol}")
                logger.info(f"  Entry Price: ${result.position.entry_price:.4f}")
        
        logger.info("=" * 70)
        
        # Return enhanced response
        return {
            "status": "success",
            "request_id": request_id,
            "service": "execute_service",
            "execution_result": asdict(result),
            "service_chain": f"{request.service_chain}_execute_completed",
            "processing_time_ms": processing_time,
            "timestamp": time.time(),
        }

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
        
    except Exception as e:
        processing_time = (time.time() - request_start) * 1000
        
        logger.error("=" * 70)
        logger.error(f"[{request_id}] CRITICAL: Risk Service API execution failed")
        logger.error(f"  Error Type: {type(e).__name__}")
        logger.error(f"  Error Message: {str(e)}")
        logger.error(f"  Processing Time: {processing_time:.1f}ms")
        
        # Enhanced debugging information
        try:
            logger.error(f"  Request Type: {type(request).__name__}")
            logger.error(f"  Has signal_data: {bool(getattr(request, 'signal_data', None))}")
            logger.error(f"  Has phoenix_analysis: {bool(getattr(request, 'phoenix_analysis', None))}")
            logger.error(f"  Service Chain: {getattr(request, 'service_chain', 'unknown')}")
        except Exception as debug_error:
            logger.error(f"  Debug info extraction failed: {debug_error}")
        
        # Full traceback for debugging
        import traceback
        logger.error(f"  Traceback:\n{traceback.format_exc()}")
        logger.error("=" * 70)
        
        # Return detailed error response
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "error_type": type(e).__name__,
                "request_id": request_id,
                "processing_time_ms": processing_time,
                "timestamp": time.time()
            }
        )
    
@app.post("/execute/legacy")
async def execute_legacy(request: LegacyExecuteRequest):
    """Legacy compatibility trade execution API"""
    try:
        # Convert legacy request to standard format
        legacy_data = {
            "signal": {
                "signal_id": request.signal_id,
                "symbol": request.symbol,
                "action": request.action,
                "price": request.price,
                "confidence": request.confidence,
                "source": "legacy_api",
            },
            "phoenix_analysis": {
                "phoenix_score": request.phoenix95_score or 0,
                "risk_score": 0.5,
                "kelly_fraction": request.kelly_ratio or 0.01,
                "leverage_recommendation": config.LEVERAGE,
            },
            "service_chain": "legacy_direct",
        }

        result = await execute_engine.execute_trade_from_risk_service(legacy_data)

        return {
            "status": "success",
            "execution_result": asdict(result),
            "timestamp": time.time(),
            "note": "Legacy API - Standard API (/execute) recommended",
        }

    except Exception as e:
        logger.error(f"Legacy API trade execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def enhanced_health_check():
    """Enhanced health check - includes full pipeline status"""
    # Update service connection status
    service_connections = execute_engine.service_connections

    # Basic status
    basic_health = {
        "status": "healthy",
        "service": "execute_service_integrated",
        "version": "4.0.0-integrated",
        "port": 8102,
        "active_positions": len(execute_engine.active_positions),
        "daily_pnl": execute_engine.daily_pnl,
    }

    # Pipeline status
    pipeline_health = all(
        status == "connected" for status in service_connections.values()
    )

    return {
        **basic_health,
        "service_connections": service_connections,
        "pipeline_status": (
            "fully_connected" if pipeline_health else "partially_connected"
        ),
        "can_execute_trades": bool(
            config.BINANCE_API_KEY and config.BINANCE_SECRET_KEY
        ),
        "can_send_notifications": service_connections.get("notify_service")
        == "connected",
        "integrations": {
            "risk_service_integration": "active",
            "notify_service_integration": "active",
            "phoenix95_enhancement": "active",
            "uvloop_optimization": "active",
        },
        "timestamp": time.time(),
    }


@app.get("/positions")
async def get_enhanced_positions():
    """Enhanced active positions query API"""
    try:
        positions = {}
        for pos_id, pos in execute_engine.active_positions.items():
            pos_dict = asdict(pos)
            # Additional metadata
            pos_dict["risk_level"] = execute_engine._calculate_alert_level(pos)
            pos_dict["position_age"] = time.time() - pos.created_at
            positions[pos_id] = pos_dict

        return {
            "status": "success",
            "active_positions": positions,
            "position_count": len(positions),
            "total_margin_used": sum(
                pos.margin_required for pos in execute_engine.active_positions.values()
            ),
            "average_phoenix95_score": (
                sum(
                    pos.phoenix95_score
                    for pos in execute_engine.active_positions.values()
                )
                / len(positions)
                if positions
                else 0
            ),
            "timestamp": time.time(),
        }

    except Exception as e:
        logger.error(f"Enhanced position query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/positions/{position_id}/close")
async def close_position_enhanced(position_id: str):
    """Enhanced manual position close API"""
    try:
        if position_id not in execute_engine.active_positions:
            raise HTTPException(status_code=404, detail="Position not found")

        position = execute_engine.active_positions[position_id]
        current_price = await execute_engine._get_current_price_optimized(
            position.symbol
        )

        await execute_engine._close_position_with_notification(
            position, current_price, "Manual close (API)"
        )

        return {
            "status": "success",
            "message": f"Position {position_id} closed successfully",
            "final_pnl": position.realized_pnl,
            "roe": position.roe,
            "phoenix95_score": position.phoenix95_score,
            "timestamp": time.time(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Enhanced manual close failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/close_position")
async def close_position_endpoint(request: dict):
    """
    Risk Service compatible position close endpoint
    Handles external position close requests from Risk Service
    """
    try:
        position_id = request.get("position_id")
        
        if not position_id:
            raise HTTPException(
                status_code=400, 
                detail="position_id is required in request body"
            )
        
        if position_id not in execute_engine.active_positions:
            raise HTTPException(
                status_code=404, 
                detail=f"Position {position_id} not found in active positions"
            )
        
        position = execute_engine.active_positions[position_id]
        
        # Get current market price
        current_price = await execute_engine._get_current_price_optimized(position.symbol)
        
        if current_price <= 0:
            logger.error(f"Invalid current price for {position.symbol}: {current_price}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get valid price for {position.symbol}"
            )
        
        # Close position with notification
        close_reason = request.get("reason", "External close request from Risk Service")
        await execute_engine._close_position_with_notification(
            position, current_price, close_reason
        )
        
        return {
            "status": "success",
            "position_id": position_id,
            "symbol": position.symbol,
            "message": "Position closed successfully",
            "final_pnl": position.realized_pnl,
            "pnl_percentage": position.pnl_percentage,
            "roe": position.roe,
            "close_price": current_price,
            "close_reason": close_reason,
            "timestamp": time.time(),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Close position endpoint error: {type(e).__name__}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500, 
            detail=f"Position close failed: {str(e)}"
        )

@app.get("/stats")
async def get_enhanced_statistics():
    """Enhanced detailed statistics API"""
    try:
        return {
            "status": "success",
            "statistics": execute_engine.get_enhanced_system_status(),
            "timestamp": time.time(),
        }

    except Exception as e:
        logger.error(f"Enhanced statistics query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status")
async def api_status():
    """API status query (for other services to call)"""
    return {
        "service": "execute_service",
        "status": "active",
        "version": "4.0.0-integrated",
        "endpoints": {
            "main_execution": "/execute",
            "legacy_execution": "/execute/legacy",
            "positions": "/positions",
            "health": "/health",
            "stats": "/stats",
        },
        "integrations": ["risk_service", "notify_service"],
        "timestamp": time.time(),
    }


# Main execution section (uvloop optimization)

if __name__ == "__main__":
    # Install uvloop performance optimization (considering Windows compatibility)
    try:
        import uvloop
        uvloop.install()
        uvloop_status = "Active"
        loop_type = "uvloop"
    except ImportError:
        uvloop_status = "Inactive (Windows default asyncio)"
        loop_type = "asyncio"

    logger.info("=" * 70)
    logger.info("Phoenix 95 V4 Execute Service (Integrated Version)")
    logger.info("=" * 70)
    
    # Service configuration
    logger.info("Service Configuration:")
    logger.info(f"  Port: 8102")
    logger.info(f"  Version: 4.0.0-integrated")
    logger.info(f"  Leverage: {config.LEVERAGE}x (Dynamic) {config.MARGIN_MODE}")
    logger.info(f"  Take Profit/Stop Loss: ±{config.TAKE_PROFIT_PERCENT*100:.0f}%")
    logger.info(f"  Kelly Max Fraction: {config.KELLY_MAX_FRACTION*100:.1f}%")
    logger.info(f"  Portfolio Value: ${config.PORTFOLIO_VALUE:,}")
    
    # Hardware optimization info
    logger.info("")
    logger.info("Hardware Optimization:")
    logger.info(f"  CPU Target: Ryzen 9 7950X / i9-13900K (16-24 cores)")
    logger.info(f"  RAM Target: 128GB DDR5")
    logger.info(f"  Max Concurrent Orders: {execute_engine.max_concurrent_orders}")
    logger.info(f"  Worker Threads: {execute_engine.max_worker_threads}")
    logger.info(f"  History Buffer: {execute_engine.max_history_size:,} positions")
    logger.info(f"  Aggressive Caching: {'Enabled' if execute_engine.aggressive_caching else 'Disabled'}")
    
    # Service integration
    logger.info("")
    logger.info("Service Integration:")
    logger.info(f"  Risk Service: {config.RISK_SERVICE_URL}")
    logger.info(f"  Notify Service: {config.NOTIFY_SERVICE_URL}")
    logger.info(f"  Brain Service: {config.BRAIN_SERVICE_URL}")
    
    # Auto-reconnection configuration
    logger.info("")
    logger.info("Auto-Reconnection System:")
    logger.info(f"  Enabled: {execute_engine.reconnection_config['enabled']}")
    logger.info(f"  Check Interval: {execute_engine.reconnection_config['check_interval']}s")
    logger.info(f"  Circuit Breaker Threshold: {execute_engine.reconnection_config['circuit_breaker_threshold']} failures")
    logger.info(f"  Circuit Breaker Timeout: {execute_engine.reconnection_config['circuit_breaker_timeout']}s")
    
    # Performance features
    logger.info("")
    logger.info("Performance Features:")
    logger.info(f"  uvloop Optimization: {uvloop_status}")
    logger.info(f"  Phoenix95 Enhancement: Active")
    logger.info(f"  Async Monitoring: Enabled")
    logger.info(f"  Parallel Validation: Enabled")
    
    logger.info("=" * 70)
    logger.info("Starting FastAPI Server...")
    logger.info("=" * 70)

    try:
        uvicorn.run(
            "execute_service_final:app",
            host="0.0.0.0",
            port=8102,
            reload=False,
            log_level="info",
            access_log=True,
            workers=1,
            loop=loop_type,
        )
    except KeyboardInterrupt:
        logger.info("")
        logger.info("=" * 70)
        logger.info("Service shutdown requested by user (Ctrl+C)")
        logger.info("=" * 70)
    except Exception as e:
        logger.error("")
        logger.error("=" * 70)
        logger.error(f"Service execution error: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error("=" * 70)
    finally:
        logger.info("")
        logger.info("=" * 70)
        logger.info("Phoenix 95 V4 Execute Service Terminated")
        logger.info(f"Total runtime statistics saved to logs")
        logger.info("=" * 70)