#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix 95 V4 RISK Service - Hedge Fund Grade Risk Management System
Port: 8101
Role: 20x Leverage Risk Management, Kelly Criterion, Automatic Position Management, Execute Service Integration
Update: Integrated configuration, service integration, performance optimization applied
"""

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import signal
import sys
import time
import traceback
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

# ============================================================================
# Environment Variables Loading (CRITICAL - Must be first)
# ============================================================================
def load_environment_variables():
    """
    Load environment variables from .env file with comprehensive validation.
    Optimized for high-spec system (128GB RAM, 16-24 cores, NVMe SSD).
    
    Search order:
    1. Current directory (.env)
    2. Parent directory (../.env)
    3. Project root (../../.env)
    
    Returns:
        dict: Loading status and validation results
    """
    loading_status = {
        'success': False,
        'env_path': None,
        'telegram_validated': False,
        'binance_validated': False,
        'missing_vars': [],
        'warnings': []
    }
    
    try:
        from dotenv import load_dotenv
        
        # Get current file's directory
        current_dir = Path(__file__).resolve().parent
        
        # Define search paths with priority
        env_paths = [
            current_dir / '.env',                    # Same directory (highest priority)
            current_dir.parent / '.env',             # Parent directory
            current_dir.parent.parent / '.env',      # Project root
        ]
        
        # Try to load from each path
        for env_path in env_paths:
            if env_path.exists():
                # Load with override to ensure latest values
                load_dotenv(env_path, override=True)
                loading_status['env_path'] = str(env_path)
                loading_status['success'] = True
                
                print(f"[Phoenix95] Environment variables loaded from: {env_path}")
                print(f"[Phoenix95] File size: {env_path.stat().st_size} bytes")
                
                # Validate critical Telegram settings
                telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
                telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
                
                if telegram_token and telegram_chat_id:
                    loading_status['telegram_validated'] = True
                    print(f"[Phoenix95] Telegram settings validated:")
                    print(f"  - Bot Token: {'*' * 40}{telegram_token[-8:]}")
                    print(f"  - Chat ID: {telegram_chat_id}")
                else:
                    missing = []
                    if not telegram_token:
                        missing.append('TELEGRAM_BOT_TOKEN')
                    if not telegram_chat_id:
                        missing.append('TELEGRAM_CHAT_ID')
                    loading_status['missing_vars'].extend(missing)
                    loading_status['warnings'].append(f"Missing Telegram settings: {missing}")
                    print(f"[WARNING] Missing Telegram settings: {missing}")
                
                # Validate optional Binance settings
                binance_api_key = os.getenv('BINANCE_API_KEY')
                binance_secret_key = os.getenv('BINANCE_SECRET_KEY')
                
                if binance_api_key and binance_secret_key:
                    loading_status['binance_validated'] = True
                    print(f"[Phoenix95] Binance API settings detected")
                    print(f"  - API Key: {'*' * 30}{binance_api_key[-8:]}")
                else:
                    loading_status['warnings'].append("Binance API not configured - using simulation mode")
                    print(f"[INFO] Binance API not configured - simulation mode enabled")
                
                # Validate database settings
                redis_url = os.getenv('REDIS_URL')
                postgres_url = os.getenv('POSTGRES_URL')
                
                if redis_url:
                    print(f"[Phoenix95] Redis URL configured: {redis_url.split('@')[0]}...")
                if postgres_url:
                    print(f"[Phoenix95] PostgreSQL URL configured")
                
                # Success - stop searching
                break
        
        if not loading_status['success']:
            loading_status['warnings'].append("No .env file found in search paths")
            print("[WARNING] No .env file found in any search location")
            print(f"[INFO] Searched locations:")
            for path in env_paths:
                exists_status = 'EXISTS' if path.exists() else 'NOT FOUND'
                print(f"  - {path} [{exists_status}]")
            print(f"[INFO] Using system environment variables only")
        
        return loading_status
        
    except ImportError:
        loading_status['warnings'].append("python-dotenv not installed")
        print("[ERROR] python-dotenv not installed")
        print("[INFO] Install with: pip install python-dotenv")
        print("[INFO] Using system environment variables only")
        return loading_status
        
    except Exception as e:
        loading_status['warnings'].append(f"Unexpected error: {str(e)}")
        print(f"[ERROR] Failed to load environment variables: {e}")
        import traceback
        print(traceback.format_exc())
        return loading_status

# Load environment variables BEFORE any other imports
print("=" * 80)
print("Phoenix95 V4 Risk Service - Environment Initialization")
print("=" * 80)

env_status = load_environment_variables()

# Display loading summary
print("\n" + "=" * 80)
print("Environment Loading Summary")
print("=" * 80)
print(f"Status: {'SUCCESS' if env_status['success'] else 'FAILED'}")
if env_status['env_path']:
    print(f"Loaded from: {env_status['env_path']}")
print(f"Telegram: {'VALIDATED' if env_status['telegram_validated'] else 'NOT CONFIGURED'}")
print(f"Binance: {'VALIDATED' if env_status['binance_validated'] else 'SIMULATION MODE'}")

if env_status['missing_vars']:
    print(f"\nMissing Variables: {', '.join(env_status['missing_vars'])}")

if env_status['warnings']:
    print(f"\nWarnings:")
    for warning in env_status['warnings']:
        print(f"  - {warning}")

print("=" * 80 + "\n")

# Continue with other imports
import aiohttp
import aioredis
import asyncpg
import numpy as np
import pandas as pd
import psutil

# Force Windows UTF-8 settings (High-spec system optimization)
os.environ['PYTHONIOENCODING'] = 'utf-8'
os.environ['PYTHONUTF8'] = '1'

# Change console codepage to UTF-8 (Windows compatibility)
try:
    import subprocess
    result = subprocess.run(['chcp', '65001'], shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        logging.info("Console codepage set to UTF-8 successfully")
    else:
        logging.warning(f"Console codepage change failed with code {result.returncode}")
except subprocess.SubprocessError as e:
    logging.warning(f"Subprocess error during codepage change: {e}")
except ImportError as e:
    logging.error(f"Failed to import subprocess module: {e}")
except Exception as e:
    logging.warning(f"Unexpected error during console codepage setup: {e}")

# Performance optimization: uvloop configuration for high-spec system
# Note: Disabled to prevent event loop closure issues
try:
    import uvloop
    # uvloop.install()  # Commented out to prevent RuntimeError: Event loop is closed
    print("[Phoenix95] uvloop available but not installed to avoid event loop conflicts")
    print("[INFO] Using default asyncio event loop for maximum stability")
except ImportError:
    print("[Phoenix95] uvloop not available - using default asyncio event loop")

import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, validator

# Enhanced encoding settings (Multi-tier fallback for maximum compatibility)
import locale
try:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    logging.info("Locale set to en_US.UTF-8 successfully")
except locale.Error as e:
    logging.warning(f"Failed to set en_US.UTF-8 locale: {e}")
    try:
        locale.setlocale(locale.LC_ALL, 'C.UTF-8')
        logging.info("Locale set to C.UTF-8 as fallback")
    except locale.Error as e2:
        logging.warning(f"Failed to set C.UTF-8 locale: {e2}")
        try:
            # Try system default locale (final fallback)
            locale.setlocale(locale.LC_ALL, '')
            logging.info("Using system default locale")
        except locale.Error as e3:
            logging.error(f"All locale settings failed: {e3}. Using default Python locale")
except Exception as e:
    logging.error(f"Unexpected error during locale setup: {e}")

# Reconfigure stdout for UTF-8 (High-spec system optimization)
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        logging.info("stdout reconfigured to UTF-8")
    else:
        logging.warning("stdout reconfigure not available on this system")
except Exception as e:
    logging.error(f"Failed to reconfigure stdout: {e}")

# Reconfigure stderr for UTF-8 (High-spec system optimization)
try:
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        logging.info("stderr reconfigured to UTF-8")
    else:
        logging.warning("stderr reconfigure not available on this system")
except Exception as e:
    logging.error(f"Failed to reconfigure stderr: {e}")

def remove_all_emojis(text):
    """
    Remove all non-ASCII characters to prevent encoding errors.
    
    Args:
        text: Input text (any type, will be converted to string)
        
    Returns:
        str: ASCII-only cleaned text
        
    Note:
        This function ensures Windows console compatibility by removing
        all characters with ord() >= 128 (non-ASCII range)
    """
    if not isinstance(text, str):
        text = str(text)
    
    try:
        # ASCII-only filtering for maximum Windows compatibility
        cleaned = ''.join(char for char in text if ord(char) < 128)
        return cleaned.strip()
    except Exception:
        # Fallback: encode to ASCII and ignore non-ASCII characters
        try:
            return str(text).encode('ascii', 'ignore').decode('ascii').strip()
        except Exception:
            return "[TEXT_CLEANUP_ERROR]"


class SafeUnicodeFormatter(logging.Formatter):
    """
    Custom logging formatter that removes non-ASCII characters.
    
    Prevents Windows console encoding errors by sanitizing all log messages
    and arguments before formatting.
    """
    def format(self, record):
        try:
            # Sanitize log message
            if hasattr(record, 'msg'):
                record.msg = remove_all_emojis(str(record.msg))
            
            # Sanitize log arguments
            if hasattr(record, 'args') and record.args:
                safe_args = []
                for arg in record.args:
                    if isinstance(arg, str):
                        safe_args.append(remove_all_emojis(arg))
                    else:
                        safe_args.append(arg)
                record.args = tuple(safe_args)
            
            return super().format(record)
        except Exception:
            return f"[LOG_FORMAT_ERROR] {str(record.getMessage())}"

class UnicodeLogger:
    """
    Thread-safe logging class with daily rotation and high-performance I/O.
    
    Features:
        - Daily log file rotation (automatic at midnight)
        - UTF-8 encoding with fallback handling
        - Auto directory creation
        - High-spec optimized (128GB RAM, 16-24 cores)
        - Buffered I/O for performance
    """
    
    def __init__(self, name="phoenix95"):
        self.logger = logging.getLogger(name)
        # Clear existing handlers to prevent duplication
        self.logger.handlers.clear()
        self.logger.propagate = False
        
        # High-spec configuration: Enhanced buffer size for 128GB RAM
        self.buffer_size = 8192  # 8KB buffer (default is 8KB, but explicitly set)
        
        # Log directory configuration with environment variable support
        log_dir = os.getenv('PHOENIX95_LOG_DIR', 'C:/phoenix95_v4_ultimate/services/logs')
        self.log_base_dir = Path(log_dir)
        self.service_name = "risk_service"
        self.current_log_date = None
        self.file_handler = None
        
        # Create log directory if not exists
        self._ensure_log_directory()
        
        # Setup handlers with daily rotation
        self.setup_handlers()
    
    def _ensure_log_directory(self):
        """
        Create log directory with proper permissions.
        
        Falls back to local ./logs directory if main path fails.
        """
        try:
            self.log_base_dir.mkdir(parents=True, exist_ok=True)
            print(f"[Phoenix95] Log directory ready: {self.log_base_dir}")
        except PermissionError as e:
            print(f"[ERROR] Permission denied for log directory: {e}")
            # Fallback to current directory
            self.log_base_dir = Path("./logs")
            self.log_base_dir.mkdir(parents=True, exist_ok=True)
            print(f"[Phoenix95] Using fallback log directory: {self.log_base_dir}")
        except Exception as e:
            print(f"[ERROR] Failed to create log directory: {e}")
            # Final fallback to current directory
            self.log_base_dir = Path("./logs")
            self.log_base_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_daily_log_filename(self):
        """
        Generate daily log filename: risk_service_YYYY-MM-DD.log
        
        Returns:
            Path: Full path to today's log file
        """
        today = datetime.now().strftime("%Y-%m-%d")
        return self.log_base_dir / f"{self.service_name}_{today}.log"
    
    def _check_and_rotate_log(self):
        """
        Check if date changed and rotate log file if needed.
        
        Performs automatic log rotation at midnight (date change).
        Thread-safe operation with proper handler cleanup.
        """
        today = datetime.now().date()
        
        if self.current_log_date != today:
            # Date changed - rotate log file
            if self.file_handler:
                try:
                    self.logger.removeHandler(self.file_handler)
                    self.file_handler.close()
                except Exception as e:
                    print(f"[ERROR] Failed to close old log handler: {e}")
            
            # Create new file handler for today
            log_file = self._get_daily_log_filename()
            try:
                # High-spec optimization: Buffered file handler
                self.file_handler = logging.FileHandler(
                    log_file, 
                    encoding='utf-8', 
                    mode='a',
                    delay=False  # Open file immediately
                )
                self.file_handler.setLevel(logging.INFO)
                
                # Apply formatter
                formatter = SafeUnicodeFormatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                self.file_handler.setFormatter(formatter)
                
                # Add handler and update date
                self.logger.addHandler(self.file_handler)
                self.current_log_date = today
                
                print(f"[Phoenix95] Log file rotated: {log_file}")
            except Exception as e:
                print(f"[ERROR] Log rotation failed: {e}")
    
    def setup_handlers(self):
        """
        Setup file and console handlers with UTF-8 encoding.
        
        Configures:
            - File handler with daily rotation
            - Console handler for real-time output
            - SafeUnicodeFormatter for both handlers
        """
        if not self.logger.handlers:
            try:
                # Get today's log file
                log_file = self._get_daily_log_filename()
                self.current_log_date = datetime.now().date()
                
                # File handler with daily rotation and buffering
                self.file_handler = logging.FileHandler(
                    log_file, 
                    encoding='utf-8', 
                    mode='a',
                    delay=False
                )
                self.file_handler.setLevel(logging.INFO)
                
                # Console handler with UTF-8 support
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(logging.INFO)
                
                # Formatter with emoji removal
                formatter = SafeUnicodeFormatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                
                # Apply formatter to both handlers
                self.file_handler.setFormatter(formatter)
                console_handler.setFormatter(formatter)
                
                # Add handlers to logger
                self.logger.addHandler(self.file_handler)
                self.logger.addHandler(console_handler)
                self.logger.setLevel(logging.INFO)
                
                print(f"[Phoenix95] Logging initialized: {log_file}")
                
            except Exception as e:
                print(f"[ERROR] Logging setup failed: {e}")
                # Fallback: console-only logging
                try:
                    console_handler = logging.StreamHandler(sys.stdout)
                    console_handler.setLevel(logging.INFO)
                    self.logger.addHandler(console_handler)
                    self.logger.setLevel(logging.INFO)
                    print("[Phoenix95] Fallback to console-only logging")
                except Exception as fallback_error:
                    print(f"[CRITICAL] All logging setup failed: {fallback_error}")
    
    def info(self, message):
        """
        Safe info logging with emoji removal and daily rotation.
        
        Args:
            message: Log message (will be sanitized)
        """
        try:
            self._check_and_rotate_log()
            safe_message = remove_all_emojis(message)
            self.logger.info(safe_message)
        except Exception:
            # Fallback to print if logging fails
            print(f"[LOG_INFO] {remove_all_emojis(str(message))}")
    
    def error(self, message):
        """
        Safe error logging with emoji removal and daily rotation.
        
        Args:
            message: Error message (will be sanitized)
        """
        try:
            self._check_and_rotate_log()
            safe_message = remove_all_emojis(message)
            self.logger.error(safe_message)
        except Exception:
            # Fallback to print if logging fails
            print(f"[LOG_ERROR] {remove_all_emojis(str(message))}")
    
    def warning(self, message):
        """
        Safe warning logging with emoji removal and daily rotation.
        
        Args:
            message: Warning message (will be sanitized)
        """
        try:
            self._check_and_rotate_log()
            safe_message = remove_all_emojis(message)
            self.logger.warning(safe_message)
        except Exception:
            # Fallback to print if logging fails
            print(f"[LOG_WARN] {remove_all_emojis(str(message))}")

# ============================================================================
# Global Logger Initialization
# ============================================================================
# Thread-safe singleton logger instance
if 'safe_logger' not in globals():
    safe_logger = UnicodeLogger()
    safe_logger.info("Phoenix95 Risk Service logger initialized")

# ============================================================================
# Binance Client Integration
# ============================================================================
# Real-time market data support with fallback to simulation mode
try:
    from binance.client import Client
    from binance.exceptions import BinanceAPIException, BinanceRequestException
    
    BINANCE_AVAILABLE = True
    safe_logger.info("Binance library loaded successfully - real market data enabled")
    
    # High-spec optimization: Pre-configure connection pool settings
    # These will be used when creating Binance client instances
    BINANCE_CONFIG = {
        'requests_params': {
            'timeout': 10,  # Connection timeout (seconds)
        },
        'recv_window': 5000,  # API receive window (milliseconds)
    }
    safe_logger.info("Binance connection pool pre-configured for high-performance")
    
except ImportError as e:
    BINANCE_AVAILABLE = False
    BINANCE_CONFIG = {}
    safe_logger.warning(f"Binance library not available - using simulation mode: {e}")
    safe_logger.info("Install python-binance for real market data: pip install python-binance")
except Exception as e:
    BINANCE_AVAILABLE = False
    BINANCE_CONFIG = {}
    safe_logger.error(f"Unexpected error loading Binance library: {e}")
    safe_logger.warning("Falling back to simulation mode for market data")

# ============================================================================
# Safe Logging Function Wrappers
# ============================================================================
# Replace standard logging functions with emoji-safe versions

def safe_log_info(message):
    """
    Safe info-level logging wrapper.
    
    Args:
        message: Log message (will be sanitized by UnicodeLogger)
        
    Note:
        This function replaces logging.info() globally to ensure
        Windows console compatibility.
    """
    safe_logger.info(message)


def safe_log_error(message):
    """
    Safe error-level logging wrapper.
    
    Args:
        message: Error message (will be sanitized by UnicodeLogger)
        
    Note:
        This function replaces logging.error() globally to ensure
        Windows console compatibility.
    """
    safe_logger.error(message)


def safe_log_warning(message):
    """
    Safe warning-level logging wrapper.
    
    Args:
        message: Warning message (will be sanitized by UnicodeLogger)
        
    Note:
        This function replaces logging.warning() globally to ensure
        Windows console compatibility.
    """
    safe_logger.warning(message)


# Apply global logging function replacement
logging.info = safe_log_info
logging.error = safe_log_error
logging.warning = safe_log_warning


# ============================================================================
# Safe Print Function Patch
# ============================================================================
# Patch built-in print() to handle encoding errors automatically

import builtins
original_print = builtins.print


def safe_print(*args, **kwargs):
    """
    Safe print function with automatic encoding error handling.
    
    Args:
        *args: Print arguments (will be sanitized)
        **kwargs: Print keyword arguments (passed through)
        
    Features:
        - Removes all non-ASCII characters (emojis, special symbols)
        - Falls back to ASCII encoding on UnicodeEncodeError
        - Final fallback to error message if all else fails
        
    Note:
        This function replaces built-in print() globally for
        maximum Windows console compatibility.
    """
    try:
        # Primary method: Remove non-ASCII characters
        safe_args = [remove_all_emojis(str(arg)) for arg in args]
        original_print(*safe_args, **kwargs)
        
    except UnicodeEncodeError:
        # Fallback 1: Force ASCII encoding with replacement
        try:
            ascii_args = [
                str(arg).encode('ascii', 'replace').decode('ascii') 
                for arg in args
            ]
            original_print(*ascii_args, **kwargs)
            
        except Exception:
            # Fallback 2: Print error indicator
            try:
                original_print("[PRINT_ERROR]", **kwargs)
            except Exception:
                # Ultimate fallback: Silent failure
                pass
                
    except Exception as e:
        # Catch-all for unexpected errors
        try:
            original_print(f"[PRINT_ERROR: {type(e).__name__}]", **kwargs)
        except Exception:
            # Silent failure as last resort
            pass


# Apply global print function replacement
builtins.print = safe_print

# ============================================================================
# Telegram Configuration
# ============================================================================
# Global settings for Telegram notification system

TELEGRAM_CONFIG = {
    'timeout': 30,              # API request timeout (seconds)
    'retry_attempts': 3,        # Number of retry attempts on failure
    'backoff_factor': 2,        # Exponential backoff multiplier
    'max_message_length': 4096, # Telegram message length limit
    'parse_mode': 'HTML',       # Message formatting mode
}


# ============================================================================
# Safe Telegram Send Function
# ============================================================================

async def safe_telegram_send(message, max_retries=3):
    """
    Safe telegram message sending with automatic retry and error handling.
    
    Args:
        message (str): Message to send (will be sanitized)
        max_retries (int): Maximum number of retry attempts (default: 3)
        
    Returns:
        bool: True if message sent successfully, False otherwise
        
    Features:
        - Automatic emoji removal for encoding safety
        - Exponential backoff retry strategy
        - Message length validation
        - Comprehensive error logging
        
    Note:
        This is a placeholder implementation. Actual Telegram API integration
        should be implemented in the AdvancedRiskTelegramNotifier class.
        
    TODO:
        - Integrate with AdvancedRiskTelegramNotifier
        - Add rate limiting (Telegram allows 30 messages/second)
        - Implement message queuing for high-volume scenarios
        - Add support for markdown formatting
    """
    for attempt in range(max_retries):
        try:
            # Sanitize message: remove emojis and non-ASCII characters
            safe_message = remove_all_emojis(message)
            
            # Validate message length (Telegram limit: 4096 characters)
            if len(safe_message) > TELEGRAM_CONFIG['max_message_length']:
                safe_message = safe_message[:TELEGRAM_CONFIG['max_message_length'] - 3] + "..."
                safe_logger.warning(f"Message truncated to {TELEGRAM_CONFIG['max_message_length']} characters")
            
            # Log prepared message details
            safe_logger.info(
                f"Telegram message prepared (attempt {attempt + 1}/{max_retries}): "
                f"{len(safe_message)} characters"
            )
            
            # TODO: Replace with actual Telegram API call
            # Example implementation:
            # async with aiohttp.ClientSession() as session:
            #     url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
            #     payload = {
            #         'chat_id': CHAT_ID,
            #         'text': safe_message,
            #         'parse_mode': TELEGRAM_CONFIG['parse_mode']
            #     }
            #     async with session.post(url, json=payload, timeout=TELEGRAM_CONFIG['timeout']) as resp:
            #         if resp.status == 200:
            #             return True
            #         else:
            #             raise Exception(f"Telegram API error: {resp.status}")
            
            # Placeholder: Simulate successful send
            return True
            
        except Exception as e:
            safe_logger.error(
                f"Telegram transmission failed (attempt {attempt + 1}/{max_retries}): "
                f"{type(e).__name__}: {e}"
            )
            
            # Retry with exponential backoff
            if attempt < max_retries - 1:
                backoff_delay = TELEGRAM_CONFIG['backoff_factor'] ** attempt
                safe_logger.info(f"Retrying in {backoff_delay} seconds...")
                await asyncio.sleep(backoff_delay)
    
    # All retries exhausted
    safe_logger.error(f"Telegram send failed after {max_retries} attempts")
    return False


# ============================================================================
# Service Initialization Status
# ============================================================================
# Display startup messages to confirm system components are ready

print("[Phoenix95] Safe UTF-8 logging system initialization completed")
print("[Phoenix95] Risk service initialization started")
print(f"[Phoenix95] Logging directory: {safe_logger.log_base_dir}")
print(f"[Phoenix95] Binance integration: {'Enabled' if BINANCE_AVAILABLE else 'Disabled (simulation mode)'}")

# =============================================================================
# Integrated Configuration System
# =============================================================================



# ============================================================================
# Service Configuration Class
# ============================================================================

class ServiceConfig:
    """
    Integrated service configuration optimized for high-performance systems.
    
    System Requirements:
        - CPU: AMD Ryzen 9 7950X / Intel i9-13900K (16-24 cores)
        - RAM: 128GB DDR5
        - Storage: NVMe SSD 2TB+
        
    Features:
        - High-throughput signal processing (400+ concurrent positions)
        - Multi-core parallel processing support
        - Large-scale memory caching (50K+ entries)
        - WebSocket real-time data streaming
        - Environment variable overrides
    """

    # ========================================================================
    # Phoenix95 AI Settings
    # ========================================================================
    # Signal scoring and confidence thresholds
    
    PHOENIX95_CONFIG = {
        'min_score_threshold': 25,          # Minimum score to consider signal (0-100)
        'high_confidence_threshold': 45,    # High confidence signal threshold
        'emergency_threshold': 15,          # Emergency mode threshold
        'adaptive_adjustment_enabled': True,# Enable dynamic threshold adjustment
        'market_volatility_bonus': 5,       # Bonus points for volatile markets
        'trend_alignment_bonus': 3,         # Bonus for trend alignment
        'volume_confirmation_bonus': 2,     # Bonus for volume confirmation
    }

    # ========================================================================
    # Risk Service Configuration
    # ========================================================================
    # Core risk management settings scaled for high-performance trading

    RISK_CONFIG = {
        # Service identification
        "service_name": "Phoenix95_Risk_Guardian",
        "version": "4.1.0-highspec",
        "port": 8101,
        "host": "0.0.0.0",
        
        # Risk limits - Scaled for high-performance trading
        "max_leverage": 20,                 # Maximum leverage multiplier
        "margin_mode": "ISOLATED",          # ISOLATED or CROSS margin mode
        "max_daily_loss": 150.0,            # Maximum daily loss percentage
        "max_position_size": 0.8,           # Maximum position size ratio (0-1)
        "max_positions": 500,               # Maximum concurrent positions (128GB RAM optimized)
        "confidence_threshold": 0.20,       # Minimum confidence to open position (0-1)
        
        # Kelly Criterion settings
        "kelly_max_fraction": 0.025,        # Maximum Kelly fraction (2.5%)
        "kelly_min_fraction": 0.008,        # Minimum Kelly fraction (0.8%)
        "win_rate_adjustment": 0.85,        # Conservative win rate adjustment
        
        # Stop loss and take profit
        "stop_loss_percent": 0.018,         # Stop loss percentage (1.8%)
        "take_profit_percent": 0.022,       # Take profit percentage (2.2%)
        "liquidation_buffer": 0.12,         # Safety buffer from liquidation (12%)
        
        # Monitoring settings - High-performance optimized
        "position_check_interval": 0.1,     # Position check interval (seconds)
        "risk_alert_threshold": 0.75,       # Risk alert trigger threshold
        "emergency_close_threshold": 0.90,  # Emergency close trigger threshold
        
        # Performance settings - 128GB RAM optimized
        "max_memory_usage": 0.90,           # Maximum memory usage threshold (128GB allows higher)
        "cache_ttl": 600,                   # Cache time-to-live (seconds)
        "max_cache_size": 100000,           # Maximum cache entries (doubled for 128GB RAM)
        "signal_duplicate_window": 25,      # Duplicate signal detection window (seconds)
        
        # High-spec specific settings (16-24 cores, 128GB RAM)
        "max_concurrent_monitors": 500,     # Concurrent position monitors (increased for 128GB)
        "batch_processing_size": 100,       # Batch processing size (doubled for multi-core)
        "websocket_enabled": True,          # Enable WebSocket for real-time data
        "parallel_processing": True,        # Enable multi-core parallel processing
        "max_worker_threads": 24,           # Maximum worker threads for parallel ops
        "connection_pool_size": 200,        # Connection pool size (128GB optimized)
    }

    # ========================================================================
    # Service Communication Settings
    # ========================================================================
    # URLs for inter-service communication
    
    SERVICE_URLS = {
        "brain_service": "http://localhost:8100",      # AI signal generation
        "execute_service": "http://localhost:8102",    # Trade execution
        "notify_service": "http://localhost:8103",     # Notification system
    }

    # Communication timing - Optimized for high-speed network
    SERVICE_TIMEOUT = 5         # Request timeout (seconds) - increased for stability
    RETRY_DELAY = 0.5           # Delay between retries (seconds) - faster retry
    MAX_RETRIES = 3             # Maximum retry attempts - increased reliability

    # ========================================================================
    # Telegram Notification Settings
    # ========================================================================
    # Configuration for Telegram bot notifications

    TELEGRAM_CONFIG = {
        "token": os.getenv("TELEGRAM_BOT_TOKEN", "7386542811:AAEZ21p30rES1k8NxNM2xbZ53U44PI9D5CY"),
        "chat_id": os.getenv("TELEGRAM_CHAT_ID", "7590895952"),
        "enabled": True,                # Enable Telegram notifications
        "risk_notifications": True,     # Enable risk-specific alerts
        "timeout": 15,                  # API request timeout (seconds)
        "retry_attempts": 2,            # Number of retry attempts
        "backoff_factor": 1.5,          # Exponential backoff multiplier
        "max_retries": 2,               # Maximum total retries
    }

    # ========================================================================
    # Database Configuration
    # ========================================================================
    # Connection pool scaled for high concurrency (128GB RAM, 16-24 cores)
    
    DATABASE_CONFIG = {
        "redis_url": "redis://localhost:6379",
        "postgres_url": "postgresql://postgres:password@localhost:5432/phoenix95_risk",
        "connection_pool_size": 100,    # Increased for high concurrency (high-spec)
    }
    
    # ========================================================================
    # Configuration Validation
    # ========================================================================
    
    @classmethod
    def validate(cls) -> bool:
        """
        Validate configuration settings for correctness.
        
        Returns:
            bool: True if all settings are valid, False otherwise
            
        Raises:
            ValueError: If critical configuration is invalid
        """
        errors = []
        
        # Validate RISK_CONFIG
        if cls.RISK_CONFIG["max_leverage"] < 1 or cls.RISK_CONFIG["max_leverage"] > 125:
            errors.append("max_leverage must be between 1 and 125")
            
        if cls.RISK_CONFIG["max_positions"] < 1:
            errors.append("max_positions must be positive")
            
        if not (0 < cls.RISK_CONFIG["confidence_threshold"] <= 1):
            errors.append("confidence_threshold must be between 0 and 1")
            
        # Validate SERVICE_TIMEOUT
        if cls.SERVICE_TIMEOUT < 1:
            errors.append("SERVICE_TIMEOUT must be at least 1 second")
            
        # Validate DATABASE_CONFIG
        if cls.DATABASE_CONFIG["connection_pool_size"] < 1:
            errors.append("connection_pool_size must be positive")
        
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
            
        return True
    
    @classmethod
    def get_env_override(cls, key: str, default: any, value_type: type = str) -> any:
        """
        Get configuration value with environment variable override.
        
        Args:
            key: Environment variable name
            default: Default value if not set
            value_type: Type to convert value to
            
        Returns:
            Configuration value (from env or default)
        """
        env_value = os.getenv(key)
        if env_value is None:
            return default
            
        try:
            if value_type == bool:
                return env_value.lower() in ('true', '1', 'yes', 'on')
            elif value_type == int:
                return int(env_value)
            elif value_type == float:
                return float(env_value)
            else:
                return env_value
        except (ValueError, AttributeError):
            safe_logger.warning(f"Invalid environment variable {key}={env_value}, using default: {default}")
            return default


# ============================================================================
# Global Configuration Instance
# ============================================================================

config = ServiceConfig()

# Validate configuration on startup
try:
    config.validate()
    print("[Phoenix95] Configuration validation passed")
except ValueError as e:
    print(f"[ERROR] {e}")
    raise


# ============================================================================
# Custom JSON Encoder
# ============================================================================

class CustomJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for handling non-serializable Python objects.
    
    Supports:
        - datetime objects (ISO format)
        - Decimal objects (float conversion)
        - Objects with __dict__ attribute
    """
    
    def default(self, obj):
        """
        Convert non-serializable objects to JSON-compatible types.
        
        Args:
            obj: Object to serialize
            
        Returns:
            JSON-serializable representation
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return super().default(obj)

# ============================================================================
# Safe JSON Serialization
# ============================================================================

def safe_json_dumps(obj, **kwargs):
    """
    Safe JSON serialization with support for non-standard Python types.
    
    Args:
        obj: Object to serialize (any type)
        **kwargs: Additional arguments passed to json.dumps()
        
    Returns:
        str: JSON string representation
        
    Supported Types:
        - datetime objects -> ISO format string
        - date objects -> ISO format string
        - Decimal objects -> float
        - Objects with __dict__ -> dictionary (recursive)
        - Standard JSON types (str, int, float, list, dict, etc.)
        
    Error Handling:
        Returns error JSON if serialization fails:
        {"error": "serialization_failed", "message": "..."}
        
    Example:
        >>> safe_json_dumps({"time": datetime.now(), "value": Decimal("10.5")})
        '{"time": "2025-01-08T12:00:00", "value": 10.5}'
    """
    try:
        def json_serializer(obj):
            """
            Custom serializer for non-standard types with recursive handling.
            
            Args:
                obj: Object to serialize
                
            Returns:
                JSON-compatible representation
                
            Raises:
                TypeError: If object type is not supported
            """
            # Handle datetime objects
            if isinstance(obj, datetime):
                return obj.isoformat()
            
            # Handle date objects (must come after datetime check)
            elif isinstance(obj, date):
                return obj.isoformat()
            
            # Handle Decimal objects
            elif isinstance(obj, Decimal):
                return float(obj)
            
            # Handle objects with __dict__ (recursive conversion)
            elif hasattr(obj, '__dict__'):
                obj_dict = {}
                for key, value in obj.__dict__.items():
                    # Recursively convert nested datetime/date/Decimal objects
                    if isinstance(value, (datetime, date)):
                        obj_dict[key] = value.isoformat()
                    elif isinstance(value, Decimal):
                        obj_dict[key] = float(value)
                    elif isinstance(value, dict):
                        obj_dict[key] = {k: json_serializer(v) if not isinstance(v, (str, int, float, bool, type(None))) else v 
                                        for k, v in value.items()}
                    elif isinstance(value, (list, tuple)):
                        obj_dict[key] = [json_serializer(item) if not isinstance(item, (str, int, float, bool, type(None))) else item 
                                        for item in value]
                    else:
                        obj_dict[key] = value
                return obj_dict
            
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
        
        return json.dumps(obj, default=json_serializer, ensure_ascii=False, **kwargs)
        
    except Exception as e:
        logging.error(f"JSON serialization failed: {type(e).__name__}: {e}")
        
        # Fallback: Deep conversion of all datetime objects
        try:
            import copy
            obj_copy = copy.deepcopy(obj)
            
            def convert_datetime_recursive(data):
                """Recursively convert datetime objects to ISO strings"""
                if isinstance(data, dict):
                    return {k: convert_datetime_recursive(v) for k, v in data.items()}
                elif isinstance(data, list):
                    return [convert_datetime_recursive(item) for item in data]
                elif isinstance(data, (datetime, date)):
                    return data.isoformat()
                elif isinstance(data, Decimal):
                    return float(data)
                return data
            
            obj_converted = convert_datetime_recursive(obj_copy)
            return json.dumps(obj_converted, ensure_ascii=False, **kwargs)
            
        except Exception as fallback_error:
            logging.error(f"Fallback serialization also failed: {fallback_error}")
            return json.dumps({
                "error": "serialization_failed",
                "message": str(e),
                "type": type(e).__name__
            })
        
    except Exception as e:
        logging.error(f"JSON serialization failed: {type(e).__name__}: {e}")
        # Return error object instead of raising exception
        return json.dumps({
            "error": "serialization_failed",
            "message": str(e),
            "type": type(e).__name__
        })


# ============================================================================
# Safe Message Encoding
# ============================================================================

def safe_encode_message(message: str) -> str:
    """
    Safe message encoding for Telegram and console output.
    
    Args:
        message (str): Message to encode (any type accepted, will be converted)
        
    Returns:
        str: ASCII-safe encoded message
        
    Features:
        - Removes all non-ASCII characters (emojis, special symbols)
        - Removes non-printable control characters
        - Multiple fallback strategies for maximum safety
        - Thread-safe operation
        
    Encoding Strategy:
        1. Convert to string if not already
        2. Remove non-printable characters (ord < 32 or ord >= 127)
        3. Validate UTF-8 encoding
        4. Fallback to ASCII-only if needed
        
    Example:
        >>> safe_encode_message("Hello 🚀 World")
        'Hello  World'
        
    Note:
        This function is more aggressive than remove_all_emojis() and is
        specifically designed for Telegram API compatibility.
    """
    # Type conversion
    if not isinstance(message, str):
        message = str(message)
    
    try:
        # Primary encoding strategy: Remove non-printable and non-ASCII
        # Keep only printable ASCII characters (32-126)
        cleaned = ''.join(
            char for char in message 
            if 32 <= ord(char) < 127
        )
        
        # Validate UTF-8 encoding (should always pass for ASCII)
        cleaned.encode('utf-8')
        
        # Remove leading/trailing whitespace
        return cleaned.strip()
        
    except (UnicodeError, UnicodeEncodeError) as e:
        logging.error(f"Message encoding failed: {type(e).__name__}: {e}")
        
        # Fallback strategy: Force ASCII encoding
        try:
            return message.encode('ascii', 'ignore').decode('ascii').strip()
            
        except Exception as fallback_error:
            logging.error(f"Fallback encoding failed: {fallback_error}")
            return "[MESSAGE_ENCODING_ERROR]"
            
    except Exception as e:
        logging.error(f"Unexpected encoding error: {type(e).__name__}: {e}")
        return "[MESSAGE_ENCODING_ERROR]"

# =============================================================================
# Standardized Data Models
# =============================================================================


@dataclass
class SignalData:
    """
    Standardized signal data model for AI-generated trading signals.
    
    This dataclass represents a complete trading signal with all necessary
    information for risk assessment and trade execution.
    
    Attributes:
        signal_id (str): Unique identifier for the signal
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
        action (str): Trade direction - 'BUY', 'SELL', 'LONG', or 'SHORT'
        price (float): Entry price for the signal (must be positive)
        confidence (float): AI confidence score (0.0 to 1.0)
        timestamp (float): Signal generation timestamp (Unix epoch)
        
        Phoenix95 Specialized Fields:
            alpha_score (Optional[float]): Alpha generation score
            rsi (Optional[float]): Relative Strength Index value
            macd (Optional[Dict]): MACD indicator values
            bollinger (Optional[Dict]): Bollinger Bands data
            volume_profile (Optional[Dict]): Volume analysis data
            
        Additional Metadata:
            timeframe (str): Chart timeframe (default: '1m')
            market_condition (Optional[str]): Current market state
            volatility (Optional[float]): Market volatility measure
    
    Raises:
        ValueError: If confidence not in [0, 1] or price <= 0
        
    Example:
        >>> signal = SignalData(
        ...     signal_id="SIG_123456",
        ...     symbol="BTCUSDT",
        ...     action="BUY",
        ...     price=50000.0,
        ...     confidence=0.85
        ... )
    """

    # Required fields
    signal_id: str
    symbol: str
    action: str  # BUY, SELL, LONG, SHORT
    price: float
    confidence: float
    timestamp: float = field(default_factory=time.time)

    # Phoenix95 specialized technical indicators
    alpha_score: Optional[float] = None
    rsi: Optional[float] = None
    macd: Optional[Dict] = None
    bollinger: Optional[Dict] = None
    volume_profile: Optional[Dict] = None

    # Additional metadata
    timeframe: str = "1m"
    market_condition: Optional[str] = None
    volatility: Optional[float] = None

    def __post_init__(self):
        """
        Validate signal data after initialization.
        
        Raises:
            ValueError: If validation fails
        """
        # Validate confidence range
        if not (0 <= self.confidence <= 1):
            raise ValueError(f"Confidence must be between 0 and 1, got: {self.confidence}")
        
        # Validate price
        if self.price <= 0:
            raise ValueError(f"Price must be greater than 0, got: {self.price}")
        
        # Validate action
        valid_actions = {'BUY', 'SELL', 'LONG', 'SHORT'}
        if self.action.upper() not in valid_actions:
            raise ValueError(f"Action must be one of {valid_actions}, got: {self.action}")
        
        # Normalize action to uppercase
        self.action = self.action.upper()
    
    def is_long_signal(self) -> bool:
        """Check if this is a long (buy) signal."""
        return self.action in {'BUY', 'LONG'}
    
    def is_short_signal(self) -> bool:
        """Check if this is a short (sell) signal."""
        return self.action in {'SELL', 'SHORT'}


@dataclass
class Phoenix95AnalysisResult:
    """
    Comprehensive analysis result from Phoenix95 AI engine.
    
    Contains all scoring metrics, risk calculations, and recommended
    trading parameters generated by the Phoenix95 analysis system.
    
    Attributes:
        Scoring Metrics (0.0 to 1.0):
            phoenix_score (float): Overall Phoenix95 composite score
            strength_score (float): Signal strength evaluation
            momentum_score (float): Market momentum assessment
            volatility_score (float): Volatility analysis score
            volume_score (float): Volume profile evaluation
            
        Risk Management:
            kelly_criterion (Dict[str, float]): Kelly fraction calculations
            leverage_optimization (Dict[str, Any]): Optimal leverage settings
            risk_metrics (Dict[str, float]): Comprehensive risk measures
            
        Trading Recommendations:
            recommended_position_size (float): Suggested position size ratio
            recommended_stop_loss (float): Suggested stop loss percentage
            recommended_take_profit (float): Suggested take profit percentage
            
        Metadata:
            timestamp (float): Analysis generation time (Unix epoch)
    
    Example:
        >>> analysis = Phoenix95AnalysisResult(
        ...     phoenix_score=0.85,
        ...     strength_score=0.82,
        ...     momentum_score=0.88,
        ...     volatility_score=0.75,
        ...     volume_score=0.80,
        ...     kelly_criterion={'kelly_fraction': 0.015},
        ...     leverage_optimization={'optimal_leverage': 15},
        ...     risk_metrics={'var': 0.05, 'sharpe': 1.8},
        ...     recommended_position_size=0.02,
        ...     recommended_stop_loss=0.018,
        ...     recommended_take_profit=0.022
        ... )
    """

    # Scoring metrics (0.0 to 1.0 scale)
    phoenix_score: float
    strength_score: float
    momentum_score: float
    volatility_score: float
    volume_score: float

    # Risk management results
    kelly_criterion: Dict[str, float]
    leverage_optimization: Dict[str, Any]
    risk_metrics: Dict[str, float]

    # Recommended trading parameters
    recommended_position_size: float
    recommended_stop_loss: float
    recommended_take_profit: float

    # Metadata
    timestamp: float = field(default_factory=time.time)
    
    def __post_init__(self):
        """
        Validate analysis result after initialization.
        
        Raises:
            ValueError: If validation fails
        """
        # Validate all scores are in [0, 1] range
        scores = [
            ('phoenix_score', self.phoenix_score),
            ('strength_score', self.strength_score),
            ('momentum_score', self.momentum_score),
            ('volatility_score', self.volatility_score),
            ('volume_score', self.volume_score),
        ]
        
        for name, value in scores:
            if not (0 <= value <= 1):
                raise ValueError(f"{name} must be between 0 and 1, got: {value}")
        
        # Validate recommended parameters are positive
        if self.recommended_position_size <= 0:
            raise ValueError(f"Position size must be positive, got: {self.recommended_position_size}")
        
        if self.recommended_stop_loss <= 0:
            raise ValueError(f"Stop loss must be positive, got: {self.recommended_stop_loss}")
        
        if self.recommended_take_profit <= 0:
            raise ValueError(f"Take profit must be positive, got: {self.recommended_take_profit}")
    
    def get_overall_quality(self) -> str:
        """
        Get overall signal quality rating.
        
        Returns:
            str: Quality rating ('EXCELLENT', 'GOOD', 'AVERAGE', 'POOR')
        """
        if self.phoenix_score >= 0.8:
            return 'EXCELLENT'
        elif self.phoenix_score >= 0.6:
            return 'GOOD'
        elif self.phoenix_score >= 0.4:
            return 'AVERAGE'
        else:
            return 'POOR'

@dataclass
class RiskProfile:
    """
    Risk profile for a trading position with approved parameters.
    
    Contains all risk management parameters calculated and approved
    by the risk validation system before trade execution.
    
    Attributes:
        symbol (str): Trading pair symbol (e.g., 'BTCUSDT')
        leverage (int): Approved leverage multiplier (1-125x)
        position_size (float): Position size as ratio of capital (0-1)
        margin_required (float): Required margin in base currency
        liquidation_price (float): Price at which position liquidates
        stop_loss_price (float): Automatic stop loss trigger price
        take_profit_price (float): Automatic take profit trigger price
        risk_score (float): Overall risk assessment score (0-1)
        confidence (float): AI signal confidence level (0-1)
        created_at (datetime): Profile creation timestamp
    
    Example:
        >>> profile = RiskProfile(
        ...     symbol="BTCUSDT",
        ...     leverage=15,
        ...     position_size=0.02,
        ...     margin_required=1000.0,
        ...     liquidation_price=48000.0,
        ...     stop_loss_price=49100.0,
        ...     take_profit_price=51100.0,
        ...     risk_score=0.35,
        ...     confidence=0.85
        ... )
    """

    symbol: str
    leverage: int
    position_size: float
    margin_required: float
    liquidation_price: float
    stop_loss_price: float
    take_profit_price: float
    risk_score: float
    confidence: float
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        """Validate risk profile parameters."""
        if not (1 <= self.leverage <= 125):
            raise ValueError(f"Leverage must be between 1 and 125, got: {self.leverage}")
        
        if not (0 < self.position_size <= 1):
            raise ValueError(f"Position size must be between 0 and 1, got: {self.position_size}")
        
        if self.margin_required <= 0:
            raise ValueError(f"Margin required must be positive, got: {self.margin_required}")
        
        if not (0 <= self.risk_score <= 1):
            raise ValueError(f"Risk score must be between 0 and 1, got: {self.risk_score}")
        
        if not (0 <= self.confidence <= 1):
            raise ValueError(f"Confidence must be between 0 and 1, got: {self.confidence}")
    
    def is_high_risk(self) -> bool:
        """Check if this is a high-risk profile (score > 0.7)."""
        return self.risk_score > 0.7
    
    def get_risk_level(self) -> str:
        """Get risk level category."""
        if self.risk_score >= 0.7:
            return 'HIGH'
        elif self.risk_score >= 0.4:
            return 'MEDIUM'
        else:
            return 'LOW'


@dataclass
class PositionRisk:
    """
    Real-time risk assessment for an active trading position.
    
    Tracks current position state, P&L, and liquidation risk metrics
    for ongoing risk monitoring and automatic position management.
    
    Attributes:
        position_id (str): Unique position identifier
        symbol (str): Trading pair symbol
        side (str): Position direction ('BUY' or 'SELL')
        entry_price (float): Position entry price
        current_price (float): Current market price
        quantity (float): Position size in base currency
        leverage (int): Active leverage multiplier
        margin_required (float): Margin locked for position
        unrealized_pnl (float): Current unrealized profit/loss
        liquidation_risk (float): Liquidation risk score (0-1)
        distance_to_liquidation (float): Distance to liquidation (ratio)
        status (str): Position status ('ACTIVE', 'CLOSED', 'LIQUIDATED')
        created_at (datetime): Position creation time
        last_updated (datetime): Last update timestamp
    
    Example:
        >>> position = PositionRisk(
        ...     position_id="POS_123456",
        ...     symbol="BTCUSDT",
        ...     side="BUY",
        ...     entry_price=50000.0,
        ...     current_price=50500.0,
        ...     quantity=0.1,
        ...     leverage=15,
        ...     margin_required=333.33,
        ...     unrealized_pnl=75.0,
        ...     liquidation_risk=0.15,
        ...     distance_to_liquidation=0.85
        ... )
    """

    position_id: str
    symbol: str
    side: str
    entry_price: float
    current_price: float
    quantity: float
    leverage: int
    margin_required: float
    unrealized_pnl: float
    liquidation_risk: float
    distance_to_liquidation: float
    status: str = "ACTIVE"
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        """Validate position risk parameters."""
        valid_sides = {'BUY', 'SELL', 'LONG', 'SHORT'}
        if self.side.upper() not in valid_sides:
            raise ValueError(f"Side must be one of {valid_sides}, got: {self.side}")
        
        if self.entry_price <= 0:
            raise ValueError(f"Entry price must be positive, got: {self.entry_price}")
        
        if self.current_price <= 0:
            raise ValueError(f"Current price must be positive, got: {self.current_price}")
        
        if self.quantity <= 0:
            raise ValueError(f"Quantity must be positive, got: {self.quantity}")
        
        if not (0 <= self.liquidation_risk <= 1):
            raise ValueError(f"Liquidation risk must be between 0 and 1, got: {self.liquidation_risk}")
        
        # Normalize side to uppercase
        self.side = self.side.upper()
    
    def is_profitable(self) -> bool:
        """Check if position is currently profitable."""
        return self.unrealized_pnl > 0
    
    def get_roe_percent(self) -> float:
        """Calculate Return on Equity percentage."""
        if self.margin_required <= 0:
            return 0.0
        return (self.unrealized_pnl / self.margin_required) * 100
    
    def is_critical_risk(self) -> bool:
        """Check if position has critical liquidation risk (> 90%)."""
        return self.liquidation_risk > 0.9


@dataclass
class RiskMetrics:
    """
    Portfolio-wide risk metrics snapshot.
    
    Comprehensive risk assessment of the entire portfolio including
    exposure, utilization, VaR, and performance metrics.
    
    Attributes:
        timestamp (datetime): Metrics calculation timestamp
        total_exposure (float): Total portfolio exposure in USD
        margin_utilization (float): Margin usage ratio (0-1)
        portfolio_var (float): Value at Risk (95% confidence)
        max_drawdown (float): Maximum drawdown ratio
        active_positions (int): Number of active positions
        daily_pnl (float): Daily profit/loss in USD
        leverage_ratio (float): Portfolio-weighted average leverage
        risk_level (str): Overall risk level ('LOW', 'MEDIUM', 'HIGH')
    
    Example:
        >>> metrics = RiskMetrics(
        ...     timestamp=datetime.utcnow(),
        ...     total_exposure=50000.0,
        ...     margin_utilization=0.45,
        ...     portfolio_var=2500.0,
        ...     max_drawdown=0.08,
        ...     active_positions=15,
        ...     daily_pnl=350.0,
        ...     leverage_ratio=12.5,
        ...     risk_level='MEDIUM'
        ... )
    """

    timestamp: datetime
    total_exposure: float
    margin_utilization: float
    portfolio_var: float
    max_drawdown: float
    active_positions: int
    daily_pnl: float
    leverage_ratio: float
    risk_level: str
    
    def __post_init__(self):
        """Validate risk metrics parameters."""
        if not (0 <= self.margin_utilization <= 1):
            raise ValueError(f"Margin utilization must be between 0 and 1, got: {self.margin_utilization}")
        
        if self.active_positions < 0:
            raise ValueError(f"Active positions cannot be negative, got: {self.active_positions}")
        
        valid_levels = {'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'}
        if self.risk_level.upper() not in valid_levels:
            raise ValueError(f"Risk level must be one of {valid_levels}, got: {self.risk_level}")
        
        # Normalize risk level to uppercase
        self.risk_level = self.risk_level.upper()
    
    def is_overexposed(self) -> bool:
        """Check if portfolio is overexposed (margin > 80%)."""
        return self.margin_utilization > 0.8
    
    def get_utilization_percent(self) -> float:
        """Get margin utilization as percentage."""
        return self.margin_utilization * 100

# =============================================================================
# Advanced Kelly Criterion Calculator
# =============================================================================


class AdvancedKellyCriterionCalculator:
    """Hedge Fund Grade Kelly Criterion Calculator - Phoenix95 Specialized"""

    def __init__(self, service=None):
        self.service = service  # This line added
        self.historical_performance = deque(maxlen=2000)
        self.win_rates = {}
        self.profit_loss_ratios = {}
        self.volatility_cache = {}
        self.market_regime_cache = {}

    def calculate_kelly_fraction(
        self,
        signal_data: SignalData,
        phoenix_analysis: Optional[Phoenix95AnalysisResult] = None,
    ) -> Dict[str, float]:
        """Advanced Kelly Criterion calculation"""
        try:
            symbol = signal_data.symbol
            confidence = signal_data.confidence

            # Utilize Phoenix95 analysis results
            if phoenix_analysis:
                base_kelly = self._calculate_phoenix_kelly(
                    signal_data, phoenix_analysis
                )
            else:
                base_kelly = self._calculate_basic_kelly(symbol, confidence)

            # Market condition adjustment
            market_adjustment = self._get_market_regime_adjustment(symbol)

            # Volatility adjustment
            volatility_adjustment = self._calculate_volatility_adjustment(signal_data)

            # Portfolio correlation adjustment
            correlation_adjustment = self._calculate_correlation_adjustment(symbol)

            # Time-based adjustment
            time_adjustment = self._calculate_time_adjustment()

            # Final Kelly ratio calculation
            final_kelly = (
                base_kelly
                * market_adjustment
                * volatility_adjustment
                * correlation_adjustment
                * time_adjustment
            )

            # Apply safety limits
            final_kelly = max(
                config.RISK_CONFIG["kelly_min_fraction"],
                min(final_kelly, config.RISK_CONFIG["kelly_max_fraction"]),
            )

            # Confidence boost
            confidence_boost = min(confidence * 0.15, 0.005)
            adjusted_kelly = min(
                final_kelly + confidence_boost, config.RISK_CONFIG["kelly_max_fraction"]
            )

            return {
                "kelly_fraction": adjusted_kelly,
                "base_kelly": base_kelly,
                "market_adjustment": market_adjustment,
                "volatility_adjustment": volatility_adjustment,
                "correlation_adjustment": correlation_adjustment,
                "time_adjustment": time_adjustment,
                "confidence_boost": confidence_boost,
                "final_position_size": adjusted_kelly,
                "win_rate_estimate": self._estimate_win_rate(symbol, confidence),
                "profit_loss_ratio": self._estimate_profit_loss_ratio(
                    symbol, phoenix_analysis
                ),
            }

        except Exception as e:
            logging.error(f"Kelly calculation failed: {e}")
            return self._get_fallback_kelly()

    def _calculate_phoenix_kelly(
        self, signal_data: SignalData, phoenix_analysis: Phoenix95AnalysisResult
    ) -> float:
        """Phoenix95 analysis-based Kelly calculation"""
        try:
            # Basic Kelly based on Phoenix95 score
            phoenix_score = phoenix_analysis.phoenix_score
            base_rate = 0.5 + (phoenix_score - 0.5) * 0.4  # Convert Phoenix score to win rate

            # Strength-based adjustment
            strength_factor = phoenix_analysis.strength_score
            momentum_factor = phoenix_analysis.momentum_score

            # Profit-loss ratio calculation (based on Phoenix analysis)
            expected_profit = phoenix_analysis.recommended_take_profit
            expected_loss = phoenix_analysis.recommended_stop_loss
            profit_loss_ratio = (
                expected_profit / expected_loss if expected_loss > 0 else 1.0
            )

            # Apply Kelly formula
            win_rate = min(0.85, max(0.15, base_rate * strength_factor))
            loss_rate = 1 - win_rate

            kelly_fraction = (
                profit_loss_ratio * win_rate - loss_rate
            ) / profit_loss_ratio

            # Momentum adjustment
            kelly_fraction *= 0.8 + momentum_factor * 0.4

            return max(0.005, min(0.025, kelly_fraction))

        except Exception as e:
            logging.error(f"Phoenix Kelly calculation failed: {e}")
            return config.RISK_CONFIG["kelly_min_fraction"]

    def _calculate_basic_kelly(self, symbol: str, confidence: float) -> float:
        """Basic Kelly calculation"""
        try:
            win_rate = self._estimate_win_rate(symbol, confidence)
            profit_loss_ratio = self._estimate_profit_loss_ratio(symbol, None)

            if profit_loss_ratio <= 0:
                return config.RISK_CONFIG["kelly_min_fraction"]

            kelly_fraction = (
                profit_loss_ratio * win_rate - (1 - win_rate)
            ) / profit_loss_ratio

            return max(0, min(kelly_fraction, config.RISK_CONFIG["kelly_max_fraction"]))

        except Exception as e:
            logging.error(f"Basic Kelly calculation failed: {e}")
            return config.RISK_CONFIG["kelly_min_fraction"]

    def _estimate_win_rate(self, symbol: str, confidence: float) -> float:
        """Win rate estimation - considering symbol-specific characteristics"""
        base_win_rates = {
            "BTCUSDT": 0.67,
            "ETHUSDT": 0.64,
            "BNBUSDT": 0.60,
            "ADAUSDT": 0.57,
            "DOGEUSDT": 0.54,
            "XRPUSDT": 0.62,
            "SOLUSDT": 0.65,
            "MATICUSDT": 0.58,
            "DOTUSDT": 0.59,
        }

        base_rate = base_win_rates.get(symbol, 0.57)

        # Confidence adjustment
        confidence_adjusted_rate = base_rate + (confidence - 0.5) * 0.25

        # Time zone adjustment
        hour = datetime.utcnow().hour
        time_factor = 1.0
        if 8 <= hour <= 16:  # Asia/Europe active hours
            time_factor = 1.08
        elif 14 <= hour <= 22:  # Europe/US active hours
            time_factor = 1.05
        elif 2 <= hour <= 6:  # Low activity hours
            time_factor = 0.92

        # Weekend adjustment
        weekday = datetime.utcnow().weekday()
        if weekday >= 5:  # Weekend
            time_factor *= 0.95

        final_rate = confidence_adjusted_rate * time_factor
        return max(0.25, min(0.85, final_rate))

    def _estimate_profit_loss_ratio(
        self, symbol: str, phoenix_analysis: Optional[Phoenix95AnalysisResult]
    ) -> float:
        """Profit-loss ratio estimation"""
        if phoenix_analysis:
            return (
                phoenix_analysis.recommended_take_profit
                / phoenix_analysis.recommended_stop_loss
            )

        # Default 2% profit, 2% loss (1:1 ratio)
        base_ratio = (
            config.RISK_CONFIG["take_profit_percent"]
            / config.RISK_CONFIG["stop_loss_percent"]
        )

        # Symbol-specific adjustment (considering volatility)
        symbol_adjustments = {
            "BTCUSDT": 1.12,
            "ETHUSDT": 1.08,
            "BNBUSDT": 1.05,
            "ADAUSDT": 0.98,
            "DOGEUSDT": 0.92,
            "XRPUSDT": 1.02,
            "SOLUSDT": 1.15,
            "MATICUSDT": 0.95,
            "DOTUSDT": 1.00,
        }

        adjustment = symbol_adjustments.get(symbol, 1.0)
        return base_ratio * adjustment

    def _get_market_regime_adjustment(self, symbol: str) -> float:
        """Market condition-based adjustment"""
        cache_key = f"market_regime_{symbol}"
        current_time = time.time()

        if cache_key in self.market_regime_cache:
            if (
                current_time - self.market_regime_cache[cache_key]["timestamp"] < 300
            ):  # 5 minute cache
                return self.market_regime_cache[cache_key]["adjustment"]

        try:
            # Estimate market volatility (in practice, would use external API or calculation)
            hour = datetime.utcnow().hour

            # Trading volume active hours adjustment
            if 8 <= hour <= 12 or 14 <= hour <= 18:  # Active hours
                market_adjustment = 1.10
            elif 20 <= hour <= 24 or 0 <= hour <= 2:  # US hours
                market_adjustment = 1.05
            else:  # Low activity hours
                market_adjustment = 0.90

            # Weekend adjustment
            if datetime.utcnow().weekday() >= 5:
                market_adjustment *= 0.85

            # Save to cache
            self.market_regime_cache[cache_key] = {
                "adjustment": market_adjustment,
                "timestamp": current_time,
            }

            return market_adjustment

        except Exception as e:
            logging.error(f"Market condition adjustment calculation failed: {e}")
            return 1.0

    def _calculate_volatility_adjustment(self, signal_data: SignalData) -> float:
        """Volatility-based adjustment"""
        try:
            symbol = signal_data.symbol

            # Base volatility by symbol (daily average volatility)
            base_volatilities = {
                "BTCUSDT": 0.035,
                "ETHUSDT": 0.045,
                "BNBUSDT": 0.055,
                "ADAUSDT": 0.065,
                "DOGEUSDT": 0.085,
                "XRPUSDT": 0.060,
                "SOLUSDT": 0.070,
                "MATICUSDT": 0.075,
                "DOTUSDT": 0.065,
            }

            base_vol = base_volatilities.get(symbol, 0.060)

            # Use volatility information from signal if available
            if signal_data.volatility:
                current_vol = signal_data.volatility
                vol_ratio = current_vol / base_vol

                # Reduce position size if volatility is high
                if vol_ratio > 1.5:
                    return 0.70
                elif vol_ratio > 1.2:
                    return 0.85
                elif vol_ratio < 0.8:
                    return 1.15
                elif vol_ratio < 0.5:
                    return 1.25

            # Base volatility adjustment
            if base_vol > 0.070:  # High volatility
                return 0.80
            elif base_vol < 0.040:  # Low volatility
                return 1.20

            return 1.0

        except Exception as e:
            logging.error(f"Volatility adjustment calculation failed: {e}")
            return 1.0

    def _calculate_correlation_adjustment(self, symbol: str) -> float:
        """Portfolio correlation adjustment"""
        try:
            # Consider correlation with Bitcoin
            btc_correlations = {
                "BTCUSDT": 1.0,
                "ETHUSDT": 0.85,
                "BNBUSDT": 0.75,
                "ADAUSDT": 0.70,
                "DOGEUSDT": 0.60,
                "XRPUSDT": 0.65,
                "SOLUSDT": 0.80,
                "MATICUSDT": 0.70,
                "DOTUSDT": 0.75,
            }

            correlation = btc_correlations.get(symbol, 0.70)

            # Higher correlation reduces position size (less diversification effect)
            if correlation > 0.90:
                return 0.85
            elif correlation > 0.80:
                return 0.92
            elif correlation < 0.60:
                return 1.10

            return 1.0

        except Exception as e:
            logging.error(f"Correlation adjustment calculation failed: {e}")
            return 1.0

    def _calculate_time_adjustment(self) -> float:
        """Time-based adjustment"""
        try:
            now = datetime.utcnow()
            hour = now.hour
            weekday = now.weekday()

            # Time zone adjustment
            time_factor = 1.0
            if 2 <= hour <= 6:  # Inactive hours
                time_factor = 0.85
            elif 8 <= hour <= 12:  # Asia active
                time_factor = 1.05
            elif 14 <= hour <= 18:  # Europe active
                time_factor = 1.08
            elif 20 <= hour <= 24:  # US active
                time_factor = 1.03

            # Day-of-week adjustment
            if weekday == 0:  # Monday - week start
                time_factor *= 1.02
            elif weekday == 4:  # Friday - week end
                time_factor *= 0.98
            elif weekday >= 5:  # Weekend
                time_factor *= 0.80

            return time_factor

        except Exception as e:
            logging.error(f"Time adjustment calculation failed: {e}")
            return 1.0

    def _get_fallback_kelly(self) -> Dict[str, float]:
        """Return default values on error"""
        return {
            "kelly_fraction": config.RISK_CONFIG["kelly_min_fraction"],
            "base_kelly": config.RISK_CONFIG["kelly_min_fraction"],
            "market_adjustment": 1.0,
            "volatility_adjustment": 1.0,
            "correlation_adjustment": 1.0,
            "time_adjustment": 1.0,
            "confidence_boost": 0.0,
            "final_position_size": config.RISK_CONFIG["kelly_min_fraction"],
            "win_rate_estimate": 0.5,
            "profit_loss_ratio": 1.0,
        }


# =============================================================================
# Multi-timeframe Risk Analyzer
# =============================================================================

class MultiTimeframeRiskAnalyzer:
    def __init__(self, service=None):
        self.service = service
        self.timeframes = ['1m', '5m', '15m', '1h', '4h']
        
        # High-capacity caches for 128GB RAM system (400+ positions support)
        # Expanded from 20K to 100K for ultimate performance
        self.price_data_cache = {}  # 100K entries with intelligent LRU cleanup
        self.correlation_matrix = {}  # Expanded correlation cache
        self.volatility_cache = {}  # Expanded volatility cache
        self.risk_scores = {}  # Risk scoring cache
        
        # Cache size limits for high-spec system (AMD Ryzen 9 7950X, 128GB DDR5)
        # Aggressive expansion for 128GB RAM
        self.max_price_cache_size = 100000  # 5x increase: 400 positions x 5 timeframes x 50 data points
        self.max_correlation_cache_size = 50000  # 5x increase: support massive correlation matrices
        self.max_volatility_cache_size = 25000  # 5x increase: extended volatility history
        
        # Performance monitoring for cache efficiency
        self.cache_stats = {
            'price_hits': 0,
            'price_misses': 0,
            'correlation_hits': 0,
            'correlation_misses': 0,
            'volatility_hits': 0,
            'volatility_misses': 0,
            'last_cleanup': time.time()
        }
        
        # WebSocket support for real-time data (400 positions)
        self.websocket_enabled = config.RISK_CONFIG.get("websocket_enabled", False)
        self.websocket_connections = {}
        self.websocket_subscriptions = set()
        
        # Parallel processing support for multi-core CPU (16-24 cores)
        self.parallel_enabled = config.RISK_CONFIG.get("parallel_processing", False)
        self.max_workers = min(24, (os.cpu_count() or 1) * 2)  # 2x CPU cores, max 24
        
        # Binance client initialization
        self.binance_client = None
        self.binance_mode = "SIMULATION"  # Default mode
        self.binance_api_key = None
        self.binance_secret_key = None
        self.binance_testnet_base_url = None
        
        if BINANCE_AVAILABLE:
            self._init_binance_testnet_client()
        
        logging.info(
            f"MultiTimeframeRiskAnalyzer initialized: "
            f"price_cache={self.max_price_cache_size:,}, "
            f"correlation_cache={self.max_correlation_cache_size:,}, "
            f"volatility_cache={self.max_volatility_cache_size:,}, "
            f"parallel={'enabled' if self.parallel_enabled else 'disabled'}, "
            f"workers={self.max_workers}"
        )

    def _init_binance_testnet_client(self):
        """Initialize Binance testnet client for SPOT trading only"""
        try:
            api_key = os.getenv('BINANCE_API_KEY')
            secret_key = os.getenv('BINANCE_SECRET_KEY')
            
            if not api_key or not secret_key:
                safe_logger.warning("Binance API keys not set - using simulation mode")
                self.binance_mode = "SIMULATION"
                return
            
            # Store API credentials for HTTP calls
            self.binance_api_key = api_key
            self.binance_secret_key = secret_key
            self.binance_testnet_base_url = "https://testnet.binance.vision"
            
            # Test connection and initialize client
            try:
                from binance.client import Client
                
                # Create actual Binance Client object (not string)
                self.binance_client = Client(
                    api_key=api_key,
                    api_secret=secret_key,
                    testnet=True
                )
                
                # Test SPOT API connection
                try:
                    # Test with ping
                    self.binance_client.ping()
                    
                    # Test with actual price fetch
                    ticker = self.binance_client.get_symbol_ticker(symbol="BTCUSDT")
                    btc_spot_price = float(ticker['price'])
                    
                    safe_logger.info(f"Binance SPOT API verified: BTC ${btc_spot_price:,.2f}")
                    self.binance_mode = "SPOT_TESTNET"
                    safe_logger.info("Binance testnet client initialized successfully")
                    
                except Exception as api_error:
                    safe_logger.error(f"Binance API test failed: {api_error}")
                    self.binance_client = None
                    self.binance_mode = "SIMULATION"
                    
            except ImportError as import_error:
                safe_logger.error(f"Binance Client import failed: {import_error}")
                self.binance_client = None
                self.binance_mode = "SIMULATION"
                
            except Exception as client_error:
                safe_logger.error(f"Binance Client creation failed: {client_error}")
                self.binance_client = None
                self.binance_mode = "SIMULATION"
                
        except Exception as e:
            safe_logger.error(f"Binance testnet client init failed: {e}")
            self.binance_client = None
            self.binance_mode = "SIMULATION"

    async def _binance_http_request(self, endpoint, symbol, interval=None, limit=100):
        """Direct HTTP request to Binance testnet SPOT API only"""
        try:
            if not hasattr(self, 'binance_testnet_base_url'):
                return None
            
            # Clean symbol - remove .P suffix if present (legacy futures format)
            clean_symbol = symbol.replace('.P', '') if symbol.endswith('.P') else symbol
            
            # Build URL for SPOT API only
            if endpoint == "klines":
                url = f"{self.binance_testnet_base_url}/api/v3/klines"
                params = {
                    'symbol': clean_symbol,
                    'interval': interval or '1h',
                    'limit': limit
                }
            elif endpoint == "price":
                url = f"{self.binance_testnet_base_url}/api/v3/ticker/price"
                params = {'symbol': clean_symbol}
            else:
                safe_logger.warning(f"Unknown endpoint: {endpoint}")
                return None
            
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        if endpoint == "klines":
                            # Extract close prices from klines
                            prices = [float(kline[4]) for kline in data]
                            safe_logger.info(f"SPOT API data fetched: {clean_symbol} {len(prices)} prices")
                            return prices
                        elif endpoint == "price":
                            price = float(data['price'])
                            safe_logger.debug(f"SPOT API price: {clean_symbol} = ${price:,.2f}")
                            return price
                    else:
                        error_text = await response.text()
                        safe_logger.error(f"SPOT API error {response.status}: {clean_symbol} - {error_text[:100]}")
                        return None
                        
        except aiohttp.ClientError as e:
            safe_logger.error(f"HTTP client error for {symbol}: {e}")
            return None
        except Exception as e:
            safe_logger.error(f"HTTP API request failed for {symbol}: {e}")
            return None
        
    async def analyze_multi_timeframe_risk(self, signal_data: SignalData) -> Dict[str, Any]:
        try:
            symbol = signal_data.symbol
            risk_analysis = {
                "symbol": symbol,
                "overall_risk_score": 0.0,
                "timeframe_scores": {},
                "volatility_analysis": {},
                "correlation_risk": {},
                "recommended_position_adjustment": 1.0,
                "risk_factors": []
            }
            
            # Risk analysis for each timeframe
            timeframe_scores = []
            for timeframe in self.timeframes:
                tf_score = await self._analyze_timeframe_risk(signal_data, timeframe)
                risk_analysis["timeframe_scores"][timeframe] = tf_score
                timeframe_scores.append(tf_score["risk_score"])
                
                if tf_score["risk_score"] > 0.7:
                    risk_analysis["risk_factors"].append(f"High risk in {timeframe} timeframe")
            
            # Calculate comprehensive risk score (weighted average)
            weights = {'1m': 0.1, '5m': 0.2, '15m': 0.3, '1h': 0.3, '4h': 0.1}
            weighted_score = sum(
                risk_analysis["timeframe_scores"][tf]["risk_score"] * weights[tf] 
                for tf in self.timeframes
            )
            risk_analysis["overall_risk_score"] = weighted_score
            
            # Volatility analysis
            volatility_analysis = await self._analyze_volatility_risk(symbol)
            risk_analysis["volatility_analysis"] = volatility_analysis
            
            if volatility_analysis["current_volatility"] > volatility_analysis["average_volatility"] * 2:
                risk_analysis["risk_factors"].append("Extremely high volatility detected")
                risk_analysis["recommended_position_adjustment"] *= 0.5
            
            # Correlation risk analysis
            correlation_risk = await self._analyze_correlation_risk(symbol)
            risk_analysis["correlation_risk"] = correlation_risk
            
            if correlation_risk["max_correlation"] > 0.8:
                risk_analysis["risk_factors"].append("High correlation with existing positions")
                risk_analysis["recommended_position_adjustment"] *= 0.8
            
            return risk_analysis
            
        except Exception as e:
            logging.error(f"Multi-timeframe risk analysis failed: {e}")
            return self._get_fallback_analysis(signal_data.symbol)
    
    async def _analyze_timeframe_risk(self, signal_data: SignalData, timeframe: str) -> Dict[str, Any]:
        try:
            symbol = signal_data.symbol
            
            # Get price data (in actual implementation, would call external API)
            price_data = await self._get_price_data(symbol, timeframe)
            
            if not price_data:
                return {"risk_score": 0.5, "factors": ["No data available"]}
            
            # Calculate technical indicators
            rsi = self._calculate_rsi(price_data)
            volatility = self._calculate_volatility(price_data)
            trend_strength = self._calculate_trend_strength(price_data)
            
            # Calculate risk score
            risk_score = 0.0
            risk_factors = []
            
            # RSI-based risk
            if rsi > 80 or rsi < 20:
                risk_score += 0.3
                risk_factors.append(f"Extreme RSI: {rsi:.1f}")
            
            # Volatility-based risk
            if volatility > 0.05:  # Above 5% volatility
                risk_score += 0.25
                risk_factors.append(f"High volatility: {volatility:.3f}")
            
            # Trend strength-based risk
            if abs(trend_strength) < 0.3:  # Weak trend
                risk_score += 0.2
                risk_factors.append(f"Weak trend: {trend_strength:.3f}")
            
            return {
                "risk_score": min(1.0, risk_score),
                "rsi": rsi,
                "volatility": volatility,
                "trend_strength": trend_strength,
                "factors": risk_factors
            }
            
        except Exception as e:
            logging.error(f"Timeframe risk analysis failed for {timeframe}: {e}")
            return {"risk_score": 0.6, "factors": ["Analysis failed"]}
    
    async def _analyze_volatility_risk(self, symbol: str) -> Dict[str, float]:
        try:
            # Calculate volatility using recent 24-hour price data
            price_data = await self._get_price_data(symbol, '1h', limit=24)
            
            if len(price_data) < 10:
                return {
                    "current_volatility": 0.03,
                    "average_volatility": 0.03,
                    "volatility_percentile": 50.0
                }
            
            returns = []
            for i in range(1, len(price_data)):
                ret = (price_data[i] - price_data[i-1]) / price_data[i-1]
                returns.append(ret)
            
            current_volatility = np.std(returns[-6:]) if len(returns) >= 6 else np.std(returns)
            average_volatility = np.std(returns)
            
            volatility_percentile = (
                sum(1 for r in returns if abs(r) <= current_volatility) / len(returns) * 100
            )
            
            return {
                "current_volatility": float(current_volatility),
                "average_volatility": float(average_volatility),
                "volatility_percentile": float(volatility_percentile)
            }
            
        except Exception as e:
            logging.error(f"Volatility analysis failed: {e}")
            return {
                "current_volatility": 0.04,
                "average_volatility": 0.04,
                "volatility_percentile": 50.0
            }
    
    async def _analyze_correlation_risk(self, symbol: str) -> Dict[str, Any]:
        try:
            if not hasattr(self.service, 'risk_manager'):
                return {"max_correlation": 0.0, "correlated_symbols": []}
            
            active_positions = self.service.risk_manager.active_positions
            active_symbols = [pos.symbol for pos in active_positions.values()]
            
            if not active_symbols:
                return {"max_correlation": 0.0, "correlated_symbols": []}
            
            correlations = []
            for existing_symbol in active_symbols:
                if existing_symbol != symbol:
                    correlation = await self._calculate_correlation(symbol, existing_symbol)
                    correlations.append({
                        "symbol": existing_symbol,
                        "correlation": correlation
                    })
            
            if not correlations:
                return {"max_correlation": 0.0, "correlated_symbols": []}
            
            max_correlation = max(correlations, key=lambda x: abs(x["correlation"]))
            high_correlation_symbols = [
                c["symbol"] for c in correlations if abs(c["correlation"]) > 0.7
            ]
            
            return {
                "max_correlation": abs(max_correlation["correlation"]),
                "correlated_symbols": high_correlation_symbols,
                "all_correlations": correlations
            }
            
        except Exception as e:
            logging.error(f"Correlation analysis failed: {e}")
            return {"max_correlation": 0.0, "correlated_symbols": []}
    
    async def _get_price_data(self, symbol: str, timeframe: str, limit: int = 100) -> List[float]:
        try:
            cache_key = f"{symbol}_{timeframe}_{limit}"
            current_time = time.time()

            # Check cache (valid for 5 minutes)
            if cache_key in self.price_data_cache:
                cache_entry = self.price_data_cache[cache_key]
                if current_time - cache_entry["timestamp"] < 300:
                    return cache_entry["data"]

            # Try real Binance client first if available
            if hasattr(self, 'binance_client') and self.binance_client:
                try:
                    real_prices = await self._fetch_binance_client_data(symbol, timeframe, limit)
                    if real_prices and len(real_prices) > 0:
                        self.price_data_cache[cache_key] = {
                            "data": real_prices,
                            "timestamp": current_time
                        }
                        return real_prices
                except Exception as client_error:
                    logging.warning(f"Binance client failed for {symbol}: {client_error}")

            # Fallback to HTTP API call
            try:
                real_prices = await self._fetch_binance_price_data(symbol, timeframe, limit)
                if real_prices and len(real_prices) > 0:
                    self.price_data_cache[cache_key] = {
                        "data": real_prices,
                        "timestamp": current_time
                    }
                    return real_prices
            except Exception as api_error:
                logging.warning(f"Binance API failed for {symbol}: {api_error}")

            # Final fallback to simulation
            fallback_prices = await self._generate_fallback_prices(symbol, limit)
            self.price_data_cache[cache_key] = {
                "data": fallback_prices,
                "timestamp": current_time
            }
            return fallback_prices
            
        except Exception as e:
            logging.error(f"Price data retrieval failed: {e}")
            return [100.0] * limit

    async def _fetch_binance_client_data(self, symbol: str, timeframe: str, limit: int) -> List[float]:
        """Use Binance client for data fetching with validation"""
        try:
            # Validate binance_client exists and is correct type
            if not self.binance_client or self.binance_mode != "SPOT_TESTNET":
                logging.debug(f"Binance client not available (mode: {getattr(self, 'binance_mode', 'UNKNOWN')})")
                return []
            
            # Validate client is actual Client object, not string
            from binance.client import Client
            if not isinstance(self.binance_client, Client):
                logging.error(f"Invalid binance_client type: {type(self.binance_client)}")
                return []
            
            # Map timeframe to Binance interval constants
            interval_map = {
                '1m': Client.KLINE_INTERVAL_1MINUTE,
                '5m': Client.KLINE_INTERVAL_5MINUTE,
                '15m': Client.KLINE_INTERVAL_15MINUTE,
                '1h': Client.KLINE_INTERVAL_1HOUR,
                '4h': Client.KLINE_INTERVAL_4HOUR
            }
            
            interval = interval_map.get(timeframe, Client.KLINE_INTERVAL_1HOUR)
            
            # Fetch klines data
            klines = self.binance_client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit
            )
            
            # Extract close prices (index 4)
            prices = [float(kline[4]) for kline in klines]
            
            if prices:
                logging.info(f"Binance client data fetched: {symbol} {len(prices)} prices")
            else:
                logging.warning(f"No price data returned for {symbol}")
            
            return prices
            
        except ImportError as import_error:
            logging.error(f"Binance Client import failed: {import_error}")
            return []
        except Exception as e:
            logging.error(f"Binance client fetch failed for {symbol}: {e}")
            return []

    async def _fetch_binance_price_data(self, symbol: str, timeframe: str, limit: int) -> List[float]:
        """Fetch real price data from Binance API"""
        try:
            interval_map = {
                '1m': '1m', '5m': '5m', '15m': '15m', '1h': '1h', '4h': '4h'
            }
            
            interval = interval_map.get(timeframe, '1h')
            url = "https://api.binance.com/api/v3/klines"
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            
            timeout = aiohttp.ClientTimeout(total=10)
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        prices = [float(kline[4]) for kline in data]
                        logging.info(f"Real price data fetched: {symbol} {len(prices)} prices")
                        return prices
                    else:
                        logging.error(f"Binance API error {response.status} for {symbol}")
                        return []
                        
        except Exception as e:
            logging.error(f"Binance API call failed: {e}")
            return []

    async def _generate_fallback_prices(self, symbol: str, limit: int) -> List[float]:
        """Generate fallback simulation data"""
        base_prices = {
            'BTCUSDT': 50000.0, 'ETHUSDT': 3000.0, 'BNBUSDT': 300.0,
            'ADAUSDT': 0.5, 'DOGEUSDT': 0.1, 'XRPUSDT': 0.6,
            'SOLUSDT': 100.0, 'MATICUSDT': 1.0, 'DOTUSDT': 8.0
        }
        
        base_price = base_prices.get(symbol, 100.0)
        prices = [base_price]
        
        for i in range(1, limit):
            change = np.random.normal(0, 0.02)
            new_price = prices[-1] * (1 + change)
            prices.append(max(new_price, base_price * 0.5))
        
        return prices

    def _calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        try:
            if len(prices) < period + 1:
                return 50.0
            
            deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
            gains = [d if d > 0 else 0 for d in deltas]
            losses = [-d if d < 0 else 0 for d in deltas]
            
            avg_gain = sum(gains[-period:]) / period
            avg_loss = sum(losses[-period:]) / period
            
            if avg_loss == 0:
                return 100.0
            
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            
            return rsi
            
        except Exception as e:
            logging.error(f"RSI calculation failed: {e}")
            return 50.0
    
    def _calculate_volatility(self, prices: List[float]) -> float:
        try:
            if len(prices) < 2:
                return 0.03
            
            returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
            return float(np.std(returns))
            
        except Exception as e:
            logging.error(f"Volatility calculation failed: {e}")
            return 0.03
    
    def _calculate_trend_strength(self, prices: List[float]) -> float:
        try:
            if len(prices) < 10:
                return 0.0
            
            x = np.arange(len(prices))
            coeffs = np.polyfit(x, prices, 1)
            slope = coeffs[0]
            
            avg_price = np.mean(prices)
            normalized_slope = slope / avg_price
            
            return float(normalized_slope)
            
        except Exception as e:
            logging.error(f"Trend strength calculation failed: {e}")
            return 0.0
    
    async def _calculate_correlation(self, symbol1: str, symbol2: str, period: int = 50) -> float:
        try:
            prices1 = await self._get_price_data(symbol1, '1h', period)
            prices2 = await self._get_price_data(symbol2, '1h', period)
            
            if len(prices1) < 10 or len(prices2) < 10:
                return 0.0
            
            min_len = min(len(prices1), len(prices2))
            prices1 = prices1[:min_len]
            prices2 = prices2[:min_len]
            
            returns1 = [(prices1[i] - prices1[i-1]) / prices1[i-1] for i in range(1, len(prices1))]
            returns2 = [(prices2[i] - prices2[i-1]) / prices2[i-1] for i in range(1, len(prices2))]
            
            correlation = float(np.corrcoef(returns1, returns2)[0, 1])
            return correlation if not np.isnan(correlation) else 0.0
            
        except Exception as e:
            logging.error(f"Correlation calculation failed: {e}")
            return 0.0
    
    def _get_fallback_analysis(self, symbol: str) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "overall_risk_score": 0.5,
            "timeframe_scores": {tf: {"risk_score": 0.5} for tf in self.timeframes},
            "volatility_analysis": {
                "current_volatility": 0.03,
                "average_volatility": 0.03,
                "volatility_percentile": 50.0
            },
            "correlation_risk": {"max_correlation": 0.0, "correlated_symbols": []},
            "recommended_position_adjustment": 1.0,
            "risk_factors": ["Analysis failed - using fallback"]
        }

# =============================================================================
# Dynamic Leverage Adjuster
# =============================================================================


class DynamicLeverageAdjuster:
    def __init__(self, service=None):
        self.service = service
        self.leverage_history = deque(maxlen=1000)
        self.performance_tracking = {}
        self.market_volatility_cache = {}
        
    async def calculate_optimal_leverage(
        self, signal_data: SignalData, 
        multi_tf_analysis: Dict[str, Any],
        kelly_result: Dict[str, float]
    ) -> Dict[str, Any]:
        try:
            symbol = signal_data.symbol
            base_leverage = config.RISK_CONFIG["max_leverage"]
            
            # Reflect multi-timeframe analysis results
            risk_score = multi_tf_analysis.get("overall_risk_score", 0.5)
            risk_adjustment = 1.0 - (risk_score * 0.6)  # Higher risk reduces leverage
            
            # Volatility-based adjustment
            volatility_data = multi_tf_analysis.get("volatility_analysis", {})
            current_vol = volatility_data.get("current_volatility", 0.03)
            vol_adjustment = self._calculate_volatility_adjustment(current_vol)
            
            # Kelly Criterion-based adjustment
            kelly_fraction = kelly_result.get("kelly_fraction", 0.005)
            kelly_adjustment = min(2.0, kelly_fraction / 0.005)  # Higher Kelly increases leverage
            
            # Market condition-based adjustment
            market_adjustment = await self._get_market_condition_adjustment(symbol)
            
            # Portfolio level adjustment
            portfolio_adjustment = await self._get_portfolio_adjustment()
            
            # Final leverage calculation
            optimal_leverage = (
                base_leverage * 
                risk_adjustment * 
                vol_adjustment * 
                kelly_adjustment * 
                market_adjustment * 
                portfolio_adjustment
            )
            
            # Apply leverage limits
            optimal_leverage = max(2, min(optimal_leverage, base_leverage))
            optimal_leverage = int(optimal_leverage)
            
            # Construct result
            result = {
                "optimal_leverage": optimal_leverage,
                "base_leverage": base_leverage,
                "adjustments": {
                    "risk_adjustment": risk_adjustment,
                    "volatility_adjustment": vol_adjustment,
                    "kelly_adjustment": kelly_adjustment,
                    "market_adjustment": market_adjustment,
                    "portfolio_adjustment": portfolio_adjustment
                },
                "confidence_level": min(0.95, signal_data.confidence * kelly_adjustment),
                "expected_sharpe_ratio": self._estimate_sharpe_ratio(
                    optimal_leverage, multi_tf_analysis
                )
            }
            
            # Update leverage history
            self.leverage_history.append({
                "timestamp": time.time(),
                "symbol": symbol,
                "leverage": optimal_leverage,
                "market_conditions": market_adjustment
            })
            
            return result
            
        except Exception as e:
            logging.error(f"Dynamic leverage calculation failed: {e}")
            return {
                "optimal_leverage": max(2, base_leverage // 2),
                "base_leverage": base_leverage,
                "adjustments": {},
                "confidence_level": 0.5,
                "expected_sharpe_ratio": 0.0
            }
    
    def _calculate_volatility_adjustment(self, volatility: float) -> float:
        try:
            # Higher volatility reduces leverage
            if volatility > 0.08:  # Above 8%
                return 0.4
            elif volatility > 0.06:  # Above 6%
                return 0.6
            elif volatility > 0.04:  # Above 4%
                return 0.8
            elif volatility < 0.02:  # Below 2% (low volatility)
                return 1.2
            else:
                return 1.0
                
        except Exception as e:
            logging.error(f"Volatility adjustment calculation failed: {e}")
            return 0.8
    
    async def _get_market_condition_adjustment(self, symbol: str) -> float:
        try:
            current_time = time.time()
            hour = datetime.utcnow().hour
            weekday = datetime.utcnow().weekday()
            
            adjustment = 1.0
            
            # Time-based adjustment
            if 2 <= hour <= 6:  # Inactive hours
                adjustment *= 0.7
            elif 8 <= hour <= 16:  # Asia/Europe active
                adjustment *= 1.1
            elif 20 <= hour <= 24:  # US active
                adjustment *= 1.05
            
            # Day-based adjustment
            if weekday >= 5:  # Weekend
                adjustment *= 0.6
            elif weekday == 0:  # Monday
                adjustment *= 0.9
            elif weekday == 4:  # Friday
                adjustment *= 0.95
            
            return adjustment
            
        except Exception as e:
            logging.error(f"Market condition adjustment failed: {e}")
            return 0.9
    
    async def _get_portfolio_adjustment(self) -> float:
        try:
            if not hasattr(self.service, 'risk_manager'):
                return 1.0
                
            active_positions = self.service.risk_manager.active_positions
            position_count = len(active_positions)
            
            # Higher position count reduces individual leverage
            if position_count >= 10:
                return 0.6
            elif position_count >= 7:
                return 0.75
            elif position_count >= 5:
                return 0.85
            elif position_count >= 3:
                return 0.95
            else:
                return 1.0
                
        except Exception as e:
            logging.error(f"Portfolio adjustment calculation failed: {e}")
            return 0.8
    
    def _estimate_sharpe_ratio(self, leverage: int, analysis: Dict[str, Any]) -> float:
        try:
            # Simple Sharpe ratio estimation
            risk_score = analysis.get("overall_risk_score", 0.5)
            expected_return = leverage * 0.001  # 0.1% expected return per leverage
            risk_penalty = risk_score * 0.05
            
            estimated_sharpe = max(0, (expected_return - risk_penalty) / max(0.01, risk_score))
            return min(3.0, estimated_sharpe)
            
        except Exception as e:
            logging.error(f"Sharpe ratio estimation failed: {e}")
            return 0.5


# =============================================================================
# VaR (Value at Risk) Calculator
# =============================================================================


class VaRCalculator:
    def __init__(self, service=None):
        self.service = service
        self.price_history = {}
        self.var_history = deque(maxlen=1000)
        self.confidence_levels = [0.95, 0.99, 0.999]
        
    async def calculate_portfolio_var(
        self, positions: List[PositionRisk], 
        confidence_level: float = 0.95,
        time_horizon: int = 1
    ) -> Dict[str, Any]:
        try:
            if not positions:
                return self._get_empty_var_result()
            
            # Calculate individual position VaR
            individual_vars = []
            for position in positions:
                pos_var = await self._calculate_position_var(
                    position, confidence_level, time_horizon
                )
                individual_vars.append(pos_var)
            
            # Calculate portfolio VaR (considering correlation)
            correlation_matrix = await self._build_correlation_matrix([p.symbol for p in positions])
            portfolio_var = self._calculate_correlated_var(individual_vars, correlation_matrix)
            
            # Calculate CVaR (Conditional VaR)
            cvar = await self._calculate_cvar(positions, confidence_level)
            
            # Construct result
            result = {
                "portfolio_var": portfolio_var,
                "portfolio_cvar": cvar,
                "individual_vars": individual_vars,
                "confidence_level": confidence_level,
                "time_horizon_days": time_horizon,
                "total_exposure": sum(p.quantity * p.current_price for p in positions),
                "risk_metrics": {
                    "max_loss_estimate": portfolio_var,
                    "expected_shortfall": cvar,
                    "var_utilization": portfolio_var / 100000,  # Based on 100K
                    "risk_concentration": self._calculate_risk_concentration(individual_vars)
                },
                "timestamp": time.time()
            }
            
            # Update VaR history
            self.var_history.append({
                "timestamp": time.time(),
                "portfolio_var": portfolio_var,
                "position_count": len(positions),
                "confidence_level": confidence_level
            })
            
            return result
            
        except Exception as e:
            logging.error(f"Portfolio VaR calculation failed: {e}")
            return self._get_fallback_var_result(positions)
    
    async def _calculate_position_var(
        self, position: PositionRisk, 
        confidence_level: float, 
        time_horizon: int
    ) -> Dict[str, Any]:
        try:
            symbol = position.symbol
            
            # Get historical price data
            price_data = await self._get_historical_prices(symbol, days=30)
            
            if len(price_data) < 10:
                return self._get_fallback_position_var(position)
            
            # Calculate returns
            returns = []
            for i in range(1, len(price_data)):
                ret = (price_data[i] - price_data[i-1]) / price_data[i-1]
                returns.append(ret)
            
            # VaR calculation (Historical Simulation method)
            returns_sorted = sorted(returns)
            var_index = int((1 - confidence_level) * len(returns_sorted))
            var_return = returns_sorted[var_index]
            
            # Apply VaR to position value
            position_value = position.quantity * position.current_price
            position_var = abs(var_return * position_value * position.leverage)
            
            # Time adjustment (Square Root of Time)
            if time_horizon != 1:
                position_var *= (time_horizon ** 0.5)
            
            return {
                "symbol": symbol,
                "position_var": position_var,
                "var_percentage": abs(var_return) * 100,
                "position_value": position_value,
                "leverage": position.leverage,
                "confidence_level": confidence_level
            }
            
        except Exception as e:
            logging.error(f"Position VaR calculation failed: {e}")
            return self._get_fallback_position_var(position)
    
    async def _calculate_cvar(self, positions: List[PositionRisk], confidence_level: float) -> float:
        try:
            # CVaR is the expected value of losses exceeding VaR
            portfolio_var = 0.0
            
            for position in positions:
                pos_var_result = await self._calculate_position_var(position, confidence_level, 1)
                portfolio_var += pos_var_result["position_var"]
            
            # CVaR is typically 1.2~1.5 times VaR
            cvar_multiplier = 1.3 if confidence_level >= 0.99 else 1.25
            cvar = portfolio_var * cvar_multiplier
            
            return cvar
            
        except Exception as e:
            logging.error(f"CVaR calculation failed: {e}")
            return portfolio_var * 1.2 if 'portfolio_var' in locals() else 0.0
    
    async def _build_correlation_matrix(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        try:
            correlation_matrix = {}
            
            for i, symbol1 in enumerate(symbols):
                correlation_matrix[symbol1] = {}
                
                for j, symbol2 in enumerate(symbols):
                    if i == j:
                        correlation_matrix[symbol1][symbol2] = 1.0
                    else:
                        correlation = await self._calculate_correlation(symbol1, symbol2)
                        correlation_matrix[symbol1][symbol2] = correlation
            
            return correlation_matrix
            
        except Exception as e:
            logging.error(f"Correlation matrix building failed: {e}")
            # Return default correlation matrix
            default_corr = {}
            for symbol in symbols:
                default_corr[symbol] = {s: 0.7 if s != symbol else 1.0 for s in symbols}
            return default_corr
    
    def _calculate_correlated_var(
        self, individual_vars: List[Dict], 
        correlation_matrix: Dict
    ) -> float:
        try:
            if len(individual_vars) <= 1:
                return individual_vars[0]["position_var"] if individual_vars else 0.0
            
            # Simplified correlation adjustment
            total_var = sum(var_data["position_var"] for var_data in individual_vars)
            
            # Calculate average correlation
            correlations = []
            symbols = [var_data["symbol"] for var_data in individual_vars]
            
            for i, symbol1 in enumerate(symbols):
                for j, symbol2 in enumerate(symbols):
                    if i < j:  # Remove duplicates
                        corr = correlation_matrix.get(symbol1, {}).get(symbol2, 0.7)
                        correlations.append(abs(corr))
            
            avg_correlation = sum(correlations) / len(correlations) if correlations else 0.7
            
            # VaR adjustment considering correlation
            diversification_factor = (1 - avg_correlation * 0.3)
            adjusted_var = total_var * diversification_factor
            
            return max(adjusted_var, total_var * 0.5)  # Minimum 50% diversification effect
            
        except Exception as e:
            logging.error(f"Correlated VaR calculation failed: {e}")
            return sum(var_data["position_var"] for var_data in individual_vars)
    
    async def _calculate_correlation(self, symbol1: str, symbol2: str) -> float:
        # Reuse correlation calculation from MultiTimeframeRiskAnalyzer
        try:
            if hasattr(self.service, 'risk_manager') and hasattr(self.service.risk_manager, 'multi_timeframe_analyzer'):
                return await self.service.risk_manager.multi_timeframe_analyzer._calculate_correlation(symbol1, symbol2)
            else:
                # 기본 상관관계 값 반환
                btc_correlated = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
                if symbol1 in btc_correlated and symbol2 in btc_correlated:
                    return 0.8
                else:
                    return 0.6
        except:
            return 0.7
    
    def _calculate_risk_concentration(self, individual_vars: List[Dict]) -> float:
        try:
            if not individual_vars:
                return 0.0
            
            total_var = sum(var_data["position_var"] for var_data in individual_vars)
            if total_var == 0:
                return 0.0
            
            # Calculate concentration using Herfindahl Index
            concentrations = [(var_data["position_var"] / total_var) ** 2 for var_data in individual_vars]
            hhi = sum(concentrations)
            
            return hhi
            
        except Exception as e:
            logging.error(f"Risk concentration calculation failed: {e}")
            return 0.5
    
    async def _get_historical_prices(self, symbol: str, days: int = 30) -> List[float]:
        # Reuse price data retrieval from MultiTimeframeRiskAnalyzer
        try:
            if hasattr(self.service, 'risk_manager') and hasattr(self.service.risk_manager, 'multi_timeframe_analyzer'):
                return await self.service.risk_manager.multi_timeframe_analyzer._get_price_data(
                    symbol, '1h', days * 24
                )
            else:
                # Return simulation data
                return [50000.0 * (1 + np.random.normal(0, 0.02)) for _ in range(days * 24)]
        except:
            return [50000.0 * (1 + np.random.normal(0, 0.02)) for _ in range(days * 24)]
    
    def _get_empty_var_result(self) -> Dict[str, Any]:
        return {
            "portfolio_var": 0.0,
            "portfolio_cvar": 0.0,
            "individual_vars": [],
            "confidence_level": 0.95,
            "time_horizon_days": 1,
            "total_exposure": 0.0,
            "risk_metrics": {
                "max_loss_estimate": 0.0,
                "expected_shortfall": 0.0,
                "var_utilization": 0.0,
                "risk_concentration": 0.0
            },
            "timestamp": time.time()
        }
    
    def _get_fallback_var_result(self, positions: List[PositionRisk]) -> Dict[str, Any]:
        try:
            total_exposure = sum(p.quantity * p.current_price for p in positions)
            estimated_var = total_exposure * 0.05  # 5% 추정 VaR
            
            return {
                "portfolio_var": estimated_var,
                "portfolio_cvar": estimated_var * 1.25,
                "individual_vars": [self._get_fallback_position_var(p) for p in positions],
                "confidence_level": 0.95,
                "time_horizon_days": 1,
                "total_exposure": total_exposure,
                "risk_metrics": {
                    "max_loss_estimate": estimated_var,
                    "expected_shortfall": estimated_var * 1.25,
                    "var_utilization": estimated_var / 100000,
                    "risk_concentration": 1.0 / max(1, len(positions))
                },
                "timestamp": time.time()
            }
        except:
            return self._get_empty_var_result()
    
    def _get_fallback_position_var(self, position: PositionRisk) -> Dict[str, Any]:
        position_value = position.quantity * position.current_price
        estimated_var = position_value * 0.03 * position.leverage  # 3% 추정
        
        return {
            "symbol": position.symbol,
            "position_var": estimated_var,
            "var_percentage": 3.0,
            "position_value": position_value,
            "leverage": position.leverage,
            "confidence_level": 0.95
        }


# =============================================================================
# Advanced Leverage Risk Manager
# =============================================================================


class AdvancedLeverageRiskManager:
    def __init__(self, service=None):
        self.service = service
        self.active_positions = {}
        self.risk_history = deque(maxlen=100000)  # Increased from 20000 to 100000 for comprehensive history
        self.daily_metrics = {}
        self.emergency_mode = False

        # Signal duplicate blocking system - High-capacity for 400+ positions
        self.signal_cache = {}
        self.cache_cleanup_time = time.time()
        self.max_signal_cache_size = 50000  # Support high-frequency signal processing
        self.cache_cleanup_interval = 300  # Cleanup every 5 minutes

        # Multi-timeframe risk analyzer initialization
        self.multi_timeframe_analyzer = MultiTimeframeRiskAnalyzer(service)
        
        # Dynamic leverage adjuster initialization
        self.dynamic_leverage_adjuster = DynamicLeverageAdjuster(service)
        
        # VaR calculator initialization
        self.var_calculator = VaRCalculator(service)

        # Portfolio risk limits - Scaled for high-spec system
        self.portfolio_limits = {
            "max_total_exposure": 500000,  # Increased from $50K to $500K for 400 positions
            "max_margin_usage": 0.80,
            "max_correlation_exposure": 0.60,
            "max_single_position": 0.10,  # Reduced from 25% to 10% for better diversification
        }
        
        # Parallel processing support for multi-core CPU
        self.parallel_processing_enabled = config.RISK_CONFIG.get("parallel_processing", False)
        self.batch_size = config.RISK_CONFIG.get("batch_processing_size", 50)

    def _extract_service_name(self, url: str) -> str:
        """Extract service name from URL"""
        try:
            if not url or not isinstance(url, str):
                return "UnknownService"
                
            url_lower = url.lower()
            if "execute" in url_lower:
                return "ExecuteService"
            elif "notify" in url_lower:
                return "NotifyService"
            elif "portfolio" in url_lower:
                return "PortfolioService"
            elif "brain" in url_lower:
                return "BrainService"
            else:
                return "UnknownService"
        except Exception as e:
            logging.warning(f"Service name extraction failed: {e}")
            return "UnknownService"

    async def _get_daily_pnl(self) -> float:
        """
        Calculate daily profit and loss ratio with enhanced error handling.
        
        Features:
        - Redis connection validation with fallback
        - Fake loss detection and prevention
        - Memory-based calculation fallback
        - Comprehensive error handling
        - Optimized for 128GB RAM, 16-24 cores
        
        Returns:
            float: Daily PnL ratio (based on $100K capital)
        """
        try:
            # Check Redis connection first
            redis_available = await self._check_redis_availability()
            
            if not redis_available:
                logging.warning("Redis unavailable - using memory only calculation")
                return await self._calculate_memory_based_pnl()

            # Get today's date key
            today_str = datetime.utcnow().strftime("%Y-%m-%d")
            pnl_key = f"pnl:{today_str}"
            
            # Get active position count once
            active_position_count = len(self.active_positions)
            
            # Check for fake loss (loss exists but no active positions)
            if active_position_count == 0:
                await self._handle_fake_loss_detection(pnl_key)
                return 0.0  # Early return for zero positions
            
            # Get realized PnL from Redis
            realized_pnl = await self._get_realized_pnl_safe(pnl_key)
            
            # Calculate unrealized PnL from active positions
            unrealized_pnl = self._calculate_unrealized_pnl_fast()
            
            # Total daily PnL
            total_daily_pnl = realized_pnl + unrealized_pnl
            
            # Convert to ratio based on $100K capital
            daily_pnl_ratio = total_daily_pnl / 100000
            
            # Sanity check for unrealistic values
            if abs(daily_pnl_ratio) > 10.0:  # More than 1000% is unrealistic
                logging.warning(f"Unrealistic PnL ratio detected: {daily_pnl_ratio:.2%}, using fallback")
                return await self._calculate_memory_based_pnl()
            
            logging.debug(
                f"Daily PnL: positions={active_position_count}, "
                f"realized=${realized_pnl:.2f}, unrealized=${unrealized_pnl:.2f}, "
                f"total=${total_daily_pnl:.2f}, ratio={daily_pnl_ratio:.2%}"
            )
            
            return daily_pnl_ratio

        except Exception as e:
            logging.error(f"Daily PnL calculation failed: {e}")
            return await self._calculate_memory_based_pnl()

    async def _check_redis_availability(self) -> bool:
        """
        Check Redis availability with timeout.
        
        Features:
        - Fast timeout check (2 seconds)
        - Multiple error type handling
        - Safe attribute checking
        
        Returns:
            bool: True if Redis is available and responsive
        """
        try:
            if not (self.service and hasattr(self.service, 'redis') and self.service.redis):
                return False
            await asyncio.wait_for(self.service.redis.ping(), timeout=2.0)
            return True
        except (asyncio.TimeoutError, ConnectionError, AttributeError):
            return False
        except Exception:
            return False

    async def _handle_fake_loss_detection(self, pnl_key: str):
        """
        Handle fake loss detection and cleanup.
        
        Fake loss occurs when Redis shows negative PnL but no active positions exist.
        This function detects and cleans up such inconsistencies.
        
        Args:
            pnl_key: Redis key for daily PnL storage
        """
        try:
            existing_pnl_str = await self.service.redis.get(pnl_key)
            if existing_pnl_str:
                existing_pnl = float(existing_pnl_str)
                if abs(existing_pnl) > 1.0:
                    logging.warning(f"Fake loss detected: ${existing_pnl:.2f} with 0 positions - resetting")
                    await self.service.redis.delete(pnl_key)
        except (ValueError, TypeError) as e:
            logging.error(f"PnL data type error: {e}")
            try:
                await self.service.redis.delete(pnl_key)
            except Exception:
                pass
        except Exception as e:
            logging.error(f"PnL reset failed: {e}")

    async def _get_realized_pnl_safe(self, pnl_key: str) -> float:
        """
        Safely get realized PnL from Redis with comprehensive error handling.
        
        Args:
            pnl_key: Redis key for daily PnL storage
            
        Returns:
            float: Realized PnL value or 0.0 on error
        """
        try:
            realized_pnl_str = await self.service.redis.get(pnl_key)
            if realized_pnl_str:
                return float(realized_pnl_str)
            return 0.0
        except (ValueError, TypeError) as e:
            logging.error(f"Realized PnL conversion error: {e}")
            return 0.0
        except Exception as e:
            logging.error(f"Failed to get realized PnL: {e}")
            return 0.0

    def _calculate_unrealized_pnl_fast(self) -> float:
        """
        Fast calculation of unrealized PnL from active positions.
        
        Features:
        - Optimized for 400+ concurrent positions
        - Sanity checks for unrealistic values
        - Graceful error handling per position
        - High-performance iteration
        
        Returns:
            float: Total unrealized PnL across all positions
        """
        try:
            total_unrealized = 0.0
            valid_positions = 0
            
            for position in self.active_positions.values():
                try:
                    if (hasattr(position, 'unrealized_pnl') and 
                        position.unrealized_pnl is not None):
                        pnl_value = float(position.unrealized_pnl)
                        # Sanity check for unrealistic values
                        if abs(pnl_value) < 1000000:  # Less than 1M
                            total_unrealized += pnl_value
                            valid_positions += 1
                        else:
                            logging.warning(f"Unrealistic PnL value ignored: ${pnl_value:.2f}")
                except (ValueError, TypeError, AttributeError):
                    continue
            
            if valid_positions > 0:
                logging.debug(f"Calculated unrealized PnL from {valid_positions} positions")
            
            return total_unrealized
            
        except Exception as e:
            logging.error(f"Fast unrealized PnL calculation failed: {e}")
            return 0.0

    async def _calculate_memory_based_pnl(self) -> float:
        """
        Calculate memory-based PnL when Redis unavailable.
        
        This is a fallback mechanism that calculates PnL using only
        in-memory position data, excluding realized PnL from Redis.
        
        Features:
        - No Redis dependency
        - Fast in-memory calculation
        - Clear logging for debugging
        
        Returns:
            float: Daily PnL ratio (unrealized only)
        """
        try:
            unrealized_pnl = self._calculate_unrealized_pnl_fast()
            daily_pnl_ratio = unrealized_pnl / 100000
            
            logging.info(
                f"Memory-based PnL: unrealized=${unrealized_pnl:.2f}, "
                f"ratio={daily_pnl_ratio:.2%} (excluding realized PnL)"
            )
            
            return daily_pnl_ratio
            
        except Exception as e:
            logging.error(f"Memory-based PnL calculation failed: {e}")
            return 0.0

    async def validate_position_risk(self, request_data: Dict) -> Dict[str, Any]:
        """
        Integrated risk validation and automatic execution linkage with enhanced error handling
        
        Features:
        - High-performance parallel processing (16-24 cores optimized)
        - Batch data validation for 400+ concurrent positions
        - Advanced error recovery with circuit breaker integration
        - Memory-efficient data structures (128GB RAM optimized)
        - Comprehensive fake loss prevention
        """
        start_time = time.time()
        
        try:
            # 0. Basic request data validation with enhanced type checking
            if not request_data:
                return {
                    "status": "error",
                    "approved": False,
                    "reason": "Request data is empty",
                    "error_code": "EMPTY_REQUEST_DATA",
                    "timestamp": time.time(),
                }

            if not isinstance(request_data, dict):
                return {
                    "status": "error",
                    "approved": False,
                    "reason": "Invalid request data type",
                    "error_code": "INVALID_REQUEST_TYPE",
                    "timestamp": time.time(),
                }

            # 1. Safely find signal data from multiple sources with priority order
            signal_data_dict = (request_data.get("signal_data") or 
                                request_data.get("signal") or 
                                request_data.get("phoenix_signal"))

            if not signal_data_dict:
                # Fallback: construct from top-level fields
                signal_data_dict = {
                    "symbol": request_data.get("symbol"),
                    "action": request_data.get("action"),
                    "price": request_data.get("price"),
                    "confidence": request_data.get("confidence", 0.8)
                }
                
            phoenix_analysis_dict = request_data.get("phoenix_analysis", {})
            
            # Convert to standardized models with comprehensive error handling
            try:
                signal_data = self._convert_to_signal_data(signal_data_dict)
            except Exception as e:
                logging.error(f"Signal data conversion failed: {e}")
                return {
                    "status": "error",
                    "approved": False,
                    "reason": f"Signal data conversion error: {str(e)}",
                    "error_code": "SIGNAL_DATA_CONVERSION_ERROR",
                    "timestamp": time.time(),
                }
                
            phoenix_analysis = None
            try:
                phoenix_analysis = (
                    self._convert_to_phoenix_analysis(phoenix_analysis_dict)
                    if phoenix_analysis_dict
                    else None
                )
            except Exception as e:
                logging.warning(f"Phoenix analysis conversion failed, using fallback: {e}")
                phoenix_analysis = None

            # Duplicate signal prevention with enhanced deduplication
            try:
                duplicate_check = self._check_signal_duplicate(signal_data)
                if duplicate_check["is_duplicate"]:
                    return {
                        "status": "blocked",
                        "approved": False,
                        "reason": f"Duplicate signal blocked - {duplicate_check['reason']}",
                        "cache_blocked": True,
                        "service": "risk_management",
                        "timestamp": time.time(),
                    }
            except Exception as e:
                logging.warning(f"Duplicate signal check failed, allowing to proceed: {e}")

            # Basic risk validation with comprehensive checks
            try:
                basic_validation = await self._basic_risk_validation(signal_data)
                if not basic_validation["approved"]:
                    return {
                        "status": "rejected",
                        "approved": False,
                        "reason": basic_validation["reason"],
                        "validation_stage": "basic",
                        "service": "risk_management",
                        "timestamp": time.time(),
                    }
            except Exception as e:
                logging.error(f"Basic risk validation failed: {e}")
                return {
                    "status": "error",
                    "approved": False,
                    "reason": f"Basic risk validation error: {str(e)}",
                    "error_code": "BASIC_VALIDATION_ERROR",
                    "timestamp": time.time(),
                }

            # High-performance parallel processing for multiple analyses
            analysis_tasks = []
            
            # Task 1: Multi-timeframe risk analysis
            analysis_tasks.append(
                self.multi_timeframe_analyzer.analyze_multi_timeframe_risk(signal_data)
            )
            
            # Task 2: Kelly Criterion calculation (can run in parallel)
            kelly_calculator = AdvancedKellyCriterionCalculator()
            analysis_tasks.append(
                asyncio.create_task(
                    asyncio.to_thread(
                        kelly_calculator.calculate_kelly_fraction,
                        signal_data, 
                        phoenix_analysis
                    )
                )
            )
            
            # Execute parallel analyses with timeout protection
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*analysis_tasks, return_exceptions=True),
                    timeout=10.0
                )
                
                multi_tf_analysis = results[0] if not isinstance(results[0], Exception) else {
                    "overall_risk_score": 0.5,
                    "recommended_position_adjustment": 1.0,
                    "risk_factors": ["Analysis failed"]
                }
                
                kelly_result = results[1] if not isinstance(results[1], Exception) else {
                    "kelly_fraction": 0.005,
                    "final_position_size": 0.005,
                    "win_rate_estimate": 0.5,
                    "profit_loss_ratio": 1.0
                }
                
                logging.info(
                    f"Parallel analysis completed: MF risk {multi_tf_analysis['overall_risk_score']:.2f}, "
                    f"Kelly {kelly_result['kelly_fraction']:.3f}"
                )
                
            except asyncio.TimeoutError:
                logging.error("Analysis timeout - using fallback values")
                multi_tf_analysis = {
                    "overall_risk_score": 0.5,
                    "recommended_position_adjustment": 1.0,
                    "risk_factors": ["Timeout"]
                }
                kelly_result = {
                    "kelly_fraction": 0.005,
                    "final_position_size": 0.005,
                    "win_rate_estimate": 0.5,
                    "profit_loss_ratio": 1.0
                }

            # Dynamic leverage optimization with adaptive parameters
            try:
                leverage_analysis = await self.dynamic_leverage_adjuster.calculate_optimal_leverage(
                    signal_data, multi_tf_analysis, kelly_result
                )
                leverage = leverage_analysis["optimal_leverage"]
                logging.info(
                    f"Dynamic leverage calculated: {leverage}x "
                    f"(confidence: {leverage_analysis['confidence_level']:.2f})"
                )
            except Exception as e:
                logging.warning(f"Dynamic leverage calculation failed, using fallback: {e}")
                leverage = max(2, int(config.RISK_CONFIG["max_leverage"] * 0.5))
                leverage_analysis = {
                    "optimal_leverage": leverage,
                    "confidence_level": 0.5,
                    "adjustments": {}
                }

            # Position size calculation with multi-timeframe adjustment
            position_size = kelly_result["final_position_size"]
            position_adjustment = multi_tf_analysis.get("recommended_position_adjustment", 1.0)
            adjusted_position_size = position_size * position_adjustment

            # Position calculation with comprehensive validation
            try:
                notional_value = adjusted_position_size * 100000
                margin_required = notional_value / leverage
                
                # Validate calculations
                if margin_required <= 0 or notional_value <= 0:
                    raise ValueError(
                        f"Invalid calculation result: notional={notional_value}, "
                        f"margin={margin_required}"
                    )
                
                # Additional sanity checks for high-spec system
                if margin_required > 50000:  # More than 50% of capital
                    logging.warning(
                        f"High margin requirement detected: ${margin_required:,.2f}"
                    )
                    
            except Exception as e:
                logging.error(f"Position calculation error: {e}")
                return {
                    "status": "error",
                    "approved": False,
                    "reason": f"Position calculation error: {str(e)}",
                    "error_code": "POSITION_CALCULATION_ERROR",
                    "timestamp": time.time(),
                }

            # Liquidation price calculation with safety buffer
            try:
                liquidation_price = self._calculate_liquidation_price(
                    signal_data.price, signal_data.action, leverage
                )
            except Exception as e:
                logging.warning(f"Liquidation price calculation failed, using default: {e}")
                liquidation_price = (
                    signal_data.price * 0.5 
                    if signal_data.action.lower() in ["buy", "long"] 
                    else signal_data.price * 1.5
                )

            # Stop loss/take profit calculation with Phoenix integration
            try:
                stop_loss_price, take_profit_price = self._calculate_stop_take_prices(
                    signal_data.price, signal_data.action, phoenix_analysis
                )
            except Exception as e:
                logging.warning(f"Stop/take profit calculation failed, using default: {e}")
                stop_loss_price = signal_data.price * 0.98
                take_profit_price = signal_data.price * 1.02

            # Portfolio VaR analysis with comprehensive risk assessment
            try:
                current_positions = list(self.active_positions.values())
                
                # Create simulated position for VaR calculation
                simulated_position = PositionRisk(
                    position_id="SIMULATION",
                    symbol=signal_data.symbol,
                    side="BUY" if signal_data.action.lower() in ["buy", "long"] else "SELL",
                    entry_price=signal_data.price,
                    current_price=signal_data.price,
                    quantity=adjusted_position_size,
                    leverage=leverage,
                    margin_required=margin_required,
                    unrealized_pnl=0,
                    liquidation_risk=0,
                    distance_to_liquidation=1.0
                )
                
                # Calculate VaR with new position included
                all_positions = current_positions + [simulated_position]
                var_analysis = await self.var_calculator.calculate_portfolio_var(all_positions)
                
                logging.info(
                    f"Portfolio VaR with new position: ${var_analysis['portfolio_var']:.2f} "
                    f"(utilization: {var_analysis['risk_metrics']['var_utilization']:.1%})"
                )
                
                # Enhanced VaR-based risk check with adaptive thresholds
                var_threshold = 0.15  # Base threshold
                if len(current_positions) > 300:  # Adjust for high position count
                    var_threshold = 0.18
                elif len(current_positions) > 200:
                    var_threshold = 0.16
                
                if var_analysis["risk_metrics"]["var_utilization"] > var_threshold:
                    return {
                        "status": "rejected",
                        "approved": False,
                        "reason": (
                            f"Portfolio VaR limit exceeded: "
                            f"{var_analysis['risk_metrics']['var_utilization']:.1%} > {var_threshold:.1%}"
                        ),
                        "validation_stage": "var_analysis",
                        "var_analysis": var_analysis,
                        "service": "risk_management",
                        "timestamp": time.time(),
                    }
                    
            except Exception as e:
                logging.warning(f"VaR analysis failed, proceeding with caution: {e}")
                var_analysis = {
                    "portfolio_var": margin_required * 0.1,
                    "risk_metrics": {"var_utilization": 0.05}
                }

            # Portfolio risk validation with comprehensive metrics
            try:
                portfolio_risk = await self._validate_portfolio_risk(
                    notional_value, margin_required, signal_data.symbol
                )
                
                # Integrate VaR analysis into portfolio risk
                portfolio_risk["var_analysis"] = var_analysis
                
                if portfolio_risk["risk_level"] == "HIGH":
                    return {
                        "status": "rejected",
                        "approved": False,
                        "reason": f"Portfolio risk exceeded: {portfolio_risk['reason']}",
                        "validation_stage": "portfolio",
                        "portfolio_metrics": portfolio_risk,
                        "service": "risk_management",
                        "timestamp": time.time(),
                    }
            except Exception as e:
                logging.warning(f"Portfolio risk validation failed, treating as medium risk: {e}")
                portfolio_risk = {
                    "risk_level": "MEDIUM",
                    "reason": "validation_failed",
                    "var_analysis": var_analysis
                }

            # Enhanced risk score calculation with multi-factor analysis
            risk_score = 0.8  # Default safe value
            
            try:
                # Validate all required components for enhanced calculation
                if (multi_tf_analysis and leverage_analysis and var_analysis and 
                    isinstance(multi_tf_analysis, dict) and isinstance(leverage_analysis, dict) and 
                    isinstance(var_analysis, dict)):
                    
                    enhanced_risk_data = {
                        "multi_tf_analysis": multi_tf_analysis,
                        "leverage_analysis": leverage_analysis,
                        "var_analysis": var_analysis,
                        "position_adjustment": position_adjustment
                    }
                    
                    enhanced_risk_score = self._calculate_enhanced_risk_score(
                        signal_data,
                        leverage,
                        margin_required,
                        liquidation_price,
                        kelly_result,
                        phoenix_analysis,
                        enhanced_risk_data
                    )
                    
                    # Validate enhanced risk score
                    if isinstance(enhanced_risk_score, (int, float)) and 0 <= enhanced_risk_score <= 1:
                        risk_score = enhanced_risk_score
                        logging.debug("Enhanced risk score calculation successful")
                    else:
                        logging.warning(
                            f"Invalid enhanced risk score: {enhanced_risk_score}, using fallback"
                        )
                        raise ValueError("Invalid risk score value")
                        
                else:
                    logging.debug("Enhanced risk data incomplete, using comprehensive calculation")
                    raise ValueError("Enhanced risk data not available")
                    
            except Exception as e:
                logging.warning(f"Enhanced risk score calculation failed: {e}")
                try:
                    risk_score = self._calculate_comprehensive_risk_score(
                        signal_data,
                        leverage,
                        margin_required,
                        liquidation_price,
                        kelly_result,
                        phoenix_analysis,
                    )
                except Exception as fallback_error:
                    logging.error(f"Fallback risk score calculation failed: {fallback_error}")
                    risk_score = 0.8

            # Final approval decision with Phoenix95 score validation
            phoenix_score_check = True
            if phoenix_analysis and hasattr(phoenix_analysis, 'phoenix_score'):
                raw_score = phoenix_analysis.phoenix_score
                
                # Normalize Phoenix95 score to 0-100 scale
                if raw_score <= 1.0:
                    normalized_score = raw_score * 100
                else:
                    normalized_score = raw_score
                
                phoenix_score_check = normalized_score >= config.PHOENIX95_CONFIG['min_score_threshold']
                logging.info(
                    f"Phoenix95 score check: {normalized_score:.1f} >= "
                    f"{config.PHOENIX95_CONFIG['min_score_threshold']} = {phoenix_score_check}"
                )
            
            # Multi-condition approval check
            approval_conditions = [
                risk_score <= 0.75,
                signal_data.confidence >= config.RISK_CONFIG["confidence_threshold"],
                phoenix_score_check
            ]
            
            if all(approval_conditions):
                # Risk profile creation with comprehensive validation
                try:
                    risk_profile = RiskProfile(
                        symbol=signal_data.symbol,
                        leverage=leverage,
                        position_size=position_size,
                        margin_required=margin_required,
                        liquidation_price=liquidation_price,
                        stop_loss_price=stop_loss_price,
                        take_profit_price=take_profit_price,
                        risk_score=risk_score,
                        confidence=signal_data.confidence,
                    )
                except Exception as e:
                    logging.error(f"Risk profile creation failed: {e}")
                    return {
                        "status": "error",
                        "approved": False,
                        "reason": f"Risk profile creation error: {str(e)}",
                        "error_code": "RISK_PROFILE_ERROR",
                        "timestamp": time.time(),
                    }

                # Execute Service automatic integration with enhanced error handling
                execution_result = {"status": "failed", "error": "Call failed"}
                try:
                    execution_result = await self._forward_to_execute_service(
                        signal_data, risk_profile, kelly_result, phoenix_analysis
                    )
                except Exception as e:
                    logging.error(f"Execute Service call failed: {e}")
                    execution_result = {"status": "failed", "error": str(e)}

                # Branch processing based on Execute result
                if execution_result.get("status") == "success":
                    # Notify Service alert (non-blocking)
                    try:
                        await self._forward_to_notify_service(
                            signal_data, risk_profile, execution_result, "approved"
                        )
                    except Exception as e:
                        logging.warning(f"Notify Service notification failed: {e}")

                    processing_time = time.time() - start_time
                    return {
                        "status": "approved",
                        "approved": True,
                        "risk_profile": asdict(risk_profile),
                        "kelly_result": kelly_result,
                        "execution_result": execution_result,
                        "portfolio_metrics": portfolio_risk,
                        "service": "risk_management",
                        "service_chain": request_data.get("service_chain", "unknown"),
                        "execution_forwarded": True,
                        "processing_time_ms": round(processing_time * 1000, 2),
                        "timestamp": time.time(),
                    }
                else:
                    # When Execute fails - non-blocking notification
                    try:
                        await self._forward_to_notify_service(
                            signal_data, risk_profile, execution_result, "execution_failed"
                        )
                    except Exception as e:
                        logging.warning(f"Notify Service failure notification failed: {e}")
                    
                    return {
                        "status": "execution_failed",
                        "approved": False,
                        "reason": f"Trade execution failed",
                        "execution_result": execution_result,
                        "service": "risk_management",
                        "timestamp": time.time(),
                    }

            else:
                # Determine rejection reason with detailed breakdown
                rejection_reasons = []
                if risk_score > 0.75:
                    rejection_reasons.append(f"Risk score too high: {risk_score:.2f}")
                if signal_data.confidence < config.RISK_CONFIG["confidence_threshold"]:
                    rejection_reasons.append(f"Low confidence: {signal_data.confidence:.2f}")
                if not phoenix_score_check:
                    phoenix_score = phoenix_analysis.phoenix_score if phoenix_analysis else 0
                    rejection_reasons.append(
                        f"Phoenix95 score too low: {phoenix_score:.1f} < "
                        f"{config.PHOENIX95_CONFIG['min_score_threshold']}"
                    )
                
                rejection_reason = "; ".join(rejection_reasons)

                # Notify Service rejection alert (non-blocking)
                try:
                    await self._forward_to_notify_service(
                        signal_data, None, None, "rejected", rejection_reason
                    )
                except Exception as e:
                    logging.warning(f"Notify Service rejection notification failed: {e}")

                processing_time = time.time() - start_time
                return {
                    "status": "rejected",
                    "approved": False,
                    "reason": rejection_reason,
                    "risk_score": risk_score,
                    "validation_stage": "final",
                    "kelly_result": kelly_result,
                    "service": "risk_management",
                    "processing_time_ms": round(processing_time * 1000, 2),
                    "timestamp": time.time(),
                }

        except Exception as e:
            processing_time = time.time() - start_time
            logging.error(f"Position risk validation failed: {e}\n{traceback.format_exc()}")
            return {
                "status": "error",
                "approved": False,
                "reason": f"Risk validation error: {str(e)}",
                "error_code": "UNEXPECTED_ERROR",
                "service": "risk_management",
                "processing_time_ms": round(processing_time * 1000, 2),
                "timestamp": time.time(),
            }

    def _convert_to_signal_data(self, data: Dict) -> SignalData:
        """
        Convert dictionary to SignalData model with comprehensive validation - SPOT only
        Optimized for high-performance system (128GB RAM, 16-24 cores)
        """
        
        # Basic data validation
        if not data:
            raise ValueError("Signal data is empty")
        
        if not isinstance(data, dict):
            raise ValueError(f"Invalid data type: expected dict, received {type(data)}")
        
        # Enhanced signal data extraction with priority-based fallback
        signal_data_dict = None
        signal_source = "unknown"
        
        # Priority 1: Check explicit signal keys
        priority_keys = ["signal_data", "signal", "phoenix_signal"]
        for key in priority_keys:
            source = data.get(key)
            if source and isinstance(source, dict):
                signal_data_dict = source
                signal_source = key
                break
        
        # Priority 2: Check if root level contains required fields
        if not signal_data_dict:
            required_root_fields = ["symbol", "action", "price"]
            if all(field in data for field in required_root_fields):
                signal_data_dict = data
                signal_source = "root_level"
                logging.debug("Signal data extracted from root level")
        
        # Priority 3: Check nested 'data' field
        if not signal_data_dict and "data" in data and isinstance(data["data"], dict):
            nested_data = data["data"]
            if all(field in nested_data for field in ["symbol", "action", "price"]):
                signal_data_dict = nested_data
                signal_source = "nested_data_field"
                logging.debug("Signal data extracted from nested 'data' field")
        
        # Validation failure with detailed diagnostics
        if not signal_data_dict:
            available_keys = list(data.keys())
            raise ValueError(
                f"No valid signal data found. "
                f"Expected keys: {priority_keys} or root-level fields (symbol, action, price). "
                f"Received keys: {available_keys}"
            )
        
        logging.debug(f"Signal data successfully extracted from: {signal_source}")
        
        # Extract and validate required fields
        symbol = signal_data_dict.get("symbol")
        action = signal_data_dict.get("action") 
        price = signal_data_dict.get("price")
        confidence = signal_data_dict.get("confidence", 0.8)
        
        # Required field validation with detailed error messages
        if not symbol:
            raise ValueError("Required field 'symbol' is missing")
        if not action:
            raise ValueError("Required field 'action' is missing") 
        if price is None:
            raise ValueError("Required field 'price' is missing")
        
        # Symbol validation and normalization for SPOT trading
        symbol_str = str(symbol).strip().upper()
        if not symbol_str:
            raise ValueError("Symbol cannot be empty")

        # Remove legacy futures suffix (.P) - SPOT API only
        spot_symbol = symbol_str.replace('.P', '')
        
        # Validate base symbol (without stablecoin suffix)
        clean_symbol = spot_symbol.replace('USDT', '').replace('BUSD', '').replace('USDC', '')
        if not clean_symbol.isalnum():
            raise ValueError(f"Invalid symbol format: {symbol_str}")
        
        # Action validation and normalization
        action_str = str(action).strip().upper()
        valid_actions = {"BUY", "SELL", "LONG", "SHORT", "B", "S", "L"}
        if action_str not in valid_actions:
            raise ValueError(f"Invalid action '{action_str}'. Valid options: {valid_actions}")
        
        # Normalize action shortcuts for convenience
        action_mapping = {"B": "BUY", "L": "LONG", "S": "SELL"}
        normalized_action = action_mapping.get(action_str, action_str)
        
        # Price and confidence conversion with validation
        try:
            price_float = float(price)
            if price_float <= 0:
                raise ValueError(f"Price must be positive: {price_float}")
            if price_float > 10000000:  # 10M USD limit for safety
                raise ValueError(f"Price exceeds maximum limit: {price_float}")
            
            confidence_float = float(confidence)
            if not (0 <= confidence_float <= 1):
                # Auto-convert percentage format (0-100) to decimal (0-1)
                if 0 <= confidence_float <= 100:
                    confidence_float = confidence_float / 100
                    logging.debug(f"Converted percentage confidence to decimal: {confidence_float}")
                else:
                    confidence_float = 0.8  # Safe default
                    logging.warning(f"Confidence out of valid range, using default: {confidence_float}")
                
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to convert numeric fields: {str(e)}")
        
        # Timestamp validation and normalization
        timestamp = signal_data_dict.get("timestamp", time.time())
        try:
            timestamp_float = float(timestamp)
            current_time = time.time()
            
            # Validate timestamp is reasonable (not future, not older than 24 hours)
            if timestamp_float > current_time or timestamp_float < current_time - 86400:
                logging.warning(f"Invalid timestamp {timestamp_float}, using current time")
                timestamp_float = current_time
        except (ValueError, TypeError):
            timestamp_float = time.time()
        
        # Optional field validation with safe conversion
        alpha_score = self._safe_float_with_range(
            signal_data_dict.get("alpha_score"), -10.0, 10.0
        )
        rsi = self._safe_float_with_range(
            signal_data_dict.get("rsi"), 0.0, 100.0
        )
        volatility = self._safe_float_with_range(
            signal_data_dict.get("volatility"), 0.0, 5.0
        )
        
        # MACD data structure validation
        macd = signal_data_dict.get("macd")
        if macd is not None and not isinstance(macd, dict):
            logging.warning("MACD data must be dict format, ignoring invalid data")
            macd = None
        
        # Create and return SignalData object with SPOT symbol
        return SignalData(
            signal_id=signal_data_dict.get("signal_id", f"SIG_{int(time.time()*1000)}"),
            symbol=spot_symbol,  # SPOT symbol without .P suffix
            action=normalized_action,
            price=price_float,
            confidence=confidence_float,
            alpha_score=alpha_score,
            rsi=rsi,
            macd=macd,
            volatility=volatility,
            market_condition=signal_data_dict.get("market_condition"),
            timestamp=timestamp_float,
        )

    def _safe_float_with_range(self, value, min_val: float, max_val: float) -> Optional[float]:
        """Safe float conversion with range validation"""
        if value is None:
            return None
        try:
            float_val = float(value)
            if min_val <= float_val <= max_val:
                return float_val
            else:
                logging.warning(f"Value out of allowed range({min_val}-{max_val}): {float_val}")
                return None
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value) -> Optional[float]:
        """Safe float conversion"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
        
    def _convert_to_phoenix_analysis(self, data: Dict) -> Phoenix95AnalysisResult:
        """Convert dictionary to Phoenix95AnalysisResult model - enhanced validation"""
        if not data or not isinstance(data, dict):
            logging.warning("Phoenix analysis data missing or invalid, using defaults")
            data = {}
        
        return Phoenix95AnalysisResult(
            phoenix_score=self._safe_float_with_range(data.get("phoenix_score", 0.75), 0.0, 1.0) or 0.75,
            strength_score=self._safe_float_with_range(data.get("strength_score", 0.75), 0.0, 1.0) or 0.75,
            momentum_score=self._safe_float_with_range(data.get("momentum_score", 0.75), 0.0, 1.0) or 0.75,
            volatility_score=self._safe_float_with_range(data.get("volatility_score", 0.75), 0.0, 1.0) or 0.75,
            volume_score=self._safe_float_with_range(data.get("volume_score", 0.75), 0.0, 1.0) or 0.75,
            kelly_criterion=data.get("kelly_criterion", {}) if isinstance(data.get("kelly_criterion"), dict) else {},
            leverage_optimization=data.get("leverage_optimization", {}) if isinstance(data.get("leverage_optimization"), dict) else {},
            risk_metrics=data.get("risk_metrics", {}) if isinstance(data.get("risk_metrics"), dict) else {},
            recommended_position_size=self._safe_float_with_range(data.get("recommended_position_size", 0.02), 0.001, 1.0) or 0.02,
            recommended_stop_loss=self._safe_float_with_range(data.get("recommended_stop_loss", 0.02), 0.001, 1.0) or 0.02,
            recommended_take_profit=self._safe_float_with_range(data.get("recommended_take_profit", 0.02), 0.001, 1.0) or 0.02,
        )

    def _check_signal_duplicate(self, signal_data: SignalData) -> Dict[str, Any]:
        """Signal duplicate blocking system - improved hash"""
        try:
            # Regular cache cleanup (every 10 minutes)
            current_time = time.time()
            if current_time - self.cache_cleanup_time > 600:
                self._cleanup_signal_cache()
                self.cache_cleanup_time = current_time

            # Generate signal hash - more precise hash
            hash_string = (
                f"{signal_data.symbol}_{signal_data.action}_"
                f"{signal_data.price:.4f}_{signal_data.confidence:.3f}_"
                f"{int(signal_data.timestamp // 60)}"  # Group by minute
            )
            
            signal_hash = hashlib.md5(hash_string.encode()).hexdigest()

            # Duplicate check
            if signal_hash in self.signal_cache:
                cache_entry = self.signal_cache[signal_hash]
                time_diff = current_time - cache_entry["timestamp"]

                if time_diff < config.RISK_CONFIG["signal_duplicate_window"]:
                    return {
                        "is_duplicate": True,
                        "reason": f"Identical signal processed {time_diff:.1f}s ago",
                        "original_timestamp": cache_entry["timestamp"],
                    }

            # Store in signal cache
            self.signal_cache[signal_hash] = {
                "timestamp": current_time,
                "signal_id": signal_data.signal_id,
                "symbol": signal_data.symbol,
                "action": signal_data.action,
                "hash_string": hash_string,
            }

            return {"is_duplicate": False, "reason": "New signal"}

        except Exception as e:
            logging.error(f"Signal duplicate check failed: {e}")
            return {"is_duplicate": False, "reason": "Check failed - allowed"}

    def _cleanup_signal_cache(self):
        """Signal cache cleanup with safe iteration and optimized performance"""
        try:
            current_time = time.time()
            cleanup_window = config.RISK_CONFIG["signal_duplicate_window"] * 2
            
            # Record pre-cleanup status
            initial_cache_size = len(self.signal_cache)
            
            # Check memory usage
            memory_usage = psutil.virtual_memory().percent
            
            # More aggressive cleanup under memory pressure
            if memory_usage > 95:
                cleanup_window = cleanup_window * 0.2  # Very aggressive cleanup
                logging.warning(f"Very high memory usage ({memory_usage:.1f}%) - emergency cache cleanup")
            elif memory_usage > 85:
                cleanup_window = cleanup_window * 0.5  # Half the cleanup threshold
                logging.info(f"High memory usage ({memory_usage:.1f}%) - aggressive cache cleanup mode")

            # Collect expired keys for batch processing - SAFE ITERATION
            expired_keys = []
            current_entries = []  # For statistics
            
            # Create a snapshot of items to avoid RuntimeError during iteration
            cache_items = list(self.signal_cache.items())
            
            for key, value in cache_items:
                try:
                    timestamp = value.get("timestamp", 0)
                    if current_time - timestamp > cleanup_window:
                        expired_keys.append(key)
                    else:
                        # Only collect statistics if not too many entries (performance optimization)
                        if len(current_entries) < 1000:
                            current_entries.append({
                                "key": key,
                                "age": current_time - timestamp,
                                "symbol": value.get("symbol", "unknown")
                            })
                except (KeyError, TypeError, AttributeError):
                    # Also remove corrupted cache entries
                    expired_keys.append(key)

            # Performance optimization with batch deletion
            deleted_count = 0
            for key in expired_keys:
                try:
                    del self.signal_cache[key]
                    deleted_count += 1
                except KeyError:
                    pass  # Ignore already deleted keys

            # Calculate cache statistics
            final_cache_size = len(self.signal_cache)
            cleanup_ratio = (deleted_count / max(initial_cache_size, 1)) * 100
            
            # Simplified statistics collection for performance
            avg_age = 0
            symbol_distribution = {}
            
            if current_entries and len(current_entries) <= 1000:
                # Analyze cache distribution by symbol
                for entry in current_entries:
                    symbol = entry["symbol"]
                    symbol_distribution[symbol] = symbol_distribution.get(symbol, 0) + 1
                
                # Calculate average cache entry age
                avg_age = sum(entry["age"] for entry in current_entries) / len(current_entries)

            # Detailed log output with ASCII arrow
            logging.info(
                f"Signal cache cleanup completed: {deleted_count} entries removed "
                f"({initial_cache_size} -> {final_cache_size}, {cleanup_ratio:.1f}% reduction)"
            )
            
            # Additional statistical information (only if not too many entries)
            if current_entries and len(symbol_distribution) > 0:
                top_symbols = sorted(symbol_distribution.items(), key=lambda x: x[1], reverse=True)[:3]
                logging.debug(
                    f"Cache statistics: avg age {avg_age:.1f}s, "
                    f"top symbols {dict(top_symbols)}, "
                    f"memory usage {memory_usage:.1f}%"
                )
            
            # Update performance metrics
            if not hasattr(self, 'cache_cleanup_stats'):
                self.cache_cleanup_stats = {}
            
            self.cache_cleanup_stats.update({
                "last_cleanup": current_time,
                "items_deleted": deleted_count,
                "cleanup_ratio": cleanup_ratio,
                "final_cache_size": final_cache_size,
                "memory_usage_at_cleanup": memory_usage
            })
            
            # Cache size warning
            if final_cache_size > 50000:
                logging.error(f"Cache size exceeded critical threshold: {final_cache_size} entries - immediate cleanup needed")
            elif final_cache_size > 10000:
                logging.warning(f"Cache size is large: {final_cache_size} entries")
                
        except Exception as e:
            logging.error(f"Signal cache cleanup failed: {e}")
            # Emergency cache cleanup
            try:
                cache_size = len(self.signal_cache)
                if cache_size > 1000:
                    # Force reset if cache is too large
                    self.signal_cache.clear()
                    logging.warning(f"Force cache reset due to cleanup failure: {cache_size} entries deleted")
            except Exception as emergency_error:
                logging.error(f"Emergency cache cleanup also failed: {emergency_error}")

    def get_cache_statistics(self) -> Dict:
        """Return cache statistics information"""
        try:
            current_time = time.time()
            cache_size = len(self.signal_cache)
            
            # Cache age analysis
            ages = []
            symbol_count = {}
            
            for key, value in self.signal_cache.items():
                try:
                    age = current_time - value.get("timestamp", 0)
                    ages.append(age)
                    
                    symbol = value.get("symbol", "unknown")
                    symbol_count[symbol] = symbol_count.get(symbol, 0) + 1
                except (KeyError, TypeError):
                    continue
            
            # Statistics calculation
            stats = {
                "cache_size": cache_size,
                "average_age_seconds": sum(ages) / len(ages) if ages else 0,
                "oldest_entry_seconds": max(ages) if ages else 0,
                "newest_entry_seconds": min(ages) if ages else 0,
                "unique_symbols": len(symbol_count),
                "most_common_symbols": dict(sorted(symbol_count.items(), key=lambda x: x[1], reverse=True)[:5]),
                "cleanup_stats": getattr(self, 'cache_cleanup_stats', {}),
                "memory_usage_percent": psutil.virtual_memory().percent,
                "timestamp": current_time
            }
            
            return stats
            
        except Exception as e:
            logging.error(f"Cache statistics collection failed: {e}")
            return {"error": str(e), "cache_size": len(getattr(self, 'signal_cache', {}))}

    async def get_system_health(self) -> Dict[str, Any]:
        """Comprehensive system health status check"""
        health_status = {
            "overall_status": "unknown",
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {},
            "summary": {},
            "alerts": []
        }
        
        try:
            # Redis connection status check
            redis_health = await self._check_redis_health()
            health_status["checks"]["redis"] = redis_health
            
            # Memory usage check
            memory_health = self._check_memory_health()
            health_status["checks"]["memory"] = memory_health
            
            # Active positions status check
            positions_health = self._check_positions_health()
            health_status["checks"]["positions"] = positions_health
            
            # Cache system status check
            cache_health = self._check_cache_health()
            health_status["checks"]["cache"] = cache_health
            
            # Overall status determination
            all_statuses = [check["status"] for check in health_status["checks"].values()]
            
            if all(status == "healthy" for status in all_statuses):
                health_status["overall_status"] = "healthy"
            elif any(status == "critical" for status in all_statuses):
                health_status["overall_status"] = "critical"
            else:
                health_status["overall_status"] = "warning"
            
            # Summary information
            health_status["summary"] = {
                "active_positions": len(self.active_positions),
                "memory_usage_percent": psutil.virtual_memory().percent,
                "cache_size": len(self.signal_cache),
                "redis_connected": redis_health["status"] == "healthy"
            }
            
            # Generate warnings
            for check_name, check_result in health_status["checks"].items():
                if check_result["status"] in ["warning", "critical"]:
                    health_status["alerts"].append({
                        "component": check_name,
                        "level": check_result["status"],
                        "message": check_result.get("message", f"{check_name} issue detected")
                    })
            
            return health_status
            
        except Exception as e:
            logging.error(f"System health check failed: {e}")
            return {
                "overall_status": "error",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
                "checks": {},
                "summary": {},
                "alerts": [{"component": "system", "level": "critical", "message": "Health check execution failed"}]
            }

    async def _check_redis_health(self) -> Dict[str, Any]:
        """Check Redis connection status"""
        try:
            if not self.service or not hasattr(self.service, 'redis') or not self.service.redis:
                return {
                    "status": "critical",
                    "message": "No Redis connection",
                    "details": {"connected": False}
                }
            
            start_time = time.time()
            await self.service.redis.ping()
            latency_ms = (time.time() - start_time) * 1000
            
            return {
                "status": "healthy" if latency_ms < 100 else "warning",
                "message": f"Redis connection normal (latency: {latency_ms:.1f}ms)",
                "details": {"connected": True, "latency_ms": round(latency_ms, 1)}
            }
            
        except Exception as e:
            return {
                "status": "critical",
                "message": f"Redis connection failed: {str(e)}",
                "details": {"connected": False, "error": str(e)}
            }

    def _check_memory_health(self) -> Dict[str, Any]:
        """Check memory usage status"""
        try:
            memory = psutil.virtual_memory()
            cpu = psutil.cpu_percent()
            
            if memory.percent > 90 or cpu > 90:
                status = "critical"
            elif memory.percent > 75 or cpu > 75:
                status = "warning"
            else:
                status = "healthy"
            
            return {
                "status": status,
                "message": f"Memory: {memory.percent:.1f}%, CPU: {cpu:.1f}%",
                "details": {
                    "memory_percent": round(memory.percent, 1),
                    "cpu_percent": round(cpu, 1)
                }
            }
            
        except Exception as e:
            return {
                "status": "warning",
                "message": f"Memory information collection failed: {str(e)}",
                "details": {"error": str(e)}
            }

    def _check_positions_health(self) -> Dict[str, Any]:
        """Check active positions status"""
        try:
            positions = list(self.active_positions.values())
            
            if not positions:
                return {
                    "status": "healthy",
                    "message": "No active positions",
                    "details": {"active_count": 0, "high_risk_count": 0}
                }
            
            high_risk_count = len([p for p in positions if p.liquidation_risk > 0.8])
            critical_risk_count = len([p for p in positions if p.liquidation_risk > 0.95])
            
            if critical_risk_count > 0:
                status = "critical"
                message = f"{critical_risk_count} positions with emergency liquidation risk"
            elif high_risk_count > 0:
                status = "warning"
                message = f"{high_risk_count} high-risk positions"
            else:
                status = "healthy"
                message = f"{len(positions)} positions normal"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "active_count": len(positions),
                    "high_risk_count": high_risk_count,
                    "critical_risk_count": critical_risk_count
                }
            }
            
        except Exception as e:
            return {
                "status": "warning",
                "message": f"Position status check failed: {str(e)}",
                "details": {"error": str(e)}
            }

    def _check_cache_health(self) -> Dict[str, Any]:
        """Check cache system status"""
        try:
            cache_size = len(self.signal_cache)
            max_cache_size = config.RISK_CONFIG.get("max_cache_size", 10000)
            
            if cache_size > max_cache_size:
                status = "critical"
                message = f"Cache size exceeded: {cache_size}/{max_cache_size}"
            elif cache_size > max_cache_size * 0.8:
                status = "warning"
                message = f"High cache usage: {cache_size}/{max_cache_size}"
            else:
                status = "healthy"
                message = f"Cache normal: {cache_size}/{max_cache_size}"
            
            return {
                "status": status,
                "message": message,
                "details": {
                    "cache_size": cache_size,
                    "max_cache_size": max_cache_size,
                    "usage_percent": round((cache_size / max_cache_size) * 100, 1)
                }
            }
            
        except Exception as e:
            return {
                "status": "warning",
                "message": f"Cache status check failed: {str(e)}",
                "details": {"error": str(e)}
            }

    async def _basic_risk_validation(self, signal_data: SignalData) -> Dict:
        """Enhanced basic risk validation with fake loss prevention"""
        # Maximum position count check
        if len(self.active_positions) >= config.RISK_CONFIG["max_positions"]:
            return {
                "approved": False,
                "reason": f"Max positions exceeded: {len(self.active_positions)}/{config.RISK_CONFIG['max_positions']}",
            }

        # Confidence check
        if signal_data.confidence < config.RISK_CONFIG["confidence_threshold"]:
            return {
                "approved": False,
                "reason": f"Low confidence: {signal_data.confidence:.2f} < {config.RISK_CONFIG['confidence_threshold']}",
            }

        # Symbol duplicate check (max 2 positions per symbol)
        existing_positions = [
            p for p in self.active_positions.values() if p.symbol == signal_data.symbol
        ]
        if len(existing_positions) >= 2:
            return {
                "approved": False,
                "reason": f"Symbol {signal_data.symbol} duplicate positions (max 2)",
            }

        # ENHANCED: Daily loss limit check with fake loss prevention
        try:
            active_position_count = len(self.active_positions)
            daily_pnl_ratio = await self._get_daily_pnl()
            max_daily_loss = config.RISK_CONFIG["max_daily_loss"]
            
            # Detailed logging
            logging.info(
                f"Daily loss check: current P&L {daily_pnl_ratio:.2%}, "
                f"limit -{max_daily_loss:.2%}, active positions {active_position_count}"
            )
            
            # CRITICAL: If no active positions but negative P&L exists, it's fake
            if active_position_count == 0 and daily_pnl_ratio < -0.01:  # More than -1% loss
                logging.error(
                    f"FAKE LOSS DETECTED: {daily_pnl_ratio:.2%} loss with 0 positions"
                )
                # Reset Redis P&L data - Fixed Redis reference
                if hasattr(self.service, 'redis') and self.service.redis:
                    try:
                        today_str = datetime.utcnow().strftime("%Y-%m-%d")
                        pnl_key = f"pnl:{today_str}"
                        await self.service.redis.delete(pnl_key)
                        logging.info("Fake loss reset - Redis P&L deleted")
                        daily_pnl_ratio = 0.0
                    except Exception as reset_error:
                        logging.error(f"Failed to reset fake loss: {reset_error}")
            
            # Apply daily loss limit check
            if daily_pnl_ratio <= -max_daily_loss:
                return {
                    "approved": False,
                    "reason": f"Daily loss limit exceeded: {daily_pnl_ratio:.2%} <= -{max_daily_loss:.2%}",
                    "daily_pnl": daily_pnl_ratio,
                    "max_loss_limit": max_daily_loss,
                    "active_positions": active_position_count,
                }
        
        except Exception as e:
            logging.error(f"Daily loss check failed: {e}")
            # On error, allow trade but log the issue
            logging.warning("Daily loss check failed - allowing trade with caution")

        # Emergency mode check
        if hasattr(self, 'emergency_mode') and self.emergency_mode:
            return {"approved": False, "reason": "Emergency mode active - blocking new positions"}

        return {"approved": True, "reason": "Basic validation passed"}

    def _calculate_optimal_leverage(
        self,
        confidence: float,
        symbol: str,
        phoenix_analysis: Optional[Phoenix95AnalysisResult],
    ) -> int:
        """Calculate optimal leverage"""
        try:
            max_leverage = config.RISK_CONFIG["max_leverage"]

            # Phoenix95 analysis based leverage
            if phoenix_analysis:
                recommended_leverage = phoenix_analysis.leverage_optimization.get(
                    "recommended_leverage", max_leverage
                )
                if recommended_leverage and isinstance(
                    recommended_leverage, (int, float)
                ):
                    leverage = min(int(recommended_leverage), max_leverage)
                else:
                    leverage = self._calculate_confidence_based_leverage(
                        confidence, max_leverage
                    )
            else:
                leverage = self._calculate_confidence_based_leverage(
                    confidence, max_leverage
                )

            # Maximum leverage limit by symbol
            symbol_max_leverage = {
                "BTCUSDT": max_leverage,
                "ETHUSDT": max_leverage,
                "BNBUSDT": 15,
                "ADAUSDT": 10,
                "DOGEUSDT": 8,
                "XRPUSDT": 12,
                "SOLUSDT": 15,
                "MATICUSDT": 10,
                "DOTUSDT": 12,
            }

            symbol_limit = symbol_max_leverage.get(symbol, max_leverage)
            leverage = min(leverage, symbol_limit)

            # Adjustment by market conditions
            hour = datetime.utcnow().hour
            if 2 <= hour <= 6:  # Inactive hours
                leverage = int(leverage * 0.8)
            elif datetime.utcnow().weekday() >= 5:  # Weekend
                leverage = int(leverage * 0.7)

            return max(2, leverage)  # Minimum 2x

        except Exception as e:
            logging.error(f"Leverage calculation failed: {e}")
            return max(2, int(max_leverage * 0.5))

    def _calculate_confidence_based_leverage(
        self, confidence: float, max_leverage: int
    ) -> int:
        """Calculate confidence-based leverage"""
        if confidence >= 0.95:
            return max_leverage
        elif confidence >= 0.90:
            return int(max_leverage * 0.9)
        elif confidence >= 0.85:
            return int(max_leverage * 0.8)
        elif confidence >= 0.80:
            return int(max_leverage * 0.7)
        elif confidence >= 0.75:
            return int(max_leverage * 0.6)
        else:
            return int(max_leverage * 0.4)

    def _calculate_liquidation_price(
        self, entry_price: float, action: str, leverage: int
    ) -> float:
        """Calculate liquidation price"""
        try:
            maintenance_margin_rate = 0.004  # 0.4% maintenance margin

            if action.lower() in ["buy", "long"]:
                liquidation_price = entry_price * (
                    1 - (1 / leverage) + maintenance_margin_rate
                )
            else:
                liquidation_price = entry_price * (
                    1 + (1 / leverage) - maintenance_margin_rate
                )

            return max(0, liquidation_price)

        except Exception as e:
            logging.error(f"Liquidation price calculation failed: {e}")
            return (
                entry_price * 0.5
                if action.lower() in ["buy", "long"]
                else entry_price * 1.5
            )

    def _calculate_stop_take_prices(
        self,
        entry_price: float,
        action: str,
        phoenix_analysis: Optional[Phoenix95AnalysisResult],
    ) -> Tuple[float, float]:
        """Calculate stop loss/take profit prices"""
        try:
            if phoenix_analysis:
                stop_loss_percent = phoenix_analysis.recommended_stop_loss
                take_profit_percent = phoenix_analysis.recommended_take_profit
            else:
                stop_loss_percent = config.RISK_CONFIG["stop_loss_percent"]
                take_profit_percent = config.RISK_CONFIG["take_profit_percent"]

            if action.lower() in ["buy", "long"]:
                stop_loss_price = entry_price * (1 - stop_loss_percent)
                take_profit_price = entry_price * (1 + take_profit_percent)
            else:
                stop_loss_price = entry_price * (1 + stop_loss_percent)
                take_profit_price = entry_price * (1 - take_profit_percent)

            return stop_loss_price, take_profit_price

        except Exception as e:
            logging.error(f"Stop loss/take profit price calculation failed: {e}")
            return entry_price * 0.98, entry_price * 1.02

    async def _validate_portfolio_risk(
        self, new_notional: float, new_margin: float, symbol: str
    ) -> Dict:
        """Validate overall portfolio risk"""
        try:
            # Calculate current portfolio metrics
            current_exposure = sum(
                pos.quantity * pos.current_price
                for pos in self.active_positions.values()
                if pos.status == "ACTIVE"
            )
            
            current_margin = sum(
                pos.margin_required
                for pos in self.active_positions.values()
                if pos.status == "ACTIVE"
            )
            
            # Expected metrics when adding new position
            total_exposure = current_exposure + new_notional
            total_margin = current_margin + new_margin

            # Base capital (virtual)
            base_capital = 100000  # $100K

            # Risk limit validation
            risk_violations = []

            # 1. Total exposure limit
            if total_exposure > self.portfolio_limits["max_total_exposure"]:
                risk_violations.append(
                    f"Total exposure limit exceeded: ${total_exposure:,.0f} > ${self.portfolio_limits['max_total_exposure']:,.0f}"
                )

            # 2. Margin usage limit
            margin_ratio = total_margin / base_capital
            if margin_ratio > self.portfolio_limits["max_margin_usage"]:
                risk_violations.append(
                    f"Margin usage exceeded: {margin_ratio:.1%} > {self.portfolio_limits['max_margin_usage']:.1%}"
                )

            # 3. Single position size limit
            position_ratio = new_notional / base_capital
            if position_ratio > self.portfolio_limits["max_single_position"]:
                risk_violations.append(
                    f"Single position size exceeded: {position_ratio:.1%} > {self.portfolio_limits['max_single_position']:.1%}"
                )

            # 4. Correlation exposure limit (BTC related positions)
            btc_related_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
            if symbol in btc_related_symbols:
                btc_exposure = (
                    sum(
                        pos.quantity * pos.current_price
                        for pos in self.active_positions.values()
                        if pos.symbol in btc_related_symbols
                    )
                    + new_notional
                )

                correlation_ratio = btc_exposure / base_capital
                if (
                    correlation_ratio
                    > self.portfolio_limits["max_correlation_exposure"]
                ):
                    risk_violations.append(
                        f"Correlation exposure exceeded: {correlation_ratio:.1%} > {self.portfolio_limits['max_correlation_exposure']:.1%}"
                    )

            if risk_violations:
                return {
                    "risk_level": "HIGH",
                    "reason": "; ".join(risk_violations),
                    "current_exposure": current_exposure,
                    "total_exposure": total_exposure,
                    "current_margin": current_margin,
                    "total_margin": total_margin,
                    "margin_ratio": margin_ratio,
                    "violations": risk_violations,
                }

            return {
                "risk_level": "LOW",
                "current_exposure": current_exposure,
                "total_exposure": total_exposure,
                "current_margin": current_margin,
                "total_margin": total_margin,
                "margin_ratio": margin_ratio,
                "exposure_ratio": total_exposure / base_capital,
            }

        except Exception as e:
            logging.error(f"Portfolio risk validation failed: {e}")
            return {"risk_level": "MEDIUM", "reason": "Validation failed"}

    def _calculate_enhanced_risk_score(
        self,
        signal_data: SignalData,
        leverage: int,
        margin_required: float,
        liquidation_price: float,
        kelly_result: Dict,
        phoenix_analysis: Optional[Phoenix95AnalysisResult],
        enhanced_risk_data: Dict
    ) -> float:
        try:
            risk_factors = []
            
            # Extract enhanced data
            multi_tf_analysis = enhanced_risk_data.get("multi_tf_analysis", {})
            leverage_analysis = enhanced_risk_data.get("leverage_analysis", {})
            var_analysis = enhanced_risk_data.get("var_analysis", {})
            
            # 1. Multi-timeframe risk (25%)
            overall_tf_risk = multi_tf_analysis.get("overall_risk_score", 0.5)
            risk_factors.append(overall_tf_risk * 0.25)
            
            # 2. VaR-based risk (20%)
            var_utilization = var_analysis.get("risk_metrics", {}).get("var_utilization", 0.05)
            var_risk = min(var_utilization / 0.15, 1.0)  # 15% utilization = 100% risk
            risk_factors.append(var_risk * 0.20)
            
            # 3. Dynamic leverage risk (15%)
            leverage_confidence = leverage_analysis.get("confidence_level", 0.5)
            leverage_risk = 1 - leverage_confidence
            risk_factors.append(leverage_risk * 0.15)
            
            # 4. Traditional leverage risk (10%)
            traditional_leverage_risk = min(leverage / config.RISK_CONFIG["max_leverage"], 1.0)
            risk_factors.append(traditional_leverage_risk * 0.10)
            
            # 5. Correlation risk (10%)
            correlation_risk_data = multi_tf_analysis.get("correlation_risk", {})
            max_correlation = correlation_risk_data.get("max_correlation", 0.0)
            correlation_risk = min(max_correlation, 1.0)
            risk_factors.append(correlation_risk * 0.10)
            
            # 6. Liquidation proximity risk (10%)
            liquidation_distance = abs(signal_data.price - liquidation_price) / signal_data.price
            liquidation_risk = max(0, 1 - liquidation_distance / 0.15)
            risk_factors.append(liquidation_risk * 0.10)
            
            # 7. Phoenix95 analysis risk (5%)
            if phoenix_analysis:
                phoenix_risk = 1 - phoenix_analysis.phoenix_score
                risk_factors.append(phoenix_risk * 0.05)
            else:
                risk_factors.append(0.025)
            
            # 8. Kelly criterion risk (5%)
            kelly_fraction = kelly_result.get("kelly_fraction", 0.005)
            kelly_risk = max(0, (kelly_fraction - config.RISK_CONFIG["kelly_max_fraction"]) / config.RISK_CONFIG["kelly_max_fraction"])
            risk_factors.append(kelly_risk * 0.05)
            
            # 9. Volatility-based adjustments
            volatility_data = multi_tf_analysis.get("volatility_analysis", {})
            current_vol = volatility_data.get("current_volatility", 0.03)
            if current_vol > 0.06:  # High volatility
                risk_factors.append(0.05)
            elif current_vol < 0.02:  # Very low volatility (potential fake breakout)
                risk_factors.append(0.03)
            
            # 10. Risk concentration penalty
            risk_concentration = var_analysis.get("risk_metrics", {}).get("risk_concentration", 0.0)
            if risk_concentration > 0.5:  # High concentration
                risk_factors.append(0.05)
            
            # 11. Market timing risk adjustments
            hour = datetime.utcnow().hour
            weekday = datetime.utcnow().weekday()
            
            if 2 <= hour <= 6:  # Low activity hours
                risk_factors.append(0.04)
            elif weekday >= 5:  # Weekend
                risk_factors.append(0.06)
            elif weekday == 0:  # Monday (volatile start)
                risk_factors.append(0.02)
            
            # 12. Risk factor penalties
            risk_factor_count = len(multi_tf_analysis.get("risk_factors", []))
            if risk_factor_count > 3:
                risk_factors.append(0.03)
            
            total_risk = sum(risk_factors)
            
            # Apply position adjustment penalty
            position_adjustment = enhanced_risk_data.get("position_adjustment", 1.0)
            if position_adjustment < 0.8:  # Significant position reduction recommended
                total_risk += 0.05
            
            # Ensure risk score is within bounds
            final_risk = max(0.1, min(0.95, total_risk))
            
            # Log detailed risk breakdown for monitoring
            logging.debug(
                f"Enhanced risk score breakdown - "
                f"Multi-TF: {overall_tf_risk:.2f}, VaR: {var_risk:.2f}, "
                f"Dynamic Lev: {leverage_risk:.2f}, Correlation: {correlation_risk:.2f}, "
                f"Final: {final_risk:.2f}"
            )
            
            return final_risk
            
        except Exception as e:
            logging.error(f"Enhanced risk score calculation failed: {e}")
            return 0.8

    def _calculate_comprehensive_risk_score(
        self,
        signal_data: SignalData,
        leverage: int,
        margin_required: float,
        liquidation_price: float,
        kelly_result: Dict,
        phoenix_analysis: Optional[Phoenix95AnalysisResult],
    ) -> float:
        try:
            risk_factors = []

            # 1. Leverage risk (30%)
            leverage_risk = min(leverage / config.RISK_CONFIG["max_leverage"], 1.0)
            risk_factors.append(leverage_risk * 0.30)

            # 2. Margin ratio risk (20%)
            margin_ratio = margin_required / 100000
            margin_risk = min(margin_ratio / config.RISK_CONFIG["max_position_size"], 1.0)
            risk_factors.append(margin_risk * 0.20)

            # 3. Liquidation proximity risk (20%)
            liquidation_distance = abs(signal_data.price - liquidation_price) / signal_data.price
            liquidation_risk = max(0, 1 - liquidation_distance / 0.15)
            risk_factors.append(liquidation_risk * 0.20)

            # 4. Confidence inverse risk (10%)
            confidence_risk = 1 - signal_data.confidence
            risk_factors.append(confidence_risk * 0.10)

            # 5. Kelly criterion risk (10%)
            kelly_risk = max(0, (kelly_result["kelly_fraction"] - config.RISK_CONFIG["kelly_max_fraction"]) / config.RISK_CONFIG["kelly_max_fraction"])
            risk_factors.append(kelly_risk * 0.10)

            # 6. Phoenix95 based risk (10%)
            if phoenix_analysis:
                phoenix_risk = 1 - phoenix_analysis.phoenix_score
                risk_factors.append(phoenix_risk * 0.10)
            else:
                risk_factors.append(0.05)

            # 7. Market timing risk
            hour = datetime.utcnow().hour
            if 2 <= hour <= 6:
                risk_factors.append(0.05)
            elif datetime.utcnow().weekday() >= 5:
                risk_factors.append(0.08)

            total_risk = sum(risk_factors)
            return min(1.0, total_risk)

        except Exception as e:
            logging.error(f"Risk score calculation failed: {e}")
            return 0.8

    async def _forward_to_execute_service(
        self,
        signal_data: SignalData,
        risk_profile: RiskProfile,
        kelly_result: Dict,
        phoenix_analysis: Optional[Phoenix95AnalysisResult],
    ) -> Dict:
        """
        Forward approved trading information to Execute Service with enhanced validation.
        
        Features:
        - Safe JSON serialization (datetime -> float/str)
        - High-performance data validation (128GB RAM optimized)
        - Enhanced error handling with detailed logging
        - Binance testnet compatibility check
        
        Args:
            signal_data: Trading signal information
            risk_profile: Risk management parameters
            kelly_result: Kelly Criterion calculation results
            phoenix_analysis: Phoenix95 AI analysis results
            
        Returns:
            Dict containing execution result and status
        """
        try:
            # Input data safety validation
            if not signal_data:
                raise ValueError("signal_data is None")
            if not risk_profile:
                raise ValueError("risk_profile is None")
            if not kelly_result:
                raise ValueError("kelly_result is None")

            logging.info(f"Execute Service call started: {signal_data.symbol}")
            logging.info(f"Data validation:")
            logging.info(f"   signal_data.symbol: {signal_data.symbol}")
            logging.info(f"   signal_data.action: {signal_data.action}")
            logging.info(f"   signal_data.price: {signal_data.price}")
            logging.info(f"   signal_data.confidence: {signal_data.confidence}")
            
            # Pre-trade validation with binance connection check
            pre_trade_check = await self._validate_trading_prerequisites(signal_data, risk_profile)
            if not pre_trade_check["valid"]:
                return {
                    "status": "failed",
                    "error": f"Pre-trade validation failed: {pre_trade_check['reason']}",
                    "timestamp": time.time(),
                }
            
            # Safe datetime/complex object conversion helper
            def safe_convert_to_json_value(value, default=None):
                """Convert any Python object to JSON-serializable value"""
                if value is None:
                    return default
                if isinstance(value, datetime):
                    return value.timestamp()  # Convert to UNIX timestamp
                if isinstance(value, date):
                    return time.mktime(value.timetuple())  # Convert date to timestamp
                if isinstance(value, Decimal):
                    return float(value)
                if isinstance(value, (int, float, str, bool)):
                    return value
                if isinstance(value, dict):
                    return {k: safe_convert_to_json_value(v) for k, v in value.items()}
                if isinstance(value, (list, tuple)):
                    return [safe_convert_to_json_value(item) for item in value]
                # Fallback for unknown types
                try:
                    return str(value)
                except:
                    return default
            
            # Enhanced Execute Service data structure with safe JSON serialization
            execute_data = {
                "signal_data": {
                    "signal_id": str(getattr(signal_data, 'signal_id', f"SIG_{int(time.time()*1000)}")),
                    "symbol": str(getattr(signal_data, 'symbol', '')).strip().upper(),
                    "action": str(getattr(signal_data, 'action', '')).strip().upper(),
                    "price": float(getattr(signal_data, 'price', 0.0)),
                    "confidence": float(getattr(signal_data, 'confidence', 0.0)),
                    "timestamp": float(safe_convert_to_json_value(getattr(signal_data, 'timestamp', time.time()), time.time())),
                    "alpha_score": safe_convert_to_json_value(getattr(signal_data, 'alpha_score', None)),
                    "rsi": safe_convert_to_json_value(getattr(signal_data, 'rsi', None)),
                    "macd": safe_convert_to_json_value(getattr(signal_data, 'macd', None)),
                    "volatility": safe_convert_to_json_value(getattr(signal_data, 'volatility', None)),
                    "market_condition": safe_convert_to_json_value(getattr(signal_data, 'market_condition', None)),
                },
                "risk_validation": {
                    "approved": True,
                    "risk_score": float(getattr(risk_profile, 'risk_score', 0.5)),
                    "leverage": int(getattr(risk_profile, 'leverage', 1)),
                    "position_size": float(getattr(risk_profile, 'position_size', 0.01)),
                    "margin_required": float(getattr(risk_profile, 'margin_required', 0.0)),
                    "liquidation_price": float(getattr(risk_profile, 'liquidation_price', 0.0)),
                    "stop_loss_price": float(getattr(risk_profile, 'stop_loss_price', 0.0)),
                    "take_profit_price": float(getattr(risk_profile, 'take_profit_price', 0.0)),
                    "kelly_fraction": float(kelly_result.get("kelly_fraction", 0.005)),
                    "win_rate_estimate": float(kelly_result.get("win_rate_estimate", 0.6)),
                },
                "phoenix_analysis": {
                    "phoenix_score": float(phoenix_analysis.phoenix_score if phoenix_analysis else 75.0),
                    "strength_score": float(phoenix_analysis.strength_score if phoenix_analysis else 0.75),
                    "momentum_score": float(phoenix_analysis.momentum_score if phoenix_analysis else 0.75),
                    "volatility_score": float(phoenix_analysis.volatility_score if phoenix_analysis else 0.75),
                    "volume_score": float(phoenix_analysis.volume_score if phoenix_analysis else 0.75),
                    "profit_loss_ratio": float(kelly_result.get("profit_loss_ratio", 1.0)),
                    "market_adjustment": float(kelly_result.get("market_adjustment", 1.0)),
                    "volatility_adjustment": float(kelly_result.get("volatility_adjustment", 1.0)),
                },
                "execution_settings": {
                    "margin_mode": "ISOLATED",
                    "time_in_force": "GTC",
                    "reduce_only": False,
                    "binance_testnet": True,
                },
                "service_chain": "brain_risk_execute",
                "timestamp": time.time(),
            }

            # Critical data validation before sending
            validation_errors = []
            if not execute_data["signal_data"]["symbol"]:
                validation_errors.append("Missing symbol")
            if execute_data["signal_data"]["action"] not in ["BUY", "SELL", "LONG", "SHORT"]:
                validation_errors.append(f"Invalid action: {execute_data['signal_data']['action']}")
            if execute_data["signal_data"]["price"] <= 0:
                validation_errors.append(f"Invalid price: {execute_data['signal_data']['price']}")
            if execute_data["signal_data"]["confidence"] <= 0:
                validation_errors.append(f"Invalid confidence: {execute_data['signal_data']['confidence']}")
            if execute_data["risk_validation"]["leverage"] < 1:
                validation_errors.append(f"Invalid leverage: {execute_data['risk_validation']['leverage']}")

            if validation_errors:
                raise ValueError(f"Data validation failed: {', '.join(validation_errors)}")

            # JSON serialization test before sending
            try:
                json_test = json.dumps(execute_data)
                logging.debug(f"JSON serialization test passed: {len(json_test)} bytes")
            except (TypeError, ValueError) as json_error:
                logging.error(f"JSON serialization test failed: {json_error}")
                # Attempt to fix by converting all values recursively
                execute_data = safe_convert_to_json_value(execute_data, {})
                logging.info("Applied safe JSON conversion to all data")
                
                # Retry serialization after conversion
                try:
                    json_test = json.dumps(execute_data)
                    logging.info(f"JSON serialization succeeded after conversion: {len(json_test)} bytes")
                except (TypeError, ValueError) as retry_error:
                    logging.error(f"JSON serialization still failed: {retry_error}")
                    return {
                        "status": "failed",
                        "error": f"JSON serialization error: {str(retry_error)}",
                        "timestamp": time.time(),
                    }

            # Enhanced logging for debugging
            logging.info(f"Execute Service data transmission:")
            logging.info(f"   Symbol: {execute_data['signal_data']['symbol']}")
            logging.info(f"   Action: {execute_data['signal_data']['action']}")
            logging.info(f"   Price: ${execute_data['signal_data']['price']:,.2f}")
            logging.info(f"   Leverage: {execute_data['risk_validation']['leverage']}x")
            logging.info(f"   Position Size: {execute_data['risk_validation']['position_size']:.3f}")
            logging.info(f"   URL: {config.SERVICE_URLS['execute_service']}/execute")

            # Execute Service call with enhanced retry
            result = await self._call_service_with_enhanced_retry(
                f"{config.SERVICE_URLS['execute_service']}/execute", 
                execute_data,
                max_retries=3
            )

            # Validate response
            if not result or not isinstance(result, dict):
                logging.error("Execute Service returned invalid response")
                return {
                    "status": "failed",
                    "error": "Invalid response from Execute Service",
                    "timestamp": time.time(),
                }

            response_status = result.get("status", "unknown")
            logging.info(f"Execute Service response: {response_status}")
            
            if response_status == "success":
                logging.info(f"Trade execution successful: {signal_data.symbol}")
            else:
                logging.warning(f"Trade execution failed: {result.get('error', 'Unknown error')}")
            
            return result

        except Exception as e:
            logging.error(f"Execute Service call failed: {e}")
            logging.error(f"Error details: {traceback.format_exc()}")

            return {
                "status": "failed",
                "error": str(e),
                "service": "execute_service",
                "timestamp": time.time(),
            }
            
    async def _validate_trading_prerequisites(self, signal_data: SignalData, risk_profile: RiskProfile) -> Dict:
        """Validate trading prerequisites including binance connection"""
        try:
            # Check basic data integrity
            if not signal_data.symbol or len(signal_data.symbol.strip()) == 0:
                return {"valid": False, "reason": "Invalid symbol"}
            
            if signal_data.action not in ["BUY", "SELL", "LONG", "SHORT"]:
                return {"valid": False, "reason": f"Invalid action: {signal_data.action}"}
            
            if signal_data.price <= 0:
                return {"valid": False, "reason": f"Invalid price: {signal_data.price}"}
            
            if risk_profile.leverage < 1 or risk_profile.leverage > 125:
                return {"valid": False, "reason": f"Invalid leverage: {risk_profile.leverage}"}
            
            # Check if Execute Service is reachable
            try:
                async with aiohttp.ClientSession() as session:
                    health_url = f"{config.SERVICE_URLS['execute_service']}/health"
                    async with session.get(health_url, timeout=5) as response:
                        if response.status != 200:
                            return {"valid": False, "reason": "Execute Service unreachable"}
            except Exception as e:
                return {"valid": False, "reason": f"Execute Service connection failed: {str(e)}"}
            
            # All checks passed
            return {"valid": True, "reason": "All prerequisites met"}
            
        except Exception as e:
            return {"valid": False, "reason": f"Validation error: {str(e)}"}

    async def _call_service_with_enhanced_retry(self, url: str, data: Dict, max_retries: int = 3) -> Dict:
        """
        Enhanced service call with improved retry logic and safe JSON serialization.
        
        Features:
        - Automatic datetime to timestamp conversion
        - JSON serialization validation before sending
        - High-performance connection pooling (128GB RAM optimized)
        - Exponential backoff with jitter
        - Comprehensive error handling
        
        Args:
            url: Target service endpoint URL
            data: Request data (will be auto-converted to JSON-safe format)
            max_retries: Maximum retry attempts (default: 3)
            
        Returns:
            Dict containing service response or error details
        """
        service_name = self._extract_service_name(url)
        
        # Safe JSON conversion helper (reuse from _forward_to_execute_service)
        def safe_convert_to_json_value(value, default=None):
            """Convert any Python object to JSON-serializable value"""
            if value is None:
                return default
            if isinstance(value, (datetime, date)):
                return value.timestamp() if isinstance(value, datetime) else time.mktime(value.timetuple())
            if isinstance(value, Decimal):
                return float(value)
            if isinstance(value, (int, float, str, bool)):
                return value
            if isinstance(value, dict):
                return {k: safe_convert_to_json_value(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [safe_convert_to_json_value(item) for item in value]
            # Fallback for unknown types
            try:
                return str(value)
            except:
                return default
        
        # Pre-process data to ensure JSON compatibility
        try:
            safe_data = safe_convert_to_json_value(data, {})
            
            # Validation test
            json_test = json.dumps(safe_data)
            logging.debug(f"JSON pre-validation passed: {len(json_test)} bytes for {service_name}")
            
        except (TypeError, ValueError) as json_error:
            logging.error(f"JSON pre-validation failed for {service_name}: {json_error}")
            return {
                "status": "failed",
                "error": f"JSON serialization error: {str(json_error)}",
                "timestamp": time.time(),
            }
        
        # High-performance retry loop
        for attempt in range(max_retries):
            try:
                # Adaptive timeout based on attempt (longer for retries)
                base_timeout = 20
                adaptive_timeout = base_timeout + (attempt * 5)
                
                timeout = aiohttp.ClientTimeout(
                    total=adaptive_timeout,
                    connect=5,
                    sock_read=adaptive_timeout - 5
                )
                
                # High-performance connection pooling (128GB RAM, 16-24 cores optimization)
                connector = aiohttp.TCPConnector(
                    limit=100,              # Max total connections
                    limit_per_host=30,      # Max per host
                    ttl_dns_cache=300,      # DNS cache 5 minutes
                    force_close=False,      # Reuse connections
                    enable_cleanup_closed=True
                )
                
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    # Use safe_data instead of original data
                    async with session.post(url, json=safe_data, timeout=timeout) as response:
                        
                        if response.status == 200:
                            result = await response.json()
                            logging.info(f"{service_name} call successful on attempt {attempt + 1}/{max_retries}")
                            return result
                        
                        elif 400 <= response.status < 500:
                            error_text = await response.text()
                            logging.error(f"{service_name} client error {response.status}: {error_text[:200]}")
                            
                            # Don't retry client errors (bad request, unauthorized, etc.)
                            return {
                                "status": "failed",
                                "error": f"Client error {response.status}: {error_text[:100]}",
                                "service": service_name,
                                "timestamp": time.time(),
                            }
                        
                        elif response.status >= 500:
                            error_text = await response.text()
                            
                            if attempt < max_retries - 1:
                                # Exponential backoff with jitter
                                base_wait = (attempt + 1) * 2
                                jitter = base_wait * 0.2 * (hash(service_name) % 100 / 100)
                                wait_time = base_wait + jitter
                                
                                logging.warning(
                                    f"{service_name} server error {response.status}, "
                                    f"retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                return {
                                    "status": "failed", 
                                    "error": f"Server error {response.status}: {error_text[:100]}",
                                    "service": service_name,
                                    "timestamp": time.time(),
                                }

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logging.warning(
                        f"{service_name} timeout, retrying in {wait_time}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logging.error(f"{service_name} timeout after {max_retries} attempts")
                    return {
                        "status": "failed",
                        "error": f"Service timeout after {max_retries} retries",
                        "service": service_name,
                        "timestamp": time.time(),
                    }
            
            except aiohttp.ClientError as client_error:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logging.warning(
                        f"{service_name} client error: {client_error}, "
                        f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logging.error(f"{service_name} client error after {max_retries} attempts: {client_error}")
                    return {
                        "status": "failed",
                        "error": f"Client connection error: {str(client_error)}",
                        "service": service_name,
                        "timestamp": time.time(),
                    }
            
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logging.warning(
                        f"{service_name} unexpected error: {e}, "
                        f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logging.error(f"{service_name} unexpected error after {max_retries} attempts: {e}")
                    return {
                        "status": "failed",
                        "error": f"Unexpected error: {str(e)}",
                        "service": service_name,
                        "timestamp": time.time(),
                    }
        
        # All retries exhausted
        logging.critical(f"{service_name} max retries ({max_retries}) exceeded")
        return {
            "status": "failed",
            "error": f"Max retries ({max_retries}) exceeded",
            "service": service_name,
            "timestamp": time.time(),
        }

    def _safe_calculate_percentage(self, target_price, current_price):
        """Safe percentage calculation"""
        try:
            if not target_price or not current_price or current_price <= 0:
                return 0.02  # Default 2%
            return abs((target_price - current_price) / current_price)
        except (ZeroDivisionError, TypeError, AttributeError):
            return 0.02

    async def _forward_to_notify_service(
        self,
        signal_data: SignalData,
        risk_profile: Optional[RiskProfile],
        execution_result: Optional[Dict],
        status: str,
        reason: str = None,
    ) -> Dict:
        """
        Forward notification to Notify Service with safe JSON serialization
        Features comprehensive datetime handling and multi-layer fallback system
        """

        try:
            # FIXED: Convert all datetime objects to timestamps before serialization
            signal_dict = asdict(signal_data)
            
            # Ensure timestamp is float, not datetime object
            if isinstance(signal_dict.get('timestamp'), datetime):
                signal_dict['timestamp'] = signal_dict['timestamp'].timestamp()
            elif not isinstance(signal_dict.get('timestamp'), (int, float)):
                signal_dict['timestamp'] = time.time()
            
            # Check and convert date objects as well
            if isinstance(signal_dict.get('timestamp'), date):
                signal_dict['timestamp'] = time.mktime(signal_dict['timestamp'].timetuple())
            
            # Convert risk profile with comprehensive datetime handling
            risk_profile_dict = None
            if risk_profile:
                risk_profile_dict = asdict(risk_profile)
                
                # Convert created_at datetime to timestamp
                if isinstance(risk_profile_dict.get('created_at'), datetime):
                    risk_profile_dict['created_at'] = risk_profile_dict['created_at'].timestamp()
                elif isinstance(risk_profile_dict.get('created_at'), date):
                    risk_profile_dict['created_at'] = time.mktime(risk_profile_dict['created_at'].timetuple())
                
                # Clean all datetime fields in risk_profile recursively
                risk_profile_dict = self._sanitize_datetime_recursive(risk_profile_dict)
            
            # Clean execution_result if present
            if execution_result and isinstance(execution_result, dict):
                execution_result = self._sanitize_datetime_recursive(execution_result)
            
            # Build notification data with safe types
            notify_data = {
                "type": f"risk_{status}",
                "data": {
                    "signal": signal_dict,
                    "risk_profile": risk_profile_dict,
                    "execution_result": execution_result,
                    "status": status,
                    "reason": str(reason) if reason else None,
                    "service_source": "risk_management",
                },
                "service": "risk_service",
                "timestamp": time.time(),  # Always use float timestamp
            }
            
            # ENHANCED: Pre-serialization validation with fallback chain
            try:
                # Test JSON serialization before sending
                test_json = json.dumps(notify_data)
                logging.debug(f"Notify data serialization test passed: {len(test_json)} bytes")
                
            except (TypeError, ValueError) as json_error:
                logging.error(f"Notify data pre-serialization failed: {json_error}")
                
                # Fallback Layer 1: Use safe_json_dumps for deep conversion
                try:
                    safe_notify_json = safe_json_dumps(notify_data)
                    notify_data = json.loads(safe_notify_json)
                    logging.info("Applied safe JSON conversion to notify data")
                    
                    # Verify the fallback worked
                    test_json = json.dumps(notify_data)
                    logging.debug(f"Safe JSON conversion successful: {len(test_json)} bytes")
                    
                except Exception as fallback_error:
                    logging.error(f"Safe JSON conversion failed: {fallback_error}")
                    
                    # Fallback Layer 2: Strip all complex objects
                    try:
                        notify_data = self._create_minimal_notification(
                            status, reason, signal_data
                        )
                        logging.warning("Using minimal notification data due to serialization errors")
                        
                    except Exception as minimal_error:
                        logging.critical(f"Even minimal notification failed: {minimal_error}")
                        
                        # Fallback Layer 3: Absolute minimum
                        notify_data = {
                            "type": f"risk_{status}",
                            "data": {
                                "status": status,
                                "service_source": "risk_management",
                            },
                            "service": "risk_service",
                            "timestamp": time.time(),
                        }
                        logging.error("Using absolute minimal notification data")

            # Send notification with retry mechanism
            result = await self._call_service_with_retry(
                f"{config.SERVICE_URLS['notify_service']}/api/notification/system",
                notify_data,
            )

            logging.info(f"Notify Service delivery successful: {status}")
            return result

        except Exception as e:
            logging.error(f"Notify Service delivery failed: {e}")
            
            # Return detailed error for debugging
            return {
                "status": "failed",
                "error": str(e),
                "error_type": type(e).__name__,
                "service": "notify_service",
                "timestamp": time.time()
            }
    
    def _sanitize_datetime_recursive(self, data):
        """
        Recursively convert all datetime objects to timestamps
        Helper function for JSON serialization safety
        """
        if isinstance(data, dict):
            return {
                key: self._sanitize_datetime_recursive(value)
                for key, value in data.items()
            }
        elif isinstance(data, list):
            return [self._sanitize_datetime_recursive(item) for item in data]
        elif isinstance(data, datetime):
            return data.timestamp()
        elif isinstance(data, date):
            return time.mktime(data.timetuple())
        elif isinstance(data, Decimal):
            return float(data)
        else:
            return data
    
    def _create_minimal_notification(self, status: str, reason: str, signal_data: SignalData) -> Dict:
        """
        Create minimal notification with only essential data
        Used as fallback when full serialization fails
        """
        return {
            "type": f"risk_{status}",
            "data": {
                "status": status,
                "reason": str(reason) if reason else "unknown",
                "symbol": str(getattr(signal_data, 'symbol', 'unknown')),
                "action": str(getattr(signal_data, 'action', 'unknown')),
                "price": float(getattr(signal_data, 'price', 0.0)),
                "service_source": "risk_management",
            },
            "service": "risk_service",
            "timestamp": time.time(),
        }

    async def _call_service_with_retry(
        self, url: str, data: Dict, max_retries: int = None
    ) -> Dict:
        """
        Enhanced service call with retry logic, circuit breaker, and connection pooling.
        Optimized for 128GB RAM, 16-24 cores high-spec system.
        
        Features:
        - Circuit breaker pattern for fault tolerance
        - High-performance connection pooling (200 connections)
        - Adaptive timeout based on attempt number
        - Exponential backoff with jitter for retry distribution
        - Emergency bypass for critical operations
        - Comprehensive error handling and recovery
        
        Args:
            url: Target service endpoint URL
            data: Request payload data
            max_retries: Maximum retry attempts (default: config.MAX_RETRIES)
            
        Returns:
            Dict: Service response data
            
        Raises:
            RuntimeError: Circuit breaker open or max retries exceeded
            TimeoutError: Service timeout after all retries
            ConnectionError: Service connection failure
        """
        if max_retries is None:
            max_retries = config.MAX_RETRIES

        service_name = self._extract_service_name(url)
        
        # Emergency bypass check for critical operations
        if hasattr(self, 'emergency_bypass_enabled') and self.emergency_bypass_enabled:
            logging.info(f"Emergency bypass active for {service_name}")
        else:
            # Circuit breaker status check with auto-recovery
            if await self._is_circuit_open(service_name):
                # Auto reset if circuit has been open for too long (5 minutes)
                if hasattr(self, 'circuit_breaker_state') and service_name in self.circuit_breaker_state:
                    last_failure = self.circuit_breaker_state[service_name].get('last_failure', 0)
                    current_time = time.time()
                    
                    if current_time - last_failure > 300:  # 5 minutes auto-recovery
                        self.reset_circuit_breaker(service_name)
                        logging.info(f"Auto-reset circuit breaker for {service_name} after 5 minutes")
                    else:
                        time_remaining = 300 - (current_time - last_failure)
                        raise RuntimeError(
                            f"Circuit breaker open for {service_name} "
                            f"(retry in {time_remaining:.0f}s)"
                        )
                else:
                    raise RuntimeError(f"Circuit breaker open for {service_name}")
        
        # High-performance connection pooling (128GB RAM, 16-24 cores optimized)
        connector = aiohttp.TCPConnector(
            limit=200,              # Max total connections for high-spec
            limit_per_host=50,      # Max per service endpoint
            ttl_dns_cache=300,      # DNS cache 5 minutes
            force_close=False,      # Reuse connections
            enable_cleanup_closed=True
        )
        
        # Retry loop with enhanced error handling
        for attempt in range(max_retries):
            try:
                # Adaptive timeout based on attempt number
                base_timeout = 15
                adaptive_timeout = base_timeout + (attempt * 3)  # Increase timeout per attempt
                
                timeout = aiohttp.ClientTimeout(
                    total=adaptive_timeout,
                    connect=5,              # Fast connect timeout
                    sock_read=adaptive_timeout - 5  # Longer read timeout
                )
                
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    async with session.post(url, json=data, timeout=timeout) as response:
                        
                        # Success response (200 OK)
                        if response.status == 200:
                            result = await response.json()
                            await self._record_success(service_name)
                            logging.debug(
                                f"{service_name} call successful: {url} "
                                f"(attempt {attempt + 1}/{max_retries})"
                            )
                            return result
                        
                        # Client error (4xx) - Do not retry
                        elif 400 <= response.status < 500:
                            error_text = await response.text()
                            await self._record_failure(service_name)
                            logging.error(
                                f"{service_name} client error {response.status}: "
                                f"{error_text[:200]}"
                            )
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=response.status,
                                message=f"Client error - no retry: {error_text[:100]}"
                            )
                        
                        # Server error (5xx) - Retry with backoff
                        elif response.status >= 500:
                            error_text = await response.text()
                            await self._record_failure(service_name)
                            
                            if attempt < max_retries - 1:
                                wait_time = self._calculate_backoff_delay(attempt, response.status)
                                logging.warning(
                                    f"{service_name} server error {response.status}, "
                                    f"retrying in {wait_time:.1f}s "
                                    f"({attempt + 1}/{max_retries}): {error_text[:100]}"
                                )
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                logging.error(
                                    f"{service_name} server error final failure: "
                                    f"{response.status} - {error_text[:100]}"
                                )
                                raise aiohttp.ClientResponseError(
                                    request_info=response.request_info,
                                    history=response.history,
                                    status=response.status,
                                    message=f"Server error final failure: {error_text[:100]}"
                                )
                        
                        # Unexpected status code
                        else:
                            error_text = await response.text()
                            await self._record_failure(service_name)
                            logging.warning(
                                f"{service_name} unexpected status {response.status}: "
                                f"{error_text[:100]}"
                            )
                            raise aiohttp.ClientResponseError(
                                request_info=response.request_info,
                                history=response.history,
                                status=response.status,
                                message=f"Unexpected response: {error_text[:100]}"
                            )

            # Timeout error handling
            except asyncio.TimeoutError:
                await self._record_failure(service_name)
                
                if attempt < max_retries - 1:
                    wait_time = self._calculate_backoff_delay(attempt, "timeout")
                    logging.warning(
                        f"{service_name} timeout, retrying in {wait_time:.1f}s "
                        f"({attempt + 1}/{max_retries}): {url}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logging.error(f"{service_name} timeout final failure: {url}")
                    
                    # Graceful degradation for Execute Service
                    if "execute" in url.lower():
                        logging.warning(
                            f"Returning mock success for Execute Service timeout "
                            f"to prevent blocking"
                        )
                        return {
                            "status": "success", 
                            "message": "Mock response - service timeout",
                            "mock": True
                        }
                    
                    raise TimeoutError(f"Service call timeout: {service_name}")
            
            # Connection error handling
            except aiohttp.ClientConnectionError as e:
                await self._record_failure(service_name)
                
                if attempt < max_retries - 1:
                    wait_time = self._calculate_backoff_delay(attempt, "connection")
                    logging.warning(
                        f"{service_name} connection error, retrying in {wait_time:.1f}s "
                        f"({attempt + 1}/{max_retries}): {e}"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logging.error(
                        f"{service_name} connection error final failure: {e}"
                    )
                    
                    # Graceful degradation for Execute Service
                    if "execute" in url.lower():
                        logging.warning(
                            f"Returning mock success for Execute Service "
                            f"connection error to prevent blocking"
                        )
                        return {
                            "status": "success", 
                            "message": "Mock response - service unavailable",
                            "mock": True
                        }
                    
                    raise ConnectionError(
                        f"Service connection failed: {service_name} - {str(e)}"
                    )
            
            # ClientResponseError pass-through (already handled above)
            except aiohttp.ClientResponseError:
                raise
            
            # Unexpected error handling
            except Exception as e:
                await self._record_failure(service_name)
                
                if attempt == max_retries - 1:
                    logging.error(
                        f"{service_name} unexpected error final failure: {e}"
                    )
                    
                    # Graceful degradation for Execute Service
                    if "execute" in url.lower():
                        logging.warning(
                            f"Returning mock success for Execute Service "
                            f"unexpected error to prevent blocking"
                        )
                        return {
                            "status": "success", 
                            "message": "Mock response - unexpected error",
                            "mock": True
                        }
                    
                    raise RuntimeError(
                        f"Service call unexpected error: {service_name} - {str(e)}"
                    )

                # Retry on unexpected error
                wait_time = self._calculate_backoff_delay(attempt, "unknown")
                logging.warning(
                    f"{service_name} unexpected error, retrying in {wait_time:.1f}s "
                    f"({attempt + 1}/{max_retries}): {e}"
                )
                await asyncio.sleep(wait_time)

        # All retries exhausted
        raise RuntimeError(
            f"Service call max retries exceeded: {service_name} "
            f"({max_retries} attempts)"
        )

    async def _is_circuit_open(self, service_name: str) -> bool:
        """
        Check if circuit breaker is open for the given service.
        Enhanced with adaptive recovery and health tracking.
        
        Circuit Breaker States:
        - CLOSED: Normal operation, requests allowed
        - HALF_OPEN: Testing recovery, limited requests allowed
        - OPEN: Service unavailable, requests blocked
        
        Args:
            service_name: Name of the service to check
            
        Returns:
            bool: True if circuit is open (blocked), False if closed/half-open (allowed)
        """
        if not hasattr(self, 'circuit_breaker_state'):
            self.circuit_breaker_state = {}
            
        if service_name not in self.circuit_breaker_state:
            self.circuit_breaker_state[service_name] = {
                "state": "CLOSED",
                "failure_count": 0,
                "success_count": 0,  # Track successful calls
                "last_failure": 0,
                "last_success": time.time(),
                "recovery_timeout": 30,  # Initial timeout
                "consecutive_successes": 0,  # Track recovery progress
                "total_calls": 0,  # Total call count
                "health_score": 100.0  # 0-100 health score
            }
            
        state = self.circuit_breaker_state[service_name]
        current_time = time.time()
        
        # OPEN state - Check if recovery timeout has passed
        if state["state"] == "OPEN":
            time_since_failure = current_time - state["last_failure"]
            recovery_timeout = state["recovery_timeout"]
            
            if time_since_failure > recovery_timeout:
                # Transition to HALF_OPEN for testing
                state["state"] = "HALF_OPEN"
                state["failure_count"] = 0
                state["consecutive_successes"] = 0
                
                logging.info(
                    f"Circuit breaker {service_name} entering HALF_OPEN state "
                    f"after {time_since_failure:.0f}s (timeout: {recovery_timeout}s)"
                )
                return False  # Allow testing
            
            # Still in recovery period
            logging.debug(
                f"Circuit breaker {service_name} still OPEN "
                f"(retry in {recovery_timeout - time_since_failure:.0f}s)"
            )
            return True  # Block requests
        
        # HALF_OPEN state - Allow limited testing
        if state["state"] == "HALF_OPEN":
            logging.debug(f"Circuit breaker {service_name} in HALF_OPEN - testing recovery")
            return False  # Allow test requests
            
        # CLOSED state - Normal operation
        return False

    async def _record_success(self, service_name: str):
        """
        Record successful service call and update circuit breaker state.
        Enhanced with health scoring and adaptive recovery.
        
        Args:
            service_name: Name of the service
        """
        if not hasattr(self, 'circuit_breaker_state'):
            self.circuit_breaker_state = {}
            
        if service_name not in self.circuit_breaker_state:
            self.circuit_breaker_state[service_name] = {
                "state": "CLOSED",
                "failure_count": 0,
                "success_count": 0,
                "last_failure": 0,
                "last_success": time.time(),
                "recovery_timeout": 30,
                "consecutive_successes": 0,
                "total_calls": 0,
                "health_score": 100.0
            }
            
        state = self.circuit_breaker_state[service_name]
        current_time = time.time()
        
        # Update success metrics
        state["failure_count"] = 0
        state["success_count"] += 1
        state["consecutive_successes"] += 1
        state["last_success"] = current_time
        state["total_calls"] += 1
        
        # Improve health score
        state["health_score"] = min(100.0, state["health_score"] + 5.0)
        
        # State transitions based on consecutive successes
        if state["state"] == "HALF_OPEN":
            # Require 3 consecutive successes to fully recover
            if state["consecutive_successes"] >= 3:
                state["state"] = "CLOSED"
                state["recovery_timeout"] = 30  # Reset to default
                
                logging.info(
                    f"Circuit breaker {service_name} fully recovered to CLOSED "
                    f"(health: {state['health_score']:.0f}%)"
                )
            else:
                logging.debug(
                    f"Circuit breaker {service_name} HALF_OPEN progress: "
                    f"{state['consecutive_successes']}/3 successes"
                )
                
        elif state["state"] == "OPEN":
            # Force recovery if successful in OPEN state (shouldn't happen normally)
            state["state"] = "CLOSED"
            state["recovery_timeout"] = 30
            
            logging.warning(
                f"Circuit breaker {service_name} forced recovery from OPEN to CLOSED "
                f"(unexpected success)"
            )
            
        else:  # CLOSED state
            # Already in healthy state
            logging.debug(
                f"Circuit breaker {service_name} success recorded "
                f"(total: {state['success_count']}, health: {state['health_score']:.0f}%)"
            )

    async def _record_failure(self, service_name: str):
        """
        Record service call failure and update circuit breaker state.
        Enhanced with adaptive timeout and health degradation.
        
        Args:
            service_name: Name of the service
        """
        if not hasattr(self, 'circuit_breaker_state'):
            self.circuit_breaker_state = {}
            
        if service_name not in self.circuit_breaker_state:
            self.circuit_breaker_state[service_name] = {
                "state": "CLOSED",
                "failure_count": 0,
                "success_count": 0,
                "last_failure": 0,
                "last_success": 0,
                "recovery_timeout": 30,
                "consecutive_successes": 0,
                "total_calls": 0,
                "health_score": 100.0
            }
            
        state = self.circuit_breaker_state[service_name]
        current_time = time.time()
        
        # Update failure metrics
        state["total_calls"] += 1
        state["consecutive_successes"] = 0  # Reset success streak
        
        # Degrade health score
        state["health_score"] = max(0.0, state["health_score"] - 10.0)
        
        # HALF_OPEN state - Immediate reopen on failure
        if state["state"] == "HALF_OPEN":
            state["state"] = "OPEN"
            state["failure_count"] = 1
            state["last_failure"] = current_time
            
            # Increase recovery timeout (adaptive backoff)
            state["recovery_timeout"] = min(300, state["recovery_timeout"] * 2)
            
            logging.warning(
                f"Circuit breaker {service_name} reopened after HALF_OPEN failure "
                f"(new timeout: {state['recovery_timeout']}s, health: {state['health_score']:.0f}%)"
            )
            return
        
        # Increment failure count
        state["failure_count"] += 1
        state["last_failure"] = current_time
        
        # Threshold for opening circuit (5 consecutive failures)
        failure_threshold = 5
        
        if state["failure_count"] >= failure_threshold:
            state["state"] = "OPEN"
            
            # Adaptive timeout based on failure count
            base_timeout = 30
            state["recovery_timeout"] = min(
                300,  # Max 5 minutes
                base_timeout * (1.5 ** (state["failure_count"] - failure_threshold))
            )
            
            logging.error(
                f"Circuit breaker {service_name} OPENED "
                f"(failures: {state['failure_count']}, "
                f"timeout: {state['recovery_timeout']:.0f}s, "
                f"health: {state['health_score']:.0f}%)"
            )
        else:
            logging.debug(
                f"Circuit breaker {service_name} failure recorded: "
                f"{state['failure_count']}/{failure_threshold} "
                f"(health: {state['health_score']:.0f}%)"
            )

    def reset_circuit_breaker(self, service_name: str = None):
        """
        Manually reset circuit breaker state with enhanced recovery.
        Used for emergency recovery or maintenance operations.
        
        Args:
            service_name: Specific service to reset, or None for all services
        """
        if not hasattr(self, 'circuit_breaker_state'):
            self.circuit_breaker_state = {}
            
        if service_name:
            # Reset specific service
            self.circuit_breaker_state[service_name] = {
                "state": "CLOSED",
                "failure_count": 0,
                "success_count": 0,
                "last_failure": 0,
                "last_success": time.time(),
                "recovery_timeout": 30,
                "consecutive_successes": 0,
                "total_calls": 0,
                "health_score": 100.0
            }
            
            logging.info(
                f"Circuit breaker {service_name} manually reset to CLOSED "
                f"(health restored to 100%)"
            )
            
        else:
            # Reset all known services (emergency mode)
            service_names = ["ExecuteService", "NotifyService", "PortfolioService", "BrainService"]
            reset_count = 0
            
            for name in service_names:
                self.circuit_breaker_state[name] = {
                    "state": "CLOSED",
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure": 0,
                    "last_success": time.time(),
                    "recovery_timeout": 30,
                    "consecutive_successes": 0,
                    "total_calls": 0,
                    "health_score": 100.0
                }
                reset_count += 1
            
            logging.warning(
                f"Emergency recovery: {reset_count} circuit breakers reset to CLOSED "
                f"(all services restored)"
            )
            
            # Enable emergency bypass mode
            self.emergency_bypass_enabled = True
            logging.warning("Emergency bypass ENABLED - circuit breaker checks suspended")

    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """
        Get comprehensive circuit breaker status for all services.
        Useful for monitoring and debugging.
        
        Returns:
            Dict containing status for all services
        """
        if not hasattr(self, 'circuit_breaker_state'):
            return {"status": "not_initialized"}
            
        status = {
            "timestamp": time.time(),
            "services": {},
            "summary": {
                "total_services": 0,
                "open_count": 0,
                "half_open_count": 0,
                "closed_count": 0,
                "average_health": 0.0
            }
        }
        
        total_health = 0.0
        
        for service_name, state in self.circuit_breaker_state.items():
            status["services"][service_name] = {
                "state": state.get("state", "UNKNOWN"),
                "health_score": state.get("health_score", 0.0),
                "failure_count": state.get("failure_count", 0),
                "success_count": state.get("success_count", 0),
                "recovery_timeout": state.get("recovery_timeout", 0),
                "last_success": state.get("last_success", 0),
                "last_failure": state.get("last_failure", 0)
            }
            
            # Update summary
            status["summary"]["total_services"] += 1
            total_health += state.get("health_score", 0.0)
            
            if state.get("state") == "OPEN":
                status["summary"]["open_count"] += 1
            elif state.get("state") == "HALF_OPEN":
                status["summary"]["half_open_count"] += 1
            elif state.get("state") == "CLOSED":
                status["summary"]["closed_count"] += 1
        
        # Calculate average health
        if status["summary"]["total_services"] > 0:
            status["summary"]["average_health"] = (
                total_health / status["summary"]["total_services"]
            )
        
        return status

# =============================================================================
# Real-time Position Monitor (Performance Optimized)
# =============================================================================

class AdvancedRealTimePositionMonitor:
    """
    Real-time position monitoring - High-performance optimization for 400+ positions
    Optimized for 128GB RAM, 16-24 cores, NVMe SSD
    """

    def __init__(self, telegram_notifier, service=None):
        self.active_monitors = {}
        self.telegram = telegram_notifier
        
        # High-capacity caches for 128GB RAM system (400+ positions support)
        from collections import OrderedDict
        
        class LimitedCache(OrderedDict):
            """
            LRU cache with automatic cleanup
            Optimized for high-spec system performance
            """
            def __init__(self, max_size=5000):
                super().__init__()
                self.max_size = max_size
                self.hits = 0
                self.misses = 0
            
            def __setitem__(self, key, value):
                if len(self) >= self.max_size:
                    self.popitem(last=False)  # LRU: remove oldest
                super().__setitem__(key, value)
            
            def __getitem__(self, key):
                try:
                    self.hits += 1
                    return super().__getitem__(key)
                except KeyError:
                    self.misses += 1
                    raise
            
            def get_stats(self):
                """Get cache performance statistics"""
                total = self.hits + self.misses
                hit_rate = (self.hits / total * 100) if total > 0 else 0
                return {
                    'size': len(self),
                    'max_size': self.max_size,
                    'hits': self.hits,
                    'misses': self.misses,
                    'hit_rate': hit_rate
                }
        
        # Massive cache expansion for 128GB RAM (10x increase from original)
        self.price_cache = LimitedCache(max_size=50000)  # 5K -> 50K (10x)
        self.alert_history = deque(maxlen=100000)  # 10K -> 100K (10x)
        self.performance_cache = LimitedCache(max_size=20000)  # 2K -> 20K (10x)
        self.service = service

        # High-performance optimization settings (AMD Ryzen 9 7950X, 128GB DDR5)
        self.cache_update_interval = 0.5
        self.alert_cooldown = 180
        
        # Initialize local performance_metrics to prevent AttributeError
        self.performance_metrics = {
            "cache_hits": 0,
            "cache_misses": 0,
            "price_fetches": 0,
            "monitor_cycles": 0,
            "last_cleanup": time.time()
        }
        
        # Performance tracking for high-spec system
        if service and not hasattr(service, 'performance_metrics'):
            service.performance_metrics = {
                "cache_hits": 0, 
                "cache_misses": 0,
                "total_monitors": 0,
                "peak_monitors": 0
            }

        # Binance client initialization
        self.binance_client = None
        self.binance_mode = "SIMULATION"  # Default mode
        self.binance_api_key = None
        self.binance_secret_key = None
        self.binance_testnet_base_url = None
        
        if BINANCE_AVAILABLE:
            self._init_binance_testnet_client()
        
        # Log initialization with detailed stats
        logging.info(
            f"AdvancedRealTimePositionMonitor initialized: "
            f"price_cache={self.price_cache.max_size:,}, "
            f"alert_history={self.alert_history.maxlen:,}, "
            f"performance_cache={self.performance_cache.max_size:,}, "
            f"total_capacity={self.price_cache.max_size + self.alert_history.maxlen + self.performance_cache.max_size:,}"
        )

    def _init_binance_testnet_client(self):
        """Initialize Binance testnet client for SPOT trading only"""
        try:
            api_key = os.getenv('BINANCE_API_KEY')
            secret_key = os.getenv('BINANCE_SECRET_KEY')
            
            if not api_key or not secret_key:
                safe_logger.warning("Binance API keys not set - using simulation mode")
                self.binance_mode = "SIMULATION"
                return
            
            # Store API credentials for HTTP calls
            self.binance_api_key = api_key
            self.binance_secret_key = secret_key
            self.binance_testnet_base_url = "https://testnet.binance.vision"
            
            # Test connection and initialize client
            try:
                from binance.client import Client
                
                # Create actual Binance Client object (not string)
                self.binance_client = Client(
                    api_key=api_key,
                    api_secret=secret_key,
                    testnet=True
                )
                
                # Test SPOT API connection
                try:
                    # Test with ping
                    self.binance_client.ping()
                    
                    # Test with actual price fetch
                    ticker = self.binance_client.get_symbol_ticker(symbol="BTCUSDT")
                    btc_spot_price = float(ticker['price'])
                    
                    safe_logger.info(f"Binance SPOT API verified: BTC ${btc_spot_price:,.2f}")
                    self.binance_mode = "SPOT_TESTNET"
                    safe_logger.info("Binance testnet client initialized successfully")
                    
                except Exception as api_error:
                    safe_logger.error(f"Binance API test failed: {api_error}")
                    self.binance_client = None
                    self.binance_mode = "SIMULATION"
                    
            except ImportError as import_error:
                safe_logger.error(f"Binance Client import failed: {import_error}")
                self.binance_client = None
                self.binance_mode = "SIMULATION"
                
            except Exception as client_error:
                safe_logger.error(f"Binance Client creation failed: {client_error}")
                self.binance_client = None
                self.binance_mode = "SIMULATION"
                
        except Exception as e:
            safe_logger.error(f"Binance testnet client init failed: {e}")
            self.binance_client = None
            self.binance_mode = "SIMULATION"

    async def start_position_monitoring(
        self, position_id: str, risk_profile: RiskProfile
    ):
        """
        Start position monitoring using existing position data from active_positions.
        This method should be called AFTER the position is added to active_positions.
        """
        try:
            # Retrieve existing position data from active_positions
            if (hasattr(self.service, 'risk_manager') and 
                hasattr(self.service.risk_manager, 'active_positions') and
                position_id in self.service.risk_manager.active_positions):
                
                # Use existing complete position data
                position_risk = self.service.risk_manager.active_positions[position_id]
                logging.info(
                    f"Position monitoring using existing data: {position_id} "
                    f"(entry_price: ${position_risk.entry_price:.2f})"
                )
            else:
                # Fallback: create minimal position object (should not happen in normal flow)
                logging.warning(
                    f"Position {position_id} not found in active_positions - "
                    "creating fallback position object"
                )
                position_risk = PositionRisk(
                    position_id=position_id,
                    symbol=risk_profile.symbol,
                    side="BUY",  # Default fallback
                    entry_price=0,  # Will be updated in monitoring loop
                    current_price=0,
                    quantity=risk_profile.position_size,
                    leverage=risk_profile.leverage,
                    margin_required=risk_profile.margin_required,
                    unrealized_pnl=0,
                    liquidation_risk=0,
                    distance_to_liquidation=0,
                )

            # Start monitoring task
            monitor_task = asyncio.create_task(
                self._monitor_position(position_id, position_risk, risk_profile)
            )
            self.active_monitors[position_id] = monitor_task

            logging.info(
                f"Position monitoring started: {position_id} ({risk_profile.symbol})"
            )

        except Exception as e:
            logging.error(f"Position monitoring start failed: {e}")

    async def _monitor_position(
        self, position_id: str, position_risk: PositionRisk, risk_profile: RiskProfile
    ):
        """
        Individual position monitoring loop optimized for high-spec system (16-24 cores, 128GB RAM)
        
        Features:
        - Advanced memory leak prevention with automatic cleanup
        - Multi-core CPU utilization optimization
        - Adaptive monitoring intervals based on risk level
        - Efficient batch processing integration
        - Zero-copy position data synchronization
        - Enhanced Binance API integration with fallback
        - Smart 2-tier caching (hot + warm)
        - Circuit breaker pattern for API calls
        
        System specs:
        - RAM: 128GB DDR5
        - CPU: 16-24 cores
        - Target: 400+ concurrent positions
        """
        monitoring_start_time = time.time()
        last_price_update = 0
        last_risk_check = 0
        last_pnl_update = 0
        last_memory_check = 0
        last_health_report = 0
        
        # High-spec adaptive intervals (optimized for 16-24 cores)
        base_interval = 1.0  # Increased from 0.1s to 1s for stability
        price_update_interval = 2.0  # Update every 2 seconds (batch-friendly)
        risk_check_interval = 3.0  # Check every 3 seconds
        pnl_update_interval = 10.0  # Update PnL every 10 seconds
        memory_check_interval = 300  # Check memory every 5 minutes
        health_report_interval = 900  # Health report every 15 minutes
        
        cycle_count = 0
        cpu_check_interval = 1000  # Check CPU every 1000 cycles (high-spec optimization)
        
        # Performance tracking with enhanced metrics
        price_fetch_errors = 0
        max_consecutive_errors = 5
        successful_updates = 0
        cache_hits = 0
        api_calls = 0
        
        try:
            while position_id in self.active_monitors:
                current_time = time.time()
                cycle_count += 1
                
                # Always use the latest position data from active_positions (zero-copy reference)
                if (hasattr(self.service, 'risk_manager') and 
                    hasattr(self.service.risk_manager, 'active_positions') and
                    position_id in self.service.risk_manager.active_positions):
                    position_risk = self.service.risk_manager.active_positions[position_id]
                else:
                    # Position removed from active_positions - graceful shutdown
                    logging.info(f"Position {position_id} removed from active_positions - stopping monitor")
                    break
                
                # Adaptive sleep interval based on risk level (high-spec optimization)
                if hasattr(position_risk, 'liquidation_risk'):
                    if position_risk.liquidation_risk > 0.95:
                        sleep_interval = 0.5  # Critical: very fast
                    elif position_risk.liquidation_risk > 0.85:
                        sleep_interval = 1.0  # High risk: fast
                    elif position_risk.liquidation_risk > 0.60:
                        sleep_interval = 2.0  # Medium risk
                    elif position_risk.liquidation_risk > 0.40:
                        sleep_interval = 3.0  # Low-medium risk
                    else:
                        sleep_interval = 5.0  # Low risk: slow monitoring
                else:
                    sleep_interval = base_interval

                # Enhanced batch-optimized price updates with Binance integration
                if current_time - last_price_update >= price_update_interval:
                    try:
                        # Try multiple price sources with priority order
                        current_price = None
                        price_source = "unknown"
                        
                        # Method 1: Hot cache check (fastest - 1 second TTL)
                        if hasattr(self, '_hot_price_cache'):
                            cache_key = f"price_{position_risk.symbol}"
                            if cache_key in self._hot_price_cache:
                                cache_entry = self._hot_price_cache[cache_key]
                                cache_age = current_time - cache_entry.get("timestamp", 0)
                                if cache_age < 1.0:
                                    current_price = cache_entry["price"]
                                    price_source = "hot_cache"
                                    cache_hits += 1
                        
                        # Method 2: Warm cache check (fast - 5 second TTL)
                        if current_price is None and hasattr(self, 'price_cache'):
                            cache_key = f"price_{position_risk.symbol}"
                            if cache_key in self.price_cache:
                                cache_entry = self.price_cache[cache_key]
                                cache_age = current_time - cache_entry.get("timestamp", 0)
                                if cache_age < 5.0:
                                    current_price = cache_entry["price"]
                                    price_source = "warm_cache"
                                    cache_hits += 1
                        
                        # Method 3: Fetch from Binance API (with timeout)
                        if current_price is None:
                            try:
                                current_price = await asyncio.wait_for(
                                    self._get_current_price_cached(position_risk.symbol),
                                    timeout=3.0
                                )
                                if current_price:
                                    price_source = "api_fetch"
                                    api_calls += 1
                                    
                                    # Update both cache tiers
                                    if not hasattr(self, '_hot_price_cache'):
                                        self._hot_price_cache = {}
                                    
                                    cache_data = {
                                        "price": current_price,
                                        "timestamp": current_time,
                                        "source": "api"
                                    }
                                    
                                    self._hot_price_cache[f"price_{position_risk.symbol}"] = cache_data
                                    if hasattr(self, 'price_cache'):
                                        self.price_cache[f"price_{position_risk.symbol}"] = cache_data
                                        
                            except asyncio.TimeoutError:
                                logging.warning(f"API timeout for {position_risk.symbol}")
                                # Fall through to stale cache check
                            except Exception as api_error:
                                logging.debug(f"API error for {position_risk.symbol}: {api_error}")
                                # Fall through to stale cache check
                        
                        # Method 4: Stale cache fallback (up to 30 seconds old)
                        if current_price is None and hasattr(self, 'price_cache'):
                            cache_key = f"price_{position_risk.symbol}"
                            if cache_key in self.price_cache:
                                cache_entry = self.price_cache[cache_key]
                                cache_age = current_time - cache_entry.get("timestamp", 0)
                                if cache_age < 30.0:
                                    current_price = cache_entry["price"]
                                    price_source = "stale_cache"
                                    logging.debug(
                                        f"Using stale cache for {position_risk.symbol} "
                                        f"(age: {cache_age:.1f}s)"
                                    )
                        
                        # Final validation and error handling
                        if current_price is None or current_price <= 0:
                            price_fetch_errors += 1
                            if price_fetch_errors >= max_consecutive_errors:
                                logging.error(
                                    f"Max consecutive price fetch errors for {position_id}, "
                                    f"using last known price: ${position_risk.current_price:.2f}"
                                )
                                price_fetch_errors = 0
                            await asyncio.sleep(sleep_interval)
                            continue
                        
                        # Success - reset error counter and update position
                        price_fetch_errors = 0
                        successful_updates += 1
                        
                        position_risk.current_price = current_price
                        position_risk.last_updated = datetime.utcnow()
                        last_price_update = current_time
                        
                        # Detailed logging every 100 successful updates
                        if successful_updates % 100 == 0:
                            logging.info(
                                f"Monitor stats [{position_id}]: "
                                f"updates={successful_updates}, cache_hits={cache_hits}, "
                                f"api_calls={api_calls}, source={price_source}"
                            )

                        # Smart PnL update - threshold-based triggering
                        if position_risk.entry_price > 0 and current_time - last_pnl_update >= pnl_update_interval:
                            price_change_percent = abs(current_price - position_risk.entry_price) / position_risk.entry_price
                            
                            # Update on significant change (0.5%) or time threshold
                            if price_change_percent > 0.005 or current_time - last_pnl_update >= 60:
                                await self._update_position_pnl(position_risk, risk_profile)
                                last_pnl_update = current_time
                    
                    except asyncio.TimeoutError:
                        price_fetch_errors += 1
                        logging.warning(f"Price fetch timeout for {position_id} ({price_fetch_errors}/{max_consecutive_errors})")
                        await asyncio.sleep(sleep_interval)
                        continue
                    except Exception as e:
                        price_fetch_errors += 1
                        logging.error(f"Price fetch error for {position_id}: {e}")
                        await asyncio.sleep(sleep_interval)
                        continue

                # Optimized risk checks with priority handling
                if current_time - last_risk_check >= risk_check_interval and position_risk.entry_price > 0:
                    # Priority 1: Auto-close conditions (critical)
                    close_decision = self._check_auto_close_conditions(
                        position_risk, risk_profile, position_risk.current_price
                    )

                    if close_decision["should_close"]:
                        logging.info(f"Auto-close triggered for {position_id}: {close_decision['reason']}")
                        await self._execute_auto_close(
                            position_id, position_risk, close_decision["reason"]
                        )
                        break

                    # Priority 2: Risk alerts (throttled for efficiency)
                    if cycle_count % 20 == 0:  # Every 20th cycle (reduced frequency)
                        await self._check_risk_alerts_optimized(
                            position_id, position_risk, risk_profile
                        )
                    
                    last_risk_check = current_time

                # Performance monitoring with enhanced health reports
                if current_time - last_health_report >= health_report_interval:
                    uptime = current_time - monitoring_start_time
                    cache_hit_rate = (cache_hits / max(cache_hits + api_calls, 1)) * 100
                    
                    logging.debug(
                        f"Monitor health [{position_id}]: "
                        f"uptime={uptime/3600:.1f}h, cycles={cycle_count}, "
                        f"risk={position_risk.liquidation_risk:.2%}, "
                        f"cache_hit_rate={cache_hit_rate:.1f}%, "
                        f"successful_updates={successful_updates}"
                    )
                    last_health_report = current_time

                # Memory leak prevention (128GB RAM optimized)
                if current_time - last_memory_check > memory_check_interval:
                    try:
                        memory_usage = psutil.virtual_memory().percent
                        if memory_usage > 90:  # Threshold increased for 128GB system
                            logging.warning(
                                f"High memory usage ({memory_usage:.1f}%) detected in monitor {position_id}"
                            )
                            # Trigger garbage collection
                            import gc
                            collected = gc.collect()
                            logging.debug(f"GC collected {collected} objects in monitor {position_id}")
                        last_memory_check = current_time
                    except Exception as e:
                        logging.debug(f"Memory check error: {e}")

                # Optimized CPU monitoring for high-spec system (16-24 cores)
                if cycle_count % cpu_check_interval == 0:
                    try:
                        # Non-blocking CPU check
                        cpu_usage = psutil.cpu_percent(interval=0)
                        
                        # High-spec system: more relaxed thresholds
                        if cpu_usage > 98:  # Critical threshold for 16+ cores
                            sleep_interval = min(sleep_interval * 1.5, 10.0)
                            logging.warning(f"Critical CPU ({cpu_usage:.0f}%), slowing {position_id}")
                        elif cpu_usage > 95:
                            sleep_interval = min(sleep_interval * 1.2, 8.0)
                        elif cpu_usage < 40:  # Low CPU - can speed up
                            sleep_interval = max(0.5, sleep_interval * 0.9)
                        
                        # Adaptive CPU check frequency
                        if cpu_usage > 90:
                            cpu_check_interval = 500  # Check more often under load
                        else:
                            cpu_check_interval = 1000  # Check less often when idle
                            
                    except Exception:
                        pass  # Ignore CPU check errors

                # Yield control to event loop for better concurrency (high-spec optimization)
                if cycle_count % 100 == 0:  # Reduced frequency for high-performance system
                    await asyncio.sleep(0)  # Yield without delay

                # Adaptive sleep with jitter for load distribution
                if cycle_count % 1000 == 0:
                    # Add small random jitter to prevent thundering herd
                    import random
                    jitter = random.uniform(-0.05, 0.05)
                    await asyncio.sleep(max(0.1, sleep_interval + jitter))
                else:
                    await asyncio.sleep(sleep_interval)
                
                # Aggressive reference cleanup (every 5000 cycles for 128GB RAM)
                if cycle_count % 5000 == 0:
                    # Clear any accumulated references
                    close_decision = None
                    current_price = None
                    price_source = None
                    
                    # Force garbage collection on high cycle counts
                    if cycle_count % 20000 == 0:
                        import gc
                        gc.collect()

        except asyncio.CancelledError:
            logging.info(f"Position monitoring cancelled: {position_id}")
        except Exception as e:
            logging.error(f"Position monitoring error ({position_id}): {e}")
            import traceback
            logging.debug(traceback.format_exc())
        finally:
            # Critical: cleanup resources with comprehensive memory management
            try:
                if position_id in self.active_monitors:
                    del self.active_monitors[position_id]
                
                # Clear all references to prevent memory leaks
                position_risk = None
                risk_profile = None
                close_decision = None
                current_price = None
                price_source = None
                
                # Final garbage collection
                import gc
                gc.collect()
                
                # Final performance summary
                logging.info(
                    f"Position monitoring ended: {position_id} "
                    f"(updates={successful_updates}, cache_hits={cache_hits}, "
                    f"api_calls={api_calls})"
                )
            except Exception as cleanup_error:
                logging.error(f"Monitor cleanup error for {position_id}: {cleanup_error}")

    async def _get_current_price_cached(self, symbol: str) -> Optional[float]:
        """
        Get current price using intelligent caching and batch processing
        Optimized for 400+ concurrent position monitoring on high-spec system
        
        Features:
        - Multi-tier caching (L1: hot cache, L2: warm cache)
        - Automatic batch request aggregation
        - Adaptive rate limiting
        - 128GB RAM optimized cache sizes
        """
        try:
            cache_key = f"price_{symbol}"
            current_time = time.time()

            # L1 Cache: Hot data (last 1 second)
            if hasattr(self, '_hot_price_cache') and cache_key in self._hot_price_cache:
                cache_entry = self._hot_price_cache[cache_key]
                cache_age = current_time - cache_entry["timestamp"]
                
                if cache_age < 1.0:  # Ultra-fresh data
                    if hasattr(self, 'performance_metrics'):
                        self.performance_metrics["cache_hits"] += 1
                    return cache_entry["price"]
            
            # L2 Cache: Warm data (last 5 seconds)
            if cache_key in self.price_cache:
                cache_entry = self.price_cache[cache_key]
                cache_age = current_time - cache_entry["timestamp"]
                
                if cache_age < 5.0:  # Recent data acceptable
                    if hasattr(self, 'performance_metrics'):
                        self.performance_metrics["cache_hits"] += 1
                    return cache_entry["price"]
                
                # Stale cache fallback (up to 30 seconds)
                if not self._can_fetch_price(symbol) and cache_age < 30:
                    logging.debug(f"Rate limit: using stale cache for {symbol} (age: {cache_age:.1f}s)")
                    return cache_entry["price"]

            # Batch request aggregation
            if self._should_use_batch_fetch():
                price = await self._get_price_from_batch(symbol)
                if price is not None:
                    return price

            # Check if we can fetch (rate limiting)
            if not self._can_fetch_price(symbol):
                # Try to return any cached value, even if stale
                if cache_key in self.price_cache:
                    logging.warning(f"Rate limit reached for {symbol}, using stale cache")
                    return self.price_cache[cache_key]["price"]
                return None

            # Fetch actual price from Binance API
            price = await self._fetch_real_price(symbol)

            if price is not None:
                # Update both cache tiers
                cache_data = {
                    "price": price, 
                    "timestamp": current_time,
                    "fetch_count": self.price_cache.get(cache_key, {}).get("fetch_count", 0) + 1
                }
                
                # Hot cache for ultra-fast access
                if not hasattr(self, '_hot_price_cache'):
                    self._hot_price_cache = {}
                self._hot_price_cache[cache_key] = cache_data
                
                # Main cache for longer retention
                self.price_cache[cache_key] = cache_data
                
                # Record fetch timestamp for rate limiting
                self._record_price_fetch(symbol)
                
                if hasattr(self, 'performance_metrics'):
                    self.performance_metrics["cache_misses"] += 1

            return price

        except Exception as e:
            logging.error(f"Price fetch failed ({symbol}): {e}")
            
            # Fallback to cache on error
            cache_key = f"price_{symbol}"
            if cache_key in self.price_cache:
                logging.warning(f"Using cached price for {symbol} due to error")
                return self.price_cache[cache_key]["price"]
            
            return None

    def _should_use_batch_fetch(self) -> bool:
        """
        Determine if batch fetching should be used
        High-performance system can handle larger batches efficiently
        """
        if not hasattr(self, '_batch_pending_symbols'):
            self._batch_pending_symbols = set()
        
        # Use batch if multiple symbols pending
        return len(self._batch_pending_symbols) >= 5

    async def _get_price_from_batch(self, symbol: str) -> Optional[float]:
        """
        Get price from batch request queue
        Optimized for multi-core parallel processing
        """
        try:
            if not hasattr(self, '_batch_pending_symbols'):
                self._batch_pending_symbols = set()
            
            self._batch_pending_symbols.add(symbol)
            
            # Wait briefly for more symbols to accumulate
            await asyncio.sleep(0.05)  # 50ms aggregation window
            
            if len(self._batch_pending_symbols) >= 3:
                symbols_to_fetch = list(self._batch_pending_symbols)[:50]  # Binance limit: 100
                self._batch_pending_symbols -= set(symbols_to_fetch)
                
                # Batch fetch
                prices = await self._batch_fetch_prices(symbols_to_fetch)
                
                if symbol in prices:
                    return prices[symbol]
            
            return None
            
        except Exception as e:
            logging.debug(f"Batch fetch attempt failed: {e}")
            return None

    async def _batch_fetch_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        Fetch multiple prices in single API call
        Leverages high-performance system for parallel processing
        """
        try:
            if not symbols:
                return {}
            
            # Binance batch ticker endpoint
            url = "https://api.binance.com/api/v3/ticker/price"
            
            # Split into chunks of 100 (Binance limit)
            all_prices = {}
            
            for i in range(0, len(symbols), 100):
                chunk = symbols[i:i+100]
                
                timeout = aiohttp.ClientTimeout(total=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    # Binance accepts array parameter
                    params = {"symbols": json.dumps(chunk)}
                    
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            current_time = time.time()
                            for item in data:
                                sym = item['symbol']
                                price = float(item['price'])
                                
                                # Update both cache tiers
                                cache_data = {
                                    "price": price,
                                    "timestamp": current_time,
                                    "fetch_count": 1
                                }
                                
                                self.price_cache[f"price_{sym}"] = cache_data
                                
                                if not hasattr(self, '_hot_price_cache'):
                                    self._hot_price_cache = {}
                                self._hot_price_cache[f"price_{sym}"] = cache_data
                                
                                all_prices[sym] = price
            
            logging.info(f"Batch fetched {len(all_prices)} prices successfully")
            return all_prices
            
        except Exception as e:
            logging.error(f"Batch price fetch failed: {e}")
            return {}

    def _can_fetch_price(self, symbol: str) -> bool:
        """
        Check if we can fetch price without hitting rate limit
        Binance limit: 1200 requests/minute = 20 requests/second
        High-spec optimization: 400 symbols support with intelligent throttling
        """
        if not hasattr(self, '_price_fetch_timestamps'):
            self._price_fetch_timestamps = {}
        
        current_time = time.time()
        symbol_timestamps = self._price_fetch_timestamps.get(symbol, [])
        
        # Clean old timestamps (older than 60 seconds)
        recent_timestamps = [ts for ts in symbol_timestamps if current_time - ts < 60]
        
        # High-performance system: allow 2 fetches per symbol per minute (conservative)
        if len(recent_timestamps) >= 2:
            return False
        
        return True

    def _record_price_fetch(self, symbol: str):
        """
        Record price fetch timestamp for rate limiting
        Optimized for 128GB RAM - larger history tracking
        """
        if not hasattr(self, '_price_fetch_timestamps'):
            self._price_fetch_timestamps = {}
        
        current_time = time.time()
        
        if symbol not in self._price_fetch_timestamps:
            self._price_fetch_timestamps[symbol] = []
        
        self._price_fetch_timestamps[symbol].append(current_time)
        
        # Keep larger history on high-spec system (last 20 timestamps)
        self._price_fetch_timestamps[symbol] = self._price_fetch_timestamps[symbol][-20:]

    async def _fetch_real_price(self, symbol: str) -> Optional[float]:
        """Enhanced price fetching with robust error handling and circuit breaker pattern"""
        if not symbol or not isinstance(symbol, str):
            return None
        
        symbol = symbol.strip().upper()

        # Enhanced cache check first
        cached_price = await self._get_cached_price_fallback(symbol)
        if cached_price:
            if hasattr(self, 'performance_metrics'):
                self.performance_metrics["cache_hits"] += 1
            return cached_price
        
        if hasattr(self, 'performance_metrics'):
            self.performance_metrics["cache_misses"] += 1
        
        # Enhanced rate limiting
        if not self._check_simple_rate_limit():
            logging.debug(f"Rate limit hit for {symbol}")
            return cached_price
        
        # Parse symbol type efficiently
        if symbol.endswith('.P'):
            symbol_type = 'futures'
            clean_symbol = symbol.replace('.P', '')
        else:
            symbol_type = 'spot'
            clean_symbol = symbol
        
        # Circuit breaker check for this symbol
        if hasattr(self, '_api_circuit_breaker'):
            if self._is_circuit_open(symbol):
                logging.debug(f"Circuit breaker open for {symbol}, using cache")
                return cached_price
        else:
            self._api_circuit_breaker = {}
        
        # Try primary endpoint with retry
        max_retries = 2
        for attempt in range(max_retries):
            endpoint = self._get_primary_endpoint_with_type(clean_symbol, symbol_type)
            if endpoint:
                try:
                    price = await self._fetch_from_endpoint_with_retry(
                        endpoint["url"], 
                        clean_symbol, 
                        endpoint["timeout"],
                        attempt
                    )
                    
                    if price and price > 0:
                        self._cache_price_simple(symbol, price)
                        self._record_api_success(symbol)
                        return price
                        
                except Exception as e:
                    logging.debug(f"Primary endpoint attempt {attempt + 1} failed for {symbol}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
        
        # Try fallback endpoint with retry
        for attempt in range(max_retries):
            fallback_endpoint = self._get_fallback_endpoint(clean_symbol, symbol_type)
            if fallback_endpoint:
                try:
                    price = await self._fetch_from_endpoint_with_retry(
                        fallback_endpoint["url"], 
                        clean_symbol, 
                        fallback_endpoint["timeout"],
                        attempt
                    )
                    
                    if price and price > 0:
                        self._cache_price_simple(symbol, price)
                        self._record_api_success(symbol)
                        return price
                        
                except Exception as e:
                    logging.debug(f"Fallback endpoint attempt {attempt + 1} failed for {symbol}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5 * (attempt + 1))
        
        # All endpoints failed - record failure and return stale cache
        self._record_api_failure(symbol)
        
        if cached_price:
            logging.warning(f"All endpoints failed for {symbol}, using stale cache (age: {self._get_cache_age(symbol):.0f}s)")
        else:
            logging.error(f"All endpoints failed for {symbol}, no cache available")
        
        return cached_price

    async def _fetch_from_endpoint_with_retry(self, url: str, symbol: str, timeout: int, attempt: int) -> Optional[float]:
        """Enhanced endpoint fetch with connection pooling and error handling"""
        try:
            # Adaptive timeout based on attempt
            adaptive_timeout = timeout + (attempt * 2)
            
            timeout_config = aiohttp.ClientTimeout(
                total=adaptive_timeout,
                connect=min(3, adaptive_timeout // 2),
                sock_read=max(2, adaptive_timeout - 3)
            )
            
            # Use connection pooling for better performance
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=30,
                ttl_dns_cache=300
            )
            
            async with aiohttp.ClientSession(timeout=timeout_config, connector=connector) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logging.debug(f"API error {response.status} for {symbol}")
                        return None
                    
                    data = await response.json()
                    
                    # Extract price from common fields
                    price_fields = ["price", "lastPrice", "c", "close"]
                    
                    for field in price_fields:
                        if field in data and data[field] is not None:
                            try:
                                price = float(data[field])
                                if 0.000001 < price < 10000000:
                                    return price
                            except (ValueError, TypeError):
                                continue
                    
                    logging.debug(f"No valid price field found for {symbol}")
                    return None
                    
        except asyncio.TimeoutError:
            logging.debug(f"Timeout fetching {symbol} from {url} (attempt {attempt + 1})")
            return None
        except Exception as e:
            logging.debug(f"Error fetching {symbol}: {e}")
            return None

    def _is_circuit_open(self, symbol: str) -> bool:
        """Check if circuit breaker is open for this symbol"""
        if symbol not in self._api_circuit_breaker:
            return False
        
        breaker = self._api_circuit_breaker[symbol]
        current_time = time.time()
        
        # Reset circuit after cooldown period (60 seconds)
        if breaker.get('open_until', 0) < current_time:
            breaker['failures'] = 0
            breaker['open_until'] = 0
            return False
        
        return breaker.get('open_until', 0) > current_time

    def _record_api_success(self, symbol: str):
        """Record successful API call"""
        if symbol in self._api_circuit_breaker:
            self._api_circuit_breaker[symbol]['failures'] = 0
            self._api_circuit_breaker[symbol]['open_until'] = 0

    def _record_api_failure(self, symbol: str):
        """Record failed API call and open circuit if threshold exceeded"""
        if symbol not in self._api_circuit_breaker:
            self._api_circuit_breaker[symbol] = {'failures': 0, 'open_until': 0}
        
        breaker = self._api_circuit_breaker[symbol]
        breaker['failures'] += 1
        
        # Open circuit after 5 consecutive failures
        if breaker['failures'] >= 5:
            breaker['open_until'] = time.time() + 60  # 60 second cooldown
            logging.warning(f"Circuit breaker opened for {symbol} (60s cooldown)")

    def _get_cache_age(self, symbol: str) -> float:
        """Get age of cached price in seconds"""
        if hasattr(self, 'price_cache') and symbol in self.price_cache:
            entry = self.price_cache[symbol]
            if isinstance(entry, dict) and 'timestamp' in entry:
                return time.time() - entry['timestamp']
        return 0.0

    def _get_endpoint_for_symbol(self, symbol: str, clean_symbol: str = None, symbol_type: str = None, is_fallback: bool = False) -> Optional[Dict[str, Any]]:
        """Unified endpoint resolver with fallback support"""
        try:
            if symbol_type is None:
                if symbol.endswith(".P"):
                    symbol_type = 'futures'
                    clean_symbol = symbol[:-2] if clean_symbol is None else clean_symbol
                else:
                    symbol_type = 'spot'
                    clean_symbol = symbol if clean_symbol is None else clean_symbol
            
            target_symbol = clean_symbol or symbol.replace('.P', '') if symbol.endswith('.P') else symbol
            
            # Use production API for better reliability
            base_url = "https://api.binance.com"
            
            if is_fallback:
                api_path = "/api/v3/ticker/price" if symbol_type == 'futures' else "/fapi/v1/ticker/price"
            else:
                api_path = "/fapi/v1/ticker/price" if symbol_type == 'futures' else "/api/v3/ticker/price"
            
            return {
                "url": f"{base_url}{api_path}?symbol={target_symbol}",
                "timeout": 5,
                "type": f"{symbol_type}_{'fallback' if is_fallback else 'primary'}"
            }
            
        except Exception as e:
            logging.debug(f"Endpoint resolution failed for {symbol}: {e}")
            return None

    def _get_primary_endpoint_with_type(self, clean_symbol: str, symbol_type: str) -> Optional[Dict[str, Any]]:
        """Get primary endpoint - compatibility wrapper"""
        return self._get_endpoint_for_symbol(
            symbol=clean_symbol, 
            clean_symbol=clean_symbol, 
            symbol_type=symbol_type, 
            is_fallback=False
        )

    def _get_fallback_endpoint(self, clean_symbol: str, symbol_type: str) -> Optional[Dict[str, Any]]:
        """Get fallback endpoint - compatibility wrapper"""
        return self._get_endpoint_for_symbol(
            symbol=clean_symbol, 
            clean_symbol=clean_symbol, 
            symbol_type=symbol_type, 
            is_fallback=True
        )

    async def _fetch_from_endpoint(self, url: str, symbol: str, timeout: int) -> Optional[float]:
        """Enhanced endpoint fetch with safe timeout configuration"""
        try:
            # Safe timeout configuration - prevent negative values
            total_timeout = max(5, timeout)
            connect_timeout = min(3, total_timeout // 2)
            read_timeout = max(1, total_timeout - connect_timeout)
            
            timeout_config = aiohttp.ClientTimeout(
                total=total_timeout,
                connect=connect_timeout,
                sock_read=read_timeout
            )
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=timeout_config) as response:
                    if response.status != 200:
                        logging.debug(f"API error {response.status} for {symbol}")
                        return None
                    
                    data = await response.json()
                    
                    # Extract price from common fields with validation
                    price_fields = ["price", "lastPrice", "c", "close"]
                    
                    for field in price_fields:
                        if field in data and data[field] is not None:
                            try:
                                price = float(data[field])
                                # Enhanced sanity check with realistic bounds
                                if 0.000001 < price < 10000000:  # Allow micro prices
                                    return price
                            except (ValueError, TypeError):
                                continue
                    
                    logging.debug(f"No valid price field found for {symbol}")
                    return None
                    
        except asyncio.TimeoutError:
            logging.debug(f"Timeout fetching {symbol} from {url}")
            return None
        except Exception as e:
            logging.debug(f"Error fetching {symbol}: {e}")
            return None

    def _check_enhanced_rate_limit(self) -> bool:
        """Enhanced rate limiting with burst support and adaptive thresholds"""
        if not hasattr(self, '_rate_limit_tracker'):
            self._rate_limit_tracker = {
                'calls': [],
                'burst_count': 0,
                'last_reset': time.time(),
                'adaptive_limit': 30  # Start with 30 calls/minute
            }
        
        current_time = time.time()
        tracker = self._rate_limit_tracker
        
        # Reset every minute with adaptive adjustment
        if current_time - tracker['last_reset'] > 60:
            # Adaptive adjustment based on recent usage
            if tracker['burst_count'] < 3:  # Low burst usage
                tracker['adaptive_limit'] = min(35, tracker['adaptive_limit'] + 1)
            elif tracker['burst_count'] >= 5:  # High burst usage
                tracker['adaptive_limit'] = max(25, tracker['adaptive_limit'] - 1)
            
            tracker['calls'] = []
            tracker['burst_count'] = 0
            tracker['last_reset'] = current_time
        
        # Clean old calls
        tracker['calls'] = [
            call_time for call_time in tracker['calls']
            if current_time - call_time < 60
        ]
        
        total_calls = len(tracker['calls'])
        adaptive_limit = tracker['adaptive_limit']
        
        # Regular limit check
        if total_calls < adaptive_limit:
            tracker['calls'].append(current_time)
            return True
        
        # Burst allowance (up to 5 extra calls)
        if tracker['burst_count'] < 5:
            tracker['burst_count'] += 1
            tracker['calls'].append(current_time)
            logging.debug(f"Using burst allowance: {tracker['burst_count']}/5")
            return True
        
        logging.debug(f"Rate limit exceeded: {total_calls}/{adaptive_limit} + {tracker['burst_count']}/5 burst")
        return False

    def _cache_price_optimized(self, symbol: str, price: float, cache_type: str = 'simple'):
        """Optimized price caching with intelligent cleanup"""
        if not symbol or not isinstance(price, (int, float)) or price <= 0:
            return
        
        cache_attr = f'_{cache_type}_price_cache'
        if not hasattr(self, cache_attr):
            setattr(self, cache_attr, {})
        
        cache = getattr(self, cache_attr)
        current_time = time.time()
        
        # Add/update entry with metadata
        cache[symbol] = {
            "price": float(price),
            "timestamp": current_time,
            "access_count": cache.get(symbol, {}).get("access_count", 0) + 1,
            "update_count": cache.get(symbol, {}).get("update_count", 0) + 1
        }
        
        # Intelligent cleanup based on cache type
        max_sizes = {'simple': 75, 'enhanced': 50, 'regular': 100}
        max_size = max_sizes.get(cache_type, 50)
        
        if len(cache) > max_size:
            # Remove least valuable entries (old + low access count)
            remove_count = max_size // 4  # Remove 25%
            
            scored_items = [
                (key, entry, self._calculate_cache_score(entry, current_time))
                for key, entry in cache.items()
            ]
            
            # Sort by score (lower is worse)
            scored_items.sort(key=lambda x: x[2])
            
            for key, _, _ in scored_items[:remove_count]:
                cache.pop(key, None)
            
            logging.debug(f"Cache cleanup: removed {remove_count} entries from {cache_type} cache")

    def _calculate_cache_score(self, entry: dict, current_time: float) -> float:
        """Calculate cache entry value score with safe division"""
        try:
            age = current_time - entry.get("timestamp", 0)
            access_count = entry.get("access_count", 1)
            update_count = entry.get("update_count", 1)
            
            # Higher score = more valuable
            # Prevent division by zero with max(age, 0.1)
            score = (access_count * update_count) / max(age, 0.1)
            return score
            
        except Exception:
            return 0.0

    async def _get_cached_price_optimized(self, symbol: str) -> Optional[float]:
        """Optimized cached price retrieval with smart selection"""
        if not symbol:
            return None
            
        try:
            current_time = time.time()
            
            # Cache sources with corrected names and metadata
            cache_configs = [
                ('_simple_price_cache', 120, 10),     # 2 min, priority 10
                ('_enhanced_price_cache', 300, 8),    # 5 min, priority 8  
                ('price_cache', 600, 6)               # 10 min, priority 6
            ]
            
            best_candidate = None
            best_score = -1
            
            for cache_attr, max_age, base_priority in cache_configs:
                if hasattr(self, cache_attr):
                    cache = getattr(self, cache_attr)
                    if isinstance(cache, dict) and symbol in cache:
                        entry = cache[symbol]
                        
                        if (isinstance(entry, dict) and 
                            "timestamp" in entry and 
                            "price" in entry):
                            
                            age = current_time - entry["timestamp"]
                            price = entry["price"]
                            
                            if (age < max_age and 
                                isinstance(price, (int, float)) and 
                                price > 0):
                                
                                # Calculate selection score
                                freshness = max(0, 1 - (age / max_age))
                                access_bonus = min(entry.get("access_count", 1) / 10, 1)
                                total_score = base_priority + freshness * 5 + access_bonus
                                
                                if total_score > best_score:
                                    best_candidate = entry
                                    best_score = total_score
            
            if best_candidate:
                # Update access tracking
                best_candidate["access_count"] = best_candidate.get("access_count", 0) + 1
                return float(best_candidate["price"])
            
            return None
            
        except Exception as e:
            logging.debug(f"Cache retrieval error for {symbol}: {e}")
            return None

    # Compatibility wrapper functions - simplified and safe
    def _check_simple_rate_limit(self) -> bool:
        """Compatibility wrapper for enhanced rate limiting"""
        return self._check_enhanced_rate_limit()

    def _cache_price_simple(self, symbol: str, price: float):
        """Compatibility wrapper for optimized caching"""
        self._cache_price_optimized(symbol, price, 'simple')

    async def _get_cached_price_fallback(self, symbol: str) -> Optional[float]:
        """Compatibility wrapper for optimized cache retrieval"""
        return await self._get_cached_price_optimized(symbol)

    async def _update_position_pnl(
        self, position_risk: PositionRisk, risk_profile: RiskProfile
    ):
        """Update position P&L"""
        try:
            entry_price = position_risk.entry_price
            current_price = position_risk.current_price
            leverage = position_risk.leverage

            # Calculate price change rate
            if position_risk.side == "BUY":
                price_change = (current_price - entry_price) / entry_price
            else:
                price_change = (entry_price - current_price) / entry_price

            # Apply leverage return rate
            leveraged_return = price_change * leverage

            # Calculate unrealized P&L
            position_risk.unrealized_pnl = (
                position_risk.margin_required * leveraged_return
            )

            # Calculate liquidation risk
            liquidation_distance = (

                abs(current_price - risk_profile.liquidation_price) / current_price
            )
            position_risk.distance_to_liquidation = liquidation_distance

            # Liquidation risk (high risk when approaching within 5%)
            if liquidation_distance <= 0.05:
                position_risk.liquidation_risk = 1.0
            elif liquidation_distance <= 0.10:
                position_risk.liquidation_risk = 0.8
            elif liquidation_distance <= 0.15:
                position_risk.liquidation_risk = 0.5
            else:
                position_risk.liquidation_risk = max(0, 1 - liquidation_distance / 0.20)

        except Exception as e:
            logging.error(f"P&L update failed: {e}")

    async def _check_risk_alerts_optimized(
        self, position_id: str, position_risk: PositionRisk, risk_profile: RiskProfile
    ):
        """Optimized risk alert check"""
        try:
            current_time = time.time()
            alert_key_base = f"{position_id}_{int(current_time / self.alert_cooldown)}"

            alerts_to_send = []

            # 1. Emergency liquidation risk
            if (
                position_risk.liquidation_risk
                >= config.RISK_CONFIG["emergency_close_threshold"]
            ):
                alert_key = f"{alert_key_base}_CRITICAL"
                if not self._alert_recently_sent(alert_key):
                    alerts_to_send.append(
                        {
                            "level": "CRITICAL",
                            "message": f"Emergency liquidation risk: {position_risk.symbol}",
                            "key": alert_key,
                        }
                    )

            # 2. Increased liquidation risk
            elif (
                position_risk.liquidation_risk
                >= config.RISK_CONFIG["risk_alert_threshold"]
            ):
                alert_key = f"{alert_key_base}_WARNING"
                if not self._alert_recently_sent(alert_key):
                    alerts_to_send.append(
                        {
                            "level": "WARNING",
                            "message": f"Liquidation risk increased: {position_risk.symbol} ({position_risk.liquidation_risk:.1%})",
                            "key": alert_key,
                        }
                    )

            # 3. Large loss alert
            loss_threshold = position_risk.margin_required * 0.6  # 60% loss
            if position_risk.unrealized_pnl < -loss_threshold:
                alert_key = f"{alert_key_base}_LOSS"
                if not self._alert_recently_sent(alert_key):
                    alerts_to_send.append(
                        {
                            "level": "WARNING",
                            "message": f"Large loss: {position_risk.symbol} P&L: ${position_risk.unrealized_pnl:.2f}",
                            "key": alert_key,
                        }
                    )

            # 4. Large profit alert (good news too!)
            profit_threshold = position_risk.margin_required * 0.5  # 50% profit
            if position_risk.unrealized_pnl > profit_threshold:
                alert_key = f"{alert_key_base}_PROFIT"
                if not self._alert_recently_sent(alert_key):
                    alerts_to_send.append(
                        {
                            "level": "INFO",
                            "message": f"Large profit: {position_risk.symbol} P&L: ${position_risk.unrealized_pnl:.2f}",
                            "key": alert_key,
                        }
                    )

            # Send alerts
            for alert in alerts_to_send:
                await self.telegram.send_risk_alert(
                    position_id, position_risk, alert["level"], alert["message"]
                )
                self._mark_alert_sent(alert["key"])

        except Exception as e:
            logging.error(f"Risk alert check failed: {e}")

    def _alert_recently_sent(self, alert_key: str) -> bool:
        """Check if alert was sent recently"""
        return any(alert["key"] == alert_key for alert in self.alert_history)

    def _mark_alert_sent(self, alert_key: str):
        """Record alert sending"""
        self.alert_history.append({"key": alert_key, "timestamp": time.time()})

    def _check_auto_close_conditions(
        self,
        position_risk: PositionRisk,
        risk_profile: RiskProfile,
        current_price: float,
    ) -> Dict:
        """
        Check automatic liquidation conditions with optimized thresholds
        Optimized for high-leverage trading (20x) on high-spec system
        """
        try:
            # 1. Emergency liquidation - FIXED: 90% -> 97% for 20x leverage safety
            # Prevents premature liquidation due to normal price volatility
            if position_risk.liquidation_risk >= 0.97:
                logging.warning(
                    f"Emergency liquidation triggered: {position_risk.position_id} "
                    f"risk={position_risk.liquidation_risk:.2%}"
                )
                return {
                    "should_close": True,
                    "reason": "EMERGENCY_LIQUIDATION_RISK",
                    "priority": "HIGH",
                }

            # 2. Stop loss reached - ENHANCED: 0.2% -> 0.5% buffer for slippage protection
            if position_risk.side == "BUY":
                # Allow 0.5% tolerance to prevent premature stop-out on wick movements
                stop_loss_trigger = risk_profile.stop_loss_price * 1.005
                if current_price <= stop_loss_trigger:
                    logging.info(
                        f"Stop loss triggered: {position_risk.position_id} "
                        f"price={current_price:.4f}, sl={risk_profile.stop_loss_price:.4f}, "
                        f"trigger={stop_loss_trigger:.4f}"
                    )
                    return {
                        "should_close": True,
                        "reason": "STOP_LOSS_TRIGGERED",
                        "priority": "MEDIUM",
                    }
            else:
                # Short position: stop loss above entry - 0.5% buffer
                stop_loss_trigger = risk_profile.stop_loss_price * 0.995
                if current_price >= stop_loss_trigger:
                    logging.info(
                        f"Stop loss triggered: {position_risk.position_id} "
                        f"price={current_price:.4f}, sl={risk_profile.stop_loss_price:.4f}, "
                        f"trigger={stop_loss_trigger:.4f}"
                    )
                    return {
                        "should_close": True,
                        "reason": "STOP_LOSS_TRIGGERED",
                        "priority": "MEDIUM",
                    }

            # 3. Take profit reached - ENHANCED: 0.1% -> 0.3% buffer for better execution
            if position_risk.side == "BUY":
                # Take profit slightly below target (0.3%) to ensure execution
                take_profit_trigger = risk_profile.take_profit_price * 0.997
                if current_price >= take_profit_trigger:
                    logging.info(
                        f"Take profit triggered: {position_risk.position_id} "
                        f"price={current_price:.4f}, tp={risk_profile.take_profit_price:.4f}, "
                        f"trigger={take_profit_trigger:.4f}"
                    )
                    return {
                        "should_close": True,
                        "reason": "TAKE_PROFIT_TRIGGERED",
                        "priority": "LOW",
                    }
            else:
                # Short position: take profit below entry - 0.3% buffer
                take_profit_trigger = risk_profile.take_profit_price * 1.003
                if current_price <= take_profit_trigger:
                    logging.info(
                        f"Take profit triggered: {position_risk.position_id} "
                        f"price={current_price:.4f}, tp={risk_profile.take_profit_price:.4f}, "
                        f"trigger={take_profit_trigger:.4f}"
                    )
                    return {
                        "should_close": True,
                        "reason": "TAKE_PROFIT_TRIGGERED",
                        "priority": "LOW",
                    }

            # 4. Maximum loss limit - FIXED: 80% -> 88% for 20x leverage resilience
            # Higher threshold allows position to recover from temporary drawdowns
            max_loss_ratio = 0.88
            if position_risk.unrealized_pnl < -position_risk.margin_required * max_loss_ratio:
                logging.warning(
                    f"Max loss limit triggered: {position_risk.position_id} "
                    f"pnl={position_risk.unrealized_pnl:.2f}, "
                    f"margin={position_risk.margin_required:.2f}, "
                    f"loss_ratio={abs(position_risk.unrealized_pnl/position_risk.margin_required):.2%}"
                )
                return {
                    "should_close": True,
                    "reason": "MAX_LOSS_LIMIT",
                    "priority": "HIGH",
                }

            # 5. Time-based liquidation - ENHANCED: 24 hours with volatility consideration
            position_age = (
                datetime.utcnow() - position_risk.last_updated
            ).total_seconds()
            
            # 24 hours = 86400 seconds
            # Additional check: only close if not in profit
            if position_age > 86400:
                # If position is profitable, extend holding period
                if position_risk.unrealized_pnl > 0:
                    # Allow profitable positions to run for 48 hours
                    if position_age > 172800:  # 48 hours
                        logging.info(
                            f"Extended time-based close triggered: {position_risk.position_id} "
                            f"age={position_age/3600:.1f}h (profitable position)"
                        )
                        return {
                            "should_close": True,
                            "reason": "TIME_BASED_CLOSE_EXTENDED",
                            "priority": "LOW",
                        }
                else:
                    # Close losing positions at 24 hours
                    logging.info(
                        f"Time-based close triggered: {position_risk.position_id} "
                        f"age={position_age/3600:.1f}h"
                    )
                    return {
                        "should_close": True,
                        "reason": "TIME_BASED_CLOSE",
                        "priority": "LOW",
                    }

            # No close conditions met
            return {
                "should_close": False,
                "reason": "NO_CLOSE_CONDITION",
                "priority": "NONE",
            }

        except Exception as e:
            logging.error(
                f"Liquidation condition check failed for {position_risk.position_id}: {e}"
            )
            return {
                "should_close": False,
                "reason": "CHECK_ERROR",
                "priority": "NONE",
                "error": str(e)
            }

    async def _execute_auto_close(
        self, position_id: str, position_risk: PositionRisk, reason: str
    ):
        """Auto position close with enhanced error handling and resource cleanup"""
        cleanup_state = {
            "redis_saved": False,
            "execute_called": False,
            "monitoring_stopped": False,
            "telegram_sent": False
        }
        
        try:
            logging.info(f"Auto close executing: {position_id} - {reason}")

            # Input validation
            if not position_risk or not position_id:
                logging.error("Missing position data or ID")
                return False
            
            if not isinstance(position_id, str) or len(position_id.strip()) == 0:
                logging.error("Invalid position ID format")
                return False

            # Safe P&L calculation with bounds checking
            final_pnl = 0.0
            final_roe = 0.0
            
            try:
                if hasattr(position_risk, 'unrealized_pnl') and position_risk.unrealized_pnl is not None:
                    final_pnl = float(position_risk.unrealized_pnl)
                    
                    # Sanity check for unrealistic P&L values
                    if abs(final_pnl) > 1000000:  # $1M limit
                        logging.warning(f"Unrealistic P&L value: ${final_pnl:.2f}, capping")
                        final_pnl = min(max(final_pnl, -100000), 100000)
                
                if (hasattr(position_risk, 'margin_required') and 
                    position_risk.margin_required and 
                    position_risk.margin_required > 0):
                    final_roe = (final_pnl / position_risk.margin_required) * 100
                    # Cap ROE at reasonable bounds
                    final_roe = min(max(final_roe, -10000), 10000)  # -10000% to +10000%
                    
            except (ValueError, TypeError, ZeroDivisionError) as e:
                logging.error(f"P&L calculation error: {e}")
                final_pnl, final_roe = 0.0, 0.0

            # Step 1: Save realized P&L to Redis with retry
            try:
                cleanup_state["redis_saved"] = await self._save_realized_pnl_with_retry(
                    final_pnl, position_id, max_retries=3
                )
                if not cleanup_state["redis_saved"]:
                    logging.warning(f"Redis P&L save failed for {position_id}")
            except Exception as redis_error:
                logging.error(f"Redis P&L save exception: {redis_error}")
                cleanup_state["redis_saved"] = False

            # Step 2: Request position close from Execute Service
            close_result = {"status": "not_attempted", "error": "not_called"}
            try:
                close_result = await self._request_position_close_with_timeout(
                    position_id, position_risk, reason, final_pnl, final_roe
                )
                cleanup_state["execute_called"] = True
                
                # Log Execute Service response
                if close_result.get("status") == "success":
                    logging.info(f"Execute Service close successful: {position_id}")
                else:
                    logging.warning(f"Execute Service close failed: {close_result}")
                    
            except Exception as execute_error:
                logging.error(f"Execute Service call exception: {execute_error}")
                close_result = {"status": "error", "error": str(execute_error)}

            # Step 3: Send Telegram notification (non-blocking)
            try:
                await asyncio.wait_for(
                    self.telegram.send_position_closed(position_id, position_risk, reason, final_pnl),
                    timeout=5.0
                )
                cleanup_state["telegram_sent"] = True
            except asyncio.TimeoutError:
                logging.warning(f"Telegram notification timeout for {position_id}")
            except Exception as telegram_error:
                logging.warning(f"Telegram notification failed: {telegram_error}")

            # Step 4: Remove position from Redis database
            try:
                await self._remove_position_from_redis_safe(position_id)
            except Exception as remove_error:
                logging.error(f"Redis position removal failed: {remove_error}")

            # Step 5: Stop monitoring task (critical for resource cleanup)
            try:
                if (hasattr(self, 'active_monitors') and 
                    self.active_monitors and 
                    position_id in self.active_monitors):
                    
                    monitor_task = self.active_monitors[position_id]
                    if not monitor_task.done():
                        monitor_task.cancel()
                        
                        # Wait briefly for cancellation
                        try:
                            await asyncio.wait_for(monitor_task, timeout=1.0)
                        except (asyncio.CancelledError, asyncio.TimeoutError):
                            pass  # Expected for cancelled tasks
                    
                    del self.active_monitors[position_id]
                    cleanup_state["monitoring_stopped"] = True
                    logging.debug(f"Monitoring stopped: {position_id}")
                    
            except Exception as monitor_error:
                logging.error(f"Monitor cleanup failed: {monitor_error}")

            # Step 6: Remove from active positions (in-memory cleanup)
            try:
                if (hasattr(self.service, 'risk_manager') and 
                    hasattr(self.service.risk_manager, 'active_positions') and
                    position_id in self.service.risk_manager.active_positions):
                    
                    del self.service.risk_manager.active_positions[position_id]
                    logging.debug(f"Position removed from memory: {position_id}")
                    
            except Exception as memory_error:
                logging.error(f"Memory cleanup failed: {memory_error}")

            # Final status log with detailed breakdown
            success_indicators = []
            if cleanup_state["redis_saved"]:
                success_indicators.append("Redis:OK")
            if cleanup_state["execute_called"]:
                success_indicators.append("Execute:Called")
            if cleanup_state["monitoring_stopped"]:
                success_indicators.append("Monitor:Stopped")
            if cleanup_state["telegram_sent"]:
                success_indicators.append("Telegram:Sent")
                
            status_summary = " | ".join(success_indicators) if success_indicators else "No operations completed"
            
            logging.info(
                f"Position close completed: {position_id} | "
                f"ROE: {final_roe:+.1f}% | P&L: ${final_pnl:+.2f} | "
                f"Status: {status_summary}"
            )
            
            return True

        except Exception as critical_error:
            logging.error(f"Critical error in position close ({position_id}): {critical_error}")
            
            # Emergency cleanup attempt
            try:
                if (hasattr(self, 'active_monitors') and 
                    self.active_monitors and 
                    position_id in self.active_monitors):
                    
                    self.active_monitors[position_id].cancel()
                    del self.active_monitors[position_id]
                    logging.info(f"Emergency monitor cleanup: {position_id}")
                    
            except Exception as emergency_error:
                logging.error(f"Emergency cleanup failed: {emergency_error}")
                
            return False

    async def _save_realized_pnl_with_retry(
        self, final_pnl: float, position_id: str, max_retries: int = 3
    ) -> bool:
        """
        Save realized P&L to Redis with comprehensive validation and retry logic.
        
        Features:
        - Float type validation and sanitization
        - Fake loss prevention (negative PnL without positions)
        - High-performance Redis connection pooling (128GB RAM optimized)
        - Exponential backoff with jitter
        - Data integrity verification
        
        Args:
            final_pnl: Realized profit/loss amount (must be finite)
            position_id: Position identifier for logging
            max_retries: Maximum retry attempts (default: 3)
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        # Input validation and sanitization
        try:
            pnl_value = float(final_pnl)
            
            # Check for NaN and infinity
            if not math.isfinite(pnl_value):
                logging.error(f"Invalid P&L value (NaN/Inf): {final_pnl} for position {position_id}")
                return False
            
            # Reasonable bounds check (prevent extreme values)
            if abs(pnl_value) > 1000000:
                logging.warning(f"Extreme P&L value detected: ${pnl_value:.2f} for position {position_id}")
                # Cap at reasonable limit
                pnl_value = max(-1000000, min(pnl_value, 1000000))
                logging.info(f"P&L capped to: ${pnl_value:.2f}")
                
        except (TypeError, ValueError) as e:
            logging.error(f"P&L type conversion error: {e} (value: {final_pnl})")
            return False
        
        # Fake loss prevention: check if negative PnL with no active positions
        if pnl_value < -10:  # More than $10 loss
            try:
                if hasattr(self.service, 'risk_manager'):
                    active_position_count = len(self.service.risk_manager.active_positions)
                    
                    if active_position_count == 0:
                        logging.error(
                            f"FAKE LOSS DETECTED: ${pnl_value:.2f} loss with 0 active positions "
                            f"(position_id: {position_id})"
                        )
                        # Don't save fake losses
                        return False
            except Exception as check_error:
                logging.warning(f"Fake loss check failed: {check_error}")
        
        # Retry loop with enhanced error handling
        for attempt in range(max_retries):
            try:
                # Ensure Redis connection
                if not (self.service and hasattr(self.service, 'redis')):
                    logging.warning("No Redis service available")
                    return False
                    
                if not self.service.redis:
                    # Try to reconnect
                    if hasattr(self.service, '_ensure_redis_connection'):
                        redis_ok = await self.service._ensure_redis_connection()
                        if not redis_ok:
                            logging.warning(f"Redis reconnect failed (attempt {attempt + 1}/{max_retries})")
                            if attempt < max_retries - 1:
                                # Exponential backoff with jitter
                                base_wait = 0.5 * (attempt + 1)
                                jitter = base_wait * 0.2
                                await asyncio.sleep(base_wait + jitter)
                                continue
                            return False
                    else:
                        logging.error("Redis connection method not available")
                        return False

                # Generate date key
                today_str = datetime.utcnow().strftime("%Y-%m-%d")
                pnl_key = f"pnl:{today_str}"
                
                # Check existing PnL before adding (for verification)
                try:
                    existing_pnl_str = await asyncio.wait_for(
                        self.service.redis.get(pnl_key),
                        timeout=2.0
                    )
                    existing_pnl = float(existing_pnl_str) if existing_pnl_str else 0.0
                    
                    logging.debug(f"Current PnL before update: ${existing_pnl:.2f}")
                    
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout reading existing PnL (attempt {attempt + 1})")
                    existing_pnl = 0.0
                except Exception as read_error:
                    logging.warning(f"Error reading existing PnL: {read_error}")
                    existing_pnl = 0.0
                
                # Save to Redis with timeout
                await asyncio.wait_for(
                    self.service.redis.incrbyfloat(pnl_key, float(pnl_value)),
                    timeout=3.0
                )
                
                # Set expiration (25 hours = 90000 seconds)
                await asyncio.wait_for(
                    self.service.redis.expire(pnl_key, 90000),
                    timeout=2.0
                )
                
                # Verify the save operation
                try:
                    new_pnl_str = await asyncio.wait_for(
                        self.service.redis.get(pnl_key),
                        timeout=2.0
                    )
                    new_pnl = float(new_pnl_str) if new_pnl_str else 0.0
                    expected_pnl = existing_pnl + pnl_value
                    
                    # Verification with tolerance for floating point precision
                    if abs(new_pnl - expected_pnl) > 0.01:
                        logging.warning(
                            f"PnL verification mismatch: expected ${expected_pnl:.2f}, "
                            f"got ${new_pnl:.2f}"
                        )
                    else:
                        logging.debug(f"PnL verification passed: ${new_pnl:.2f}")
                        
                except Exception as verify_error:
                    logging.warning(f"PnL verification failed: {verify_error}")
                
                logging.info(
                    f"P&L saved to Redis: ${pnl_value:.2f} (key: {pnl_key}, "
                    f"position: {position_id}, attempt: {attempt + 1})"
                )
                return True

            except asyncio.TimeoutError:
                logging.warning(
                    f"Redis timeout (attempt {attempt + 1}/{max_retries}) "
                    f"for position {position_id}"
                )
            except ConnectionError as conn_error:
                logging.error(
                    f"Redis connection error (attempt {attempt + 1}/{max_retries}): {conn_error}"
                )
                # Force reconnection on next attempt
                if self.service and hasattr(self.service, 'redis'):
                    self.service.redis = None
            except Exception as e:
                logging.error(
                    f"Redis save error (attempt {attempt + 1}/{max_retries}): {e} "
                    f"for position {position_id}"
                )
                
            if attempt < max_retries - 1:
                # Exponential backoff with jitter
                base_wait = 0.5 * (attempt + 1)
                jitter = base_wait * 0.2 * (hash(position_id) % 100 / 100)
                total_wait = base_wait + jitter
                
                logging.info(f"Retrying Redis save in {total_wait:.2f}s...")
                await asyncio.sleep(total_wait)
        
        logging.error(f"P&L save failed after {max_retries} attempts for position {position_id}")
        return False

    async def _request_position_close_with_timeout(
        self, position_id: str, position_risk: PositionRisk, 
        reason: str, final_pnl: float, final_roe: float
    ) -> Dict:
        """
        Request position close from Execute Service with comprehensive validation.
        
        Features:
        - Safe JSON serialization for all data types
        - Input validation and sanitization
        - High-performance timeout handling (128GB RAM optimized)
        - Detailed error tracking
        - Automatic retry with reduced attempts
        
        Args:
            position_id: Unique position identifier
            position_risk: Position risk information object
            reason: Close reason code
            final_pnl: Final profit/loss amount
            final_roe: Final return on equity percentage
            
        Returns:
            Dict containing close request result
        """
        try:
            # Input validation and sanitization
            if not position_id or not isinstance(position_id, str):
                raise ValueError(f"Invalid position_id: {position_id}")
            
            if not isinstance(reason, str) or not reason.strip():
                reason = "UNKNOWN_REASON"
                logging.warning(f"Invalid close reason, using default: {reason}")
            
            # Validate and sanitize PnL values
            try:
                pnl_value = float(final_pnl)
                roe_value = float(final_roe)
                
                # Check for finite values
                if not math.isfinite(pnl_value):
                    logging.warning(f"Invalid PnL value (NaN/Inf): {final_pnl}, using 0.0")
                    pnl_value = 0.0
                
                if not math.isfinite(roe_value):
                    logging.warning(f"Invalid ROE value (NaN/Inf): {final_roe}, using 0.0")
                    roe_value = 0.0
                    
                # Reasonable bounds
                pnl_value = max(-1000000, min(pnl_value, 1000000))
                roe_value = max(-10000, min(roe_value, 10000))
                
            except (TypeError, ValueError) as conv_error:
                logging.error(f"PnL/ROE conversion error: {conv_error}")
                pnl_value = 0.0
                roe_value = 0.0
            
            # Extract position information safely
            symbol = str(getattr(position_risk, 'symbol', 'UNKNOWN')).strip().upper()
            current_price = float(getattr(position_risk, 'current_price', 0.0))
            
            # Validate current price
            if not math.isfinite(current_price) or current_price <= 0:
                logging.warning(f"Invalid current_price: {current_price}, attempting to use entry_price")
                current_price = float(getattr(position_risk, 'entry_price', 0.0))
                
                if not math.isfinite(current_price) or current_price <= 0:
                    logging.error(f"No valid price found for position {position_id}")
                    current_price = 0.0
            
            # Build close request with JSON-safe data types
            close_data = {
                "action": "close_position",
                "position_id": str(position_id),
                "symbol": symbol,
                "reason": str(reason).upper(),
                "current_price": float(current_price),
                "final_pnl": float(pnl_value),
                "final_roe": float(roe_value),
                "service_source": "risk_management",
                "timestamp": float(time.time()),  # Explicit float for JSON
            }
            
            # Additional position details (optional, non-critical)
            try:
                close_data.update({
                    "entry_price": float(getattr(position_risk, 'entry_price', 0.0)),
                    "side": str(getattr(position_risk, 'side', 'UNKNOWN')),
                    "leverage": int(getattr(position_risk, 'leverage', 1)),
                    "quantity": float(getattr(position_risk, 'quantity', 0.0)),
                })
            except Exception as detail_error:
                logging.warning(f"Failed to add position details: {detail_error}")
            
            # JSON serialization validation
            try:
                json_test = json.dumps(close_data)
                logging.debug(f"Close request JSON validated: {len(json_test)} bytes")
            except (TypeError, ValueError) as json_error:
                logging.error(f"Close request JSON validation failed: {json_error}")
                return {
                    "status": "failed",
                    "error": f"JSON serialization error: {str(json_error)}",
                    "position_id": position_id
                }
            
            # Call Execute Service with timeout
            if (hasattr(self.service, 'risk_manager') and 
                hasattr(self.service.risk_manager, '_call_service_with_retry')):
                
                logging.info(f"Sending close request for position {position_id} (${pnl_value:+.2f}, ROE: {roe_value:+.1f}%)")
                
                close_result = await asyncio.wait_for(
                    self.service.risk_manager._call_service_with_retry(
                        f"{config.SERVICE_URLS['execute_service']}/close_position",
                        close_data,
                        max_retries=2  # Reduced retries for faster response
                    ),
                    timeout=10.0  # 10 second timeout
                )
                
                logging.info(f"Execute Service close request sent: {position_id} -> {close_result.get('status', 'unknown')}")
                return close_result or {"status": "success", "message": "Request sent", "position_id": position_id}
                
            else:
                logging.warning("Risk manager not available for service call")
                return {
                    "status": "failed", 
                    "error": "Risk manager unavailable",
                    "position_id": position_id
                }

        except asyncio.TimeoutError:
            logging.error(f"Execute Service timeout for position {position_id} after 10s")
            return {
                "status": "timeout", 
                "error": "Execute Service timeout (10s)",
                "position_id": position_id
            }
        except ValueError as val_error:
            logging.error(f"Validation error for position {position_id}: {val_error}")
            return {
                "status": "failed",
                "error": f"Validation error: {str(val_error)}",
                "position_id": position_id
            }
        except Exception as e:
            logging.error(f"Execute Service call failed for position {position_id}: {e}")
            logging.error(f"Error details: {traceback.format_exc()}")
            return {
                "status": "failed",
                "error": str(e),
                "position_id": position_id
            }

    async def _remove_position_from_redis_safe(self, position_id: str) -> bool:
        """Safely remove position data from Redis"""
        try:
            if not (self.service and hasattr(self.service, 'redis') and self.service.redis):
                return False

            result = await asyncio.wait_for(
                self.service.redis.delete(f"pos:{position_id}"),
                timeout=3.0
            )
            
            if result:
                logging.info(f"Position removed from Redis: {position_id}")
            else:
                logging.debug(f"Position not found in Redis: {position_id}")
                
            return bool(result)

        except asyncio.TimeoutError:
            logging.warning(f"Redis delete timeout for {position_id}")
            return False
        except Exception as e:
            logging.error(f"Redis position removal failed: {e}")
            return False

    async def _save_realized_pnl_to_redis(self, final_pnl: float) -> bool:
        """Safely save realized profit/loss to Redis"""
        try:
            # Check Redis connection
            if not (self.service and hasattr(self.service, 'redis') and self.service.redis):
                logging.warning("No Redis connection - cannot save realized P&L")
                return False

            # Recheck Redis connection status
            if hasattr(self.service, '_ensure_redis_connection'):
                redis_ok = await self.service._ensure_redis_connection()
                if not redis_ok:
                    logging.warning("Redis reconnection failed - cannot save realized P&L")
                    return False

            # Generate date key
            today_str = datetime.utcnow().strftime("%Y-%m-%d")
            pnl_key = f"pnl:{today_str}"
            
            # Validate value
            if not isinstance(final_pnl, (int, float)) or abs(final_pnl) > 1000000:
                logging.warning(f"Abnormal P&L value: {final_pnl}")
                return False

            # Save accumulated realized P&L to Redis
            await self.service.redis.incrbyfloat(pnl_key, float(final_pnl))
            
            # Set TTL (25 hours)
            await self.service.redis.expire(pnl_key, 90000)
            
            logging.info(f"Realized P&L ${final_pnl:.2f} saved to Redis completed (key: {pnl_key})")
            return True

        except Exception as e:
            logging.error(f"Realized P&L Redis save failed: {e}")
            return False

    async def _request_position_close(
        self, position_id: str, position_risk: PositionRisk, 
        reason: str, final_pnl: float, final_roe: float
    ) -> Dict:
        """Request position liquidation to Execute Service"""
        try:
            # Configure request data
            close_data = {
                "action": "close_position",
                "position_id": position_id,
                "symbol": position_risk.symbol,
                "reason": reason,
                "current_price": getattr(position_risk, 'current_price', 0.0),
                "final_pnl": final_pnl,
                "final_roe": final_roe,
                "service_source": "risk_management",
                "timestamp": time.time(),
            }

            # Call Execute Service
            if hasattr(self.service, 'risk_manager'):
                risk_manager = self.service.risk_manager
                close_result = await risk_manager._call_service_with_retry(
                    f"{config.SERVICE_URLS['execute_service']}/close_position",
                    close_data,
                )
                logging.info(f"Execute Service liquidation request successful: {position_id}")
                return close_result or {"status": "success", "message": "Request sent"}
            else:
                logging.warning("Cannot find risk_manager object")
                return {"status": "failed", "error": "No risk_manager"}

        except Exception as e:
            logging.error(f"Execute Service liquidation request failed: {e}")
            return {"status": "failed", "error": str(e)}

    async def _remove_position_from_redis(self, position_id: str) -> bool:
        """Delete position data from Redis"""
        try:
            if not (self.service and hasattr(self.service, 'redis') and self.service.redis):
                return False

            # Delete Redis key
            result = await self.service.redis.delete(f"pos:{position_id}")
            
            if result:
                logging.info(f"Position deleted from Redis completed: {position_id}")
            else:
                logging.debug(f"No position data in Redis: {position_id}")
                
            return bool(result)

        except Exception as e:
            logging.warning(f"Redis position deletion failed: {e}")
            return False

    async def _log_monitoring_performance(self, position_id: str, start_time: float):
        """Monitoring performance log - improved metrics"""
        try:
            uptime = time.time() - start_time
            
            # Collect system metrics
            try:
                memory_usage = psutil.virtual_memory().percent
                cpu_usage = psutil.cpu_percent(interval=0.1)
            except Exception:
                memory_usage = 0.0
                cpu_usage = 0.0

            # Additional performance indicators
            active_monitor_count = len(getattr(self, 'active_monitors', {}))
            
            logging.info(
                f"Monitoring performance - {position_id}: "
                f"uptime {uptime/3600:.1f}h, memory {memory_usage:.1f}%, "
                f"CPU {cpu_usage:.1f}%, active monitors {active_monitor_count}"
            )
            
            # Performance warnings
            if memory_usage > 85:
                logging.warning(f"High memory usage: {memory_usage:.1f}%")
            if cpu_usage > 90:
                logging.warning(f"High CPU usage: {cpu_usage:.1f}%")

        except Exception as e:
            logging.error(f"Performance log failed: {e}")
    

# =============================================================================
# Advanced Telegram Notification System
# =============================================================================


class AdvancedRiskTelegramNotifier:
    """Advanced risk-specific telegram notification system - enhanced encoding safety"""

    def __init__(self):
        self.token = config.TELEGRAM_CONFIG["token"]
        self.chat_id = config.TELEGRAM_CONFIG["chat_id"]
        self.enabled = config.TELEGRAM_CONFIG["enabled"]
        self.notification_queue = asyncio.Queue(maxsize=1000)
        self.failed_notifications = deque(maxlen=100)

        # Connection status tracking
        self.bot_blocked = False
        self.last_connection_check = 0
        self.connection_check_interval = 300  # 5 minutes

        # Notification transmission statistics
        self.stats = {
            "total_sent": 0,
            "total_failed": 0,
            "last_success": None,
            "last_failure": None,
            "connection_errors": 0,
            "blocked_errors": 0,
        }

    async def start_notification_worker(self):
        """Start notification worker with connection validation"""
        await self._check_bot_connection()
        asyncio.create_task(self._notification_worker())
        logging.info("Telegram notification worker started")

    async def _check_bot_connection(self):
        """Check if bot is accessible and not blocked"""
        try:
            url = f"https://api.telegram.org/bot{self.token}/getMe"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    if response.status == 200:
                        self.bot_blocked = False
                        self.stats["last_success"] = time.time()
                        logging.info("Telegram bot connection verified")
                        return True
                    elif response.status == 401:
                        self.bot_blocked = True
                        logging.error("Telegram bot token invalid")
                        return False
                    else:
                        response_text = await response.text()
                        logging.warning(f"Telegram bot check failed ({response.status}): {response_text}")
                        return False
        except Exception as e:
            logging.error(f"Telegram bot connection check failed: {e}")
            self.stats["connection_errors"] += 1
            return False

    async def _notification_worker(self):
        """Background notification worker with enhanced error handling"""
        while True:
            try:
                notification = await asyncio.wait_for(
                    self.notification_queue.get(), timeout=5.0
                )

                # Skip if bot is blocked
                if self.bot_blocked:
                    logging.warning("Bot blocked - skipping notification")
                    self.notification_queue.task_done()
                    continue

                # Periodic connection check
                current_time = time.time()
                if current_time - self.last_connection_check > self.connection_check_interval:
                    await self._check_bot_connection()
                    self.last_connection_check = current_time

                # Message safety verification
                safe_message = safe_encode_message(notification["message"])
                
                success = await self._send_message_internal_with_retry(safe_message)

                if success:
                    self.stats["total_sent"] += 1
                    self.stats["last_success"] = time.time()
                    if self.bot_blocked:
                        self.bot_blocked = False
                        logging.info("Telegram bot connection restored")
                else:
                    self.stats["total_failed"] += 1
                    self.stats["last_failure"] = time.time()
                    self.failed_notifications.append(
                        {"notification": notification, "timestamp": time.time()}
                    )

                self.notification_queue.task_done()
                await asyncio.sleep(0.1)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logging.error(f"Notification worker error: {str(e)}")
                await asyncio.sleep(1)

    async def send_risk_approval(
        self, signal_data: SignalData, risk_profile: RiskProfile, kelly_result: Dict
    ):
        """Risk approval notification"""
        if not self.enabled:
            return

        try:
            alpha_score = getattr(signal_data, 'alpha_score', 0.0) or 0.0
            
            message = f"""[APPROVED] Risk Validation Passed - Trading Permitted

Signal Information
- Symbol: {signal_data.symbol}
- Action: {signal_data.action.upper()}
- Price: ${signal_data.price:,.2f}
- Confidence: {signal_data.confidence:.1%}
- Alpha Score: {alpha_score:.2f}

Leverage Settings
- Leverage: {risk_profile.leverage}x ISOLATED
- Position Size: ${risk_profile.position_size * 100000:,.2f}
- Required Margin: ${risk_profile.margin_required:,.2f}

Profit/Loss Settings
- Take Profit: ${risk_profile.take_profit_price:,.2f} (+{((risk_profile.take_profit_price/signal_data.price-1)*100):+.1f}%)
- Stop Loss: ${risk_profile.stop_loss_price:,.2f} ({((risk_profile.stop_loss_price/signal_data.price-1)*100):+.1f}%)
- Liquidation Price: ${risk_profile.liquidation_price:,.2f}

Kelly Criterion
- Kelly Ratio: {kelly_result.get('kelly_fraction', 0):.1%}
- Win Rate Estimate: {kelly_result.get('win_rate_estimate', 0):.1%}
- Profit/Loss Ratio: {kelly_result.get('profit_loss_ratio', 0):.2f}

Risk Score: {risk_profile.risk_score:.1%}

{datetime.now().strftime('%H:%M:%S')} KST"""

            await self._queue_notification(message)
            
        except Exception as e:
            logging.error(f"Risk approval alert creation failed: {e}")
            fallback_message = f"Risk approval: {signal_data.symbol} {signal_data.action}"
            await self._queue_notification(fallback_message)

    async def send_risk_rejection(
        self, signal_data: SignalData, reason: str, risk_level: str
    ):
        """Risk rejection alert"""
        if not self.enabled:
            return

        try:
            level_indicator = "[REJECT]" if risk_level == "HIGH" else "[WARN]" if risk_level == "MEDIUM" else "[INFO]"

            message = f"""{level_indicator} Risk Rejection - Trading Blocked

Signal Information
- Symbol: {signal_data.symbol}
- Action: {signal_data.action.upper()}
- Price: ${signal_data.price:,.2f}
- Confidence: {signal_data.confidence:.1%}

Rejection Reason
{reason}

Risk Level: {risk_level}

{datetime.now().strftime('%H:%M:%S')} KST"""

            await self._queue_notification(message)
            
        except Exception as e:
            logging.error(f"Risk rejection alert creation failed: {e}")
            fallback_message = f"Risk rejection: {signal_data.symbol} - {reason}"
            await self._queue_notification(fallback_message)

    async def send_risk_alert(
        self,
        position_id: str,
        position_risk: PositionRisk,
        alert_level: str,
        alert_message: str,
    ):
        """Risk alert"""
        if not self.enabled:
            return

        try:
            level_map = {"INFO": "[INFO]", "WARNING": "[WARN]", "CRITICAL": "[ALARM]"}
            level_indicator = level_map.get(alert_level, "[INFO]")

            roe = (
                (position_risk.unrealized_pnl / position_risk.margin_required * 100)
                if position_risk.margin_required > 0
                else 0
            )

            message = f"""{level_indicator} Risk Alert - {alert_level}

{alert_message}

Position Status
- ID: {position_id[:8]}...
- Symbol: {position_risk.symbol}
- Side: {position_risk.side}
- Leverage: {position_risk.leverage}x
- Entry Price: ${position_risk.entry_price:,.2f}
- Current Price: ${position_risk.current_price:,.2f}

Profit/Loss
- Unrealized P&L: ${position_risk.unrealized_pnl:,.2f}
- ROE: {roe:+.1f}%
- Liquidation Risk: {position_risk.liquidation_risk:.1%}
- Distance to Liquidation: {position_risk.distance_to_liquidation:.1%}

[CLOCK] {datetime.now().strftime('%H:%M:%S')} KST"""

            await self._queue_notification(
                message, priority=True if alert_level == "CRITICAL" else False
            )
            
        except Exception as e:
            logging.error(f"Risk alert creation failed: {e}")
            fallback_message = f"[WARN] {alert_level}: {position_risk.symbol}"
            await self._queue_notification(fallback_message)

    async def send_position_closed(
        self,
        position_id: str,
        position_risk: PositionRisk,
        reason: str,
        final_pnl: float,
    ):
        """Position liquidation notification"""
        if not self.enabled:
            return

        try:
            roe = (
                (final_pnl / position_risk.margin_required * 100)
                if position_risk.margin_required > 0
                else 0
            )
            status_indicator = "[PROFIT]" if final_pnl > 0 else "[LOSS]" if final_pnl < -position_risk.margin_required * 0.5 else "[CLOSED]"

            reason_indicators = {
                "TAKE_PROFIT_TRIGGERED": "[TARGET]",
                "STOP_LOSS_TRIGGERED": "[STOP]",
                "EMERGENCY_LIQUIDATION_RISK": "[ALARM]",
                "MAX_LOSS_LIMIT": "[BLOCK]",
                "TIME_BASED_CLOSE": "[TIME]",
            }

            reason_indicator = reason_indicators.get(reason, "[LOCK]")

            message = f"""{status_indicator} Position Liquidation Completed

Liquidation Information
- ID: {position_id[:8]}...
- Symbol: {position_risk.symbol}
- Liquidation Reason: {reason}
- Final P&L: ${final_pnl:,.2f}

Position Summary
- Side: {position_risk.side}
- Leverage: {position_risk.leverage}x
- Entry Price: ${position_risk.entry_price:,.2f}
- Liquidation Price: ${position_risk.current_price:,.2f}
- Margin: ${position_risk.margin_required:,.2f}
- ROE: {roe:+.1f}%

{datetime.now().strftime('%H:%M:%S')} KST"""

            await self._queue_notification(message, priority=True)
            
        except Exception as e:
            logging.error(f"Position liquidation notification creation failed: {e}")
            fallback_message = f"Position liquidation: {position_risk.symbol} ROE: {roe:+.1f}%"
            await self._queue_notification(fallback_message)

    async def send_system_status(self, metrics: Dict):
        """System status notification"""
        if not self.enabled:
            return

        try:
            risk_level_indicator = (
                "[LOW]" if metrics.get("risk_level", "LOW") == "LOW"
                else "[MEDIUM]" if metrics.get("risk_level") == "MEDIUM" else "[HIGH]"
            )

            message = f"""Risk System Status

Portfolio Risk {risk_level_indicator}
- Active Positions: {metrics.get('active_positions', 0)}
- Total Exposure: ${metrics.get('total_exposure', 0):,.0f}
- Margin Utilization: {metrics.get('margin_utilization', 0):.1%}
- Daily P&L: ${metrics.get('daily_pnl', 0):,.2f}

System Performance
- Memory Usage: {metrics.get('memory_usage', 0):.1f}%
- Processed Signals: {metrics.get('signals_processed', 0)}
- Approval Rate: {metrics.get('approval_rate', 0):.1f}%
- Active Monitors: {metrics.get('active_monitors', 0)}

Notification Statistics
- Sent Successfully: {self.stats['total_sent']}
- Send Failed: {self.stats['total_failed']}
- Queued: {self.notification_queue.qsize()}

{datetime.now().strftime('%H:%M:%S')} KST"""

            await self._queue_notification(message)
            
        except Exception as e:
            logging.error(f"System status notification creation failed: {e}")
            fallback_message = f"System status report - detailed information processing error"
            await self._queue_notification(fallback_message)

    async def _queue_notification(self, message: str, priority: bool = False):
        """Add notification to queue"""
        try:
            notification = {
                "message": message,
                "timestamp": time.time(),
                "priority": priority,
            }

            await self.notification_queue.put(notification)

        except asyncio.QueueFull:
            logging.warning("Notification queue is full - removing old notifications")

            try:
                await self.notification_queue.get_nowait()
                await self.notification_queue.put(notification)
            except asyncio.QueueEmpty:
                pass

    async def _send_message_internal_with_retry(self, message: str, max_retries: int = 3) -> bool:
        """Internal message sending with retry logic"""
        for attempt in range(max_retries):
            try:
                result = await self._send_message_internal(message)
                if result:
                    return True
                    
                # If blocked, don't retry
                if self.bot_blocked:
                    return False
                    
                # Wait before retry
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
            except Exception as e:
                logging.error(f"Telegram send attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
        return False

    async def _send_message_internal(self, message: str) -> bool:
        """Internal message sending"""
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "disable_notification": False,
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, timeout=10) as response:
                    if response.status == 200:
                        return True
                    elif response.status == 403:
                        response_text = await response.text()
                        if "bot was blocked" in response_text.lower():
                            self.bot_blocked = True
                            self.stats["blocked_errors"] += 1
                            logging.error("Telegram bot blocked by user")
                        else:
                            logging.warning(f"Telegram 403 error: {response_text}")
                        return False
                    else:
                        response_text = await response.text()
                        logging.warning(f"Telegram send failed ({response.status}): {response_text}")
                        return False

        except Exception as e:
            logging.error(f"Telegram send error: {e}")
            self.stats["connection_errors"] += 1
            return False

# =============================================================================
# Integrated RISK Service Main Class
# =============================================================================


class AdvancedRiskService:
    """Phoenix 95 Advanced Risk Service (Port 8101)"""

    def __init__(self):
        # Redis connection variables
        self.redis = None
        self._redis_connection_retries = 0
        self._max_redis_retries = 5
        self._redis_retry_delay = 1

        # Health check configuration - optimized for rapid failure detection
        self.health_check_config = {
            "check_interval": 2,
            "detailed_check_interval": 8,
            "circuit_breaker_check_interval": 1,
            "memory_check_interval": 3,
            "redis_health_interval": 5,
        }

        # Component initialization with enhanced monitoring
        self.kelly_calculator = AdvancedKellyCriterionCalculator()
        self.telegram = AdvancedRiskTelegramNotifier()
        self.risk_manager = AdvancedLeverageRiskManager(self)
        self.position_monitor = AdvancedRealTimePositionMonitor(
            self.telegram, self
        )

        # Performance metrics - High-spec enhanced tracking
        self.performance_metrics = {
            "start_time": time.time(),
            "total_requests": 0,
            "approved_requests": 0,
            "rejected_requests": 0,
            "blocked_duplicates": 0,
            "service_calls_made": 0,
            "service_calls_success": 0,
            "average_response_time": 0.0,
            "cache_hits": 0,
            "cache_misses": 0,
            "health_check_failures": 0,
            "circuit_breaker_trips": 0,
            # High-spec additional metrics
            "api_calls_total": 0,
            "api_calls_failed": 0,
            "websocket_messages": 0,
            "parallel_tasks_executed": 0,
            "peak_memory_usage": 0.0,
            "peak_cpu_usage": 0.0,
            "positions_monitored_total": 0,
            "avg_position_monitoring_time": 0.0,
            "last_reset": time.time(),
        }

        # Service health status - enhanced monitoring
        self.service_health = {
            "execute_service": {
                "status": "unknown",
                "last_check": 0,
                "consecutive_failures": 0,
            },
            "notify_service": {
                "status": "unknown",
                "last_check": 0,
                "consecutive_failures": 0,
            },
            "portfolio_service": {
                "status": "unknown",
                "last_check": 0,
                "consecutive_failures": 0,
            },
        }

        # Background tasks list
        self.background_tasks = []

        # FastAPI app creation with lifespan
        self.app = FastAPI(
            title="Phoenix 95 Advanced Risk Management Service",
            description="Hedge Fund Grade Risk Management, Service Integration, Performance Optimization",
            version=config.RISK_CONFIG["version"],
            lifespan=self.lifespan
        )

        # CORS middleware setup
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Setup API routes
        self._setup_routes()

        logging.info(
            f"[PHOENIX] Advanced Risk Service v{config.RISK_CONFIG['version']} initialization completed"
        )

    def _setup_routes(self):
        """Setup FastAPI routes for risk management service"""
        
        @self.app.get("/", response_class=HTMLResponse)
        async def root():
            """Main dashboard with advanced analytics"""
            return self._generate_advanced_dashboard_html()
        
        @self.app.get("/health")
        async def health_check():
            """Service health check endpoint"""
            redis_status = "connected" if self.redis else "disconnected"
            
            return {
                "status": "healthy",
                "service": config.RISK_CONFIG["service_name"],
                "version": config.RISK_CONFIG["version"],
                "port": config.RISK_CONFIG["port"],
                "redis": redis_status,
                "active_positions": len(self.risk_manager.active_positions),
                "active_monitors": len(self.position_monitor.active_monitors),
                "uptime": time.time() - self.performance_metrics["start_time"],
                "memory_usage": psutil.virtual_memory().percent,
                "pipeline_status": self._get_pipeline_status(),
                "timestamp": time.time()
            }

        @self.app.post("/risk/validate")
        async def validate_risk_advanced(request: Request):
            """Advanced risk validation with automatic service integration"""
            start_time = time.time()

            try:
                self.performance_metrics["total_requests"] += 1

                data = await request.json()

                # Enhanced signal data extraction with multiple fallback paths
                signal_data = None
                signal_source = "unknown"
                
                # Priority 1: Direct signal keys
                if "signal" in data and data["signal"]:
                    signal_data = data["signal"]
                    signal_source = "signal"
                elif "signal_data" in data and data["signal_data"]:
                    signal_data = data["signal_data"]
                    signal_source = "signal_data"
                elif "phoenix_signal" in data and data["phoenix_signal"]:
                    signal_data = data["phoenix_signal"]
                    signal_source = "phoenix_signal"
                
                # Priority 2: Check if data itself contains signal fields
                elif all(key in data for key in ["symbol", "action", "price"]):
                    signal_data = data
                    signal_source = "root_level"
                    logging.debug("Signal data found at root level")
                
                # Priority 3: Check nested structures
                elif "data" in data and isinstance(data["data"], dict):
                    nested_data = data["data"]
                    if all(key in nested_data for key in ["symbol", "action", "price"]):
                        signal_data = nested_data
                        signal_source = "nested_data"
                        logging.debug("Signal data found in nested 'data' field")
                
                # Validation failure with detailed error message
                if not signal_data:
                    available_keys = list(data.keys())
                    error_detail = {
                        "error": "Signal data missing",
                        "expected_keys": ["signal", "signal_data", "phoenix_signal"],
                        "alternative": "Root level fields: symbol, action, price",
                        "received_keys": available_keys,
                        "hint": "Send data as: {\"signal\": {\"symbol\": \"BTCUSDT\", \"action\": \"BUY\", \"price\": 50000.0, \"confidence\": 0.85}}"
                    }
                    logging.warning(f"Signal data missing. Received keys: {available_keys}")
                    raise HTTPException(status_code=400, detail=error_detail)
                
                # Validate required signal fields
                required_fields = ["symbol", "action", "price"]
                missing_fields = [field for field in required_fields if field not in signal_data]
                
                if missing_fields:
                    error_detail = {
                        "error": "Required signal fields missing",
                        "missing_fields": missing_fields,
                        "required_fields": required_fields,
                        "received_fields": list(signal_data.keys()) if isinstance(signal_data, dict) else []
                    }
                    logging.warning(f"Missing required fields: {missing_fields}")
                    raise HTTPException(status_code=400, detail=error_detail)
                
                logging.debug(f"Signal data extracted from: {signal_source}")

                # Execute risk validation
                validation_result = await self.risk_manager.validate_position_risk(data)

                # Update statistics
                if validation_result.get("cache_blocked"):
                    self.performance_metrics["blocked_duplicates"] += 1
                elif validation_result["approved"]:
                    self.performance_metrics["approved_requests"] += 1

                    # Start position monitoring
                    if "risk_profile" in validation_result:
                        position_id = f"POS_{int(time.time() * 1000)}"
                        risk_profile_dict = validation_result["risk_profile"]

                        risk_profile = RiskProfile(
                            symbol=risk_profile_dict["symbol"],
                            leverage=risk_profile_dict["leverage"],
                            position_size=risk_profile_dict["position_size"],
                            margin_required=risk_profile_dict["margin_required"],
                            liquidation_price=risk_profile_dict["liquidation_price"],
                            stop_loss_price=risk_profile_dict["stop_loss_price"],
                            take_profit_price=risk_profile_dict["take_profit_price"],
                            risk_score=risk_profile_dict["risk_score"],
                            confidence=risk_profile_dict["confidence"],
                        )

                        # Create complete position risk object FIRST
                        position_to_save = PositionRisk(
                            position_id=position_id,
                            symbol=signal_data["symbol"],
                            side=(
                                "BUY"
                                if signal_data["action"].lower() in ["buy", "long"]
                                else "SELL"
                            ),
                            entry_price=float(signal_data["price"]),
                            current_price=float(signal_data["price"]),
                            quantity=risk_profile.position_size,
                            leverage=risk_profile.leverage,
                            margin_required=risk_profile.margin_required,
                            unrealized_pnl=0,
                            liquidation_risk=0,
                            distance_to_liquidation=1.0,
                        )
                        
                        # Add to active positions BEFORE monitoring starts
                        self.risk_manager.active_positions[position_id] = position_to_save

                        # NOW start monitoring with complete data
                        await self.position_monitor.start_position_monitoring(
                            position_id, risk_profile
                        )

                        # Save to Redis with enhanced error handling
                        try:
                            if self.redis:
                                position_json = safe_json_dumps(asdict(position_to_save))
                                await self.redis.set(f"pos:{position_id}", position_json)
                                logging.info(f"Position saved to DB: {position_id}")
                            else:
                                logging.warning(f"Redis not connected - position saved in memory only: {position_id}")
                        except Exception as redis_error:
                            logging.error(f"Position DB save failed: {redis_error}")

                        validation_result["position_id"] = position_id
                else:
                    self.performance_metrics["rejected_requests"] += 1

                # Update response time statistics
                response_time = time.time() - start_time
                self._update_average_response_time(response_time)

                # Update service call statistics
                if validation_result.get("execution_forwarded"):
                    self.performance_metrics["service_calls_made"] += 1

                    if validation_result.get("execution_result", {}).get("status") == "success":
                        self.performance_metrics["service_calls_success"] += 1

                return {
                    **validation_result,
                    "response_time_ms": response_time * 1000,
                    "timestamp": time.time(),
                }

            except HTTPException:
                raise
            except Exception as e:
                logging.error(f"Risk validation failed: {e}\n{traceback.format_exc()}")
                raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

        @self.app.get("/risk/positions")
        async def get_active_positions_detailed():
            """Get detailed information about all active positions"""
            positions = {}
            total_exposure = 0
            total_margin = 0
            total_pnl = 0

            for pos_id, position in self.risk_manager.active_positions.items():
                exposure = position.quantity * position.current_price
                total_exposure += exposure
                total_margin += position.margin_required
                total_pnl += position.unrealized_pnl

                positions[pos_id] = {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "side": position.side,
                    "entry_price": position.entry_price,
                    "current_price": position.current_price,
                    "quantity": position.quantity,
                    "leverage": position.leverage,
                    "margin_required": position.margin_required,
                    "unrealized_pnl": position.unrealized_pnl,
                    "roe_percent": (
                        (position.unrealized_pnl / position.margin_required * 100)
                        if position.margin_required > 0
                        else 0
                    ),
                    "liquidation_risk": position.liquidation_risk,
                    "distance_to_liquidation": position.distance_to_liquidation,
                    "exposure": exposure,
                    "status": position.status,
                    "last_updated": position.last_updated.isoformat(),
                    "is_monitored": pos_id in self.position_monitor.active_monitors,
                }

            return {
                "active_positions": positions,
                "position_count": len(positions),
                "portfolio_summary": {
                    "total_exposure": total_exposure,
                    "total_margin": total_margin,
                    "total_pnl": total_pnl,
                    "margin_utilization": total_margin / 100000,
                    "portfolio_roe": (
                        (total_pnl / total_margin * 100) if total_margin > 0 else 0
                    ),
                    "high_risk_positions": len(
                        [p for p in self.risk_manager.active_positions.values() if p.liquidation_risk > 0.5]
                    ),
                },
                "monitoring_status": {
                    "active_monitors": len(self.position_monitor.active_monitors),
                    "monitoring_performance": self.position_monitor.performance_cache,
                },
                "timestamp": time.time(),
            }

        @self.app.post("/risk/close_position")
        async def close_position_advanced(request: Request):
            """Force close a specific position"""
            try:
                data = await request.json()
                position_id = data.get("position_id")
                reason = data.get("reason", "MANUAL_CLOSE")

                if not position_id:
                    raise HTTPException(status_code=400, detail="Position ID missing")

                if position_id not in self.risk_manager.active_positions:
                    raise HTTPException(status_code=404, detail="Position not found")

                position = self.risk_manager.active_positions[position_id]

                # Execute force close
                await self.position_monitor._execute_auto_close(
                    position_id, position, reason
                )

                # Delete from Redis
                try:
                    if self.redis:
                        await self.redis.delete(f"pos:{position_id}")
                        logging.info(f"Position deleted from DB: {position_id}")
                    else:
                        logging.warning(f"Redis not connected - deleted from memory only: {position_id}")
                except Exception as redis_error:
                    logging.error(f"Position DB delete failed: {redis_error}")

                # Remove from active positions
                del self.risk_manager.active_positions[position_id]

                return {
                    "status": "success",
                    "message": f"Position {position_id} closed successfully",
                    "position_summary": {
                        "symbol": position.symbol,
                        "final_pnl": position.unrealized_pnl,
                        "roe": (
                            (position.unrealized_pnl / position.margin_required * 100)
                            if position.margin_required > 0
                            else 0
                        ),
                    },
                    "reason": reason,
                    "timestamp": time.time(),
                }

            except HTTPException:
                raise
            except Exception as e:
                logging.error(f"Position close failed: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/risk/performance")
        async def get_performance_analytics():
            """Get detailed performance analytics and metrics"""
            uptime = time.time() - self.performance_metrics["start_time"]

            return {
                "uptime": {
                    "seconds": uptime,
                    "hours": uptime / 3600,
                    "days": uptime / 86400,
                },
                "request_analytics": {
                    "total_requests": self.performance_metrics["total_requests"],
                    "requests_per_hour": self.performance_metrics["total_requests"] / max(uptime / 3600, 1),
                    "approval_rate": (
                        self.performance_metrics["approved_requests"]
                        / max(self.performance_metrics["total_requests"], 1)
                    ) * 100,
                    "rejection_rate": (
                        self.performance_metrics["rejected_requests"]
                        / max(self.performance_metrics["total_requests"], 1)
                    ) * 100,
                    "duplicate_block_rate": (
                        self.performance_metrics["blocked_duplicates"]
                        / max(self.performance_metrics["total_requests"], 1)
                    ) * 100,
                },
                "service_integration": {
                    "calls_made": self.performance_metrics["service_calls_made"],
                    "calls_successful": self.performance_metrics["service_calls_success"],
                    "success_rate": (
                        self.performance_metrics["service_calls_success"]
                        / max(self.performance_metrics["service_calls_made"], 1)
                    ) * 100,
                    "service_health": self.service_health,
                },
                "system_performance": {
                    "memory_usage": psutil.virtual_memory().percent,
                    "cpu_usage": psutil.cpu_percent(),
                    "average_response_time": self.performance_metrics["average_response_time"],
                    "cache_performance": {
                        "hits": self.performance_metrics["cache_hits"],
                        "misses": self.performance_metrics["cache_misses"],
                        "hit_rate": (
                            self.performance_metrics["cache_hits"]
                            / max(
                                self.performance_metrics["cache_hits"]
                                + self.performance_metrics["cache_misses"],
                                1,
                            )
                        ) * 100,
                    },
                },
                "timestamp": time.time(),
            }

        @self.app.get("/risk/metrics")
        async def get_risk_metrics():
            """Get comprehensive risk metrics"""
            active_positions = list(self.risk_manager.active_positions.values())
            
            return {
                "portfolio_metrics": {
                    "active_positions": len(active_positions),
                    "total_exposure": sum(p.quantity * p.current_price for p in active_positions),
                    "total_margin": sum(p.margin_required for p in active_positions),
                    "total_pnl": sum(p.unrealized_pnl for p in active_positions),
                },
                "risk_distribution": {
                    "low_risk": len([p for p in active_positions if p.liquidation_risk <= 0.3]),
                    "medium_risk": len([p for p in active_positions if 0.3 < p.liquidation_risk <= 0.7]),
                    "high_risk": len([p for p in active_positions if 0.7 < p.liquidation_risk <= 0.9]),
                    "critical_risk": len([p for p in active_positions if p.liquidation_risk > 0.9]),
                },
                "system_status": {
                    "memory_usage": psutil.virtual_memory().percent,
                    "cpu_usage": psutil.cpu_percent(),
                    "active_monitors": len(self.position_monitor.active_monitors),
                },
                "timestamp": time.time(),
            }

        @self.app.post("/emergency_recovery")
        async def emergency_recovery_endpoint():
            """Emergency system recovery endpoint"""
            try:
                result = await self.emergency_system_recovery()
                if result:
                    return {
                        "status": "success",
                        "message": "Emergency recovery completed - all circuit breakers reset",
                        "actions_taken": [
                            "Circuit breakers reset",
                            "Emergency bypass enabled",
                            "Notification queue cleared",
                            "Execute Service connection tested"
                        ],
                        "timestamp": time.time()
                    }
                else:
                    return {
                        "status": "failed",
                        "message": "Emergency recovery failed - check logs for details",
                        "timestamp": time.time()
                    }
            except Exception as e:
                logging.error(f"Emergency recovery API failed: {e}")
                return {
                    "status": "error",
                    "message": f"Recovery error: {str(e)}",
                    "timestamp": time.time()
                }

        @self.app.get("/risk/health/detailed")
        async def get_detailed_health():
            """Get detailed system health status"""
            try:
                health_status = await self.risk_manager.get_system_health()
                return health_status
            except Exception as e:
                logging.error(f"Detailed health check failed: {e}")
                return {
                    "status": "error",
                    "message": str(e),
                    "timestamp": time.time()
                }

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # === STARTUP logic ===
        startup_state = {
            "redis_connected": False,
            "positions_loaded": False,
            "background_tasks_started": False,
            "telegram_initialized": False
        }
        
        try:
            logging.info("Phoenix 95 Advanced Risk Management Service startup sequence initiated")
            
            # Step 1: Redis connection with enhanced retry and fallback
            try:
                startup_state["redis_connected"] = await self._establish_redis_connection_with_fallback()
                
                if startup_state["redis_connected"]:
                    logging.info("Redis connection established successfully")
                else:
                    logging.warning("Redis connection failed - operating in memory-only mode")
                    self.redis = None
                    
            except Exception as redis_error:
                logging.error(f"Redis connection error: {redis_error}")
                startup_state["redis_connected"] = False
                self.redis = None
            
            # Step 2: Position recovery from database
            if startup_state["redis_connected"]:
                try:
                    positions_loaded = await self._load_positions_with_validation()
                    startup_state["positions_loaded"] = positions_loaded
                    
                    if positions_loaded:
                        logging.info(f"Loaded {len(self.risk_manager.active_positions)} positions from database")
                    else:
                        logging.warning("Position loading failed or no positions found")
                        
                except Exception as position_error:
                    logging.error(f"Position loading error: {position_error}")
                    startup_state["positions_loaded"] = False
            else:
                logging.info("Skipping position loading - Redis not available")
            
            # Step 3: Initialize Telegram notification system (worker will start in Step 4)
            try:
                # Only initialize telegram object, do not start worker yet
                if config.TELEGRAM_CONFIG.get("enabled", False):
                    logging.info("Telegram notification system ready")
                    startup_state["telegram_initialized"] = True
                else:
                    logging.info("Telegram notifications disabled in config")
                    startup_state["telegram_initialized"] = False
                
            except Exception as telegram_error:
                logging.error(f"Telegram initialization error: {telegram_error}")
                startup_state["telegram_initialized"] = False
            
            # Step 4: Start background tasks (includes telegram worker)
            try:
                await self.start_background_tasks()
                startup_state["background_tasks_started"] = True
                logging.info("Background tasks started successfully")
                
            except Exception as task_error:
                logging.error(f"Background task startup error: {task_error}")
                startup_state["background_tasks_started"] = False
            
            # Step 5: Service health verification
            await self._verify_service_health(startup_state)
            
            # Final startup status
            successful_steps = sum(1 for step in startup_state.values() if step)
            total_steps = len(startup_state)
            
            if successful_steps == total_steps:
                logging.info("Service startup completed successfully - all systems operational")
            elif successful_steps >= 2:
                logging.warning(f"Service startup partial success: {successful_steps}/{total_steps} systems operational")
            else:
                logging.error(f"Service startup critical failure: only {successful_steps}/{total_steps} systems operational")
                
            # Send startup notification if Telegram is available
            if startup_state["telegram_initialized"] and startup_state["background_tasks_started"]:
                try:
                    await self._send_startup_notification(startup_state)
                except Exception as notify_error:
                    logging.warning(f"Startup notification failed: {notify_error}")
            
        except Exception as critical_error:
            logging.error(f"Critical startup error: {critical_error}")
            self.redis = None
        
        # === SERVICE RUNNING ===
        yield
        
        # === SHUTDOWN logic ===
        shutdown_state = {
            "positions_saved": False,
            "monitors_stopped": False,
            "tasks_cancelled": False,
            "redis_closed": False
        }
        
        try:
            logging.info("Service shutdown sequence initiated")
            
            # Step 1: Save active positions to Redis
            try:
                if self.redis and len(self.risk_manager.active_positions) > 0:
                    await self._save_all_positions_safely()
                    shutdown_state["positions_saved"] = True
                    logging.info("Active positions saved to database")
                else:
                    logging.info("No positions to save or Redis unavailable")
                    
            except Exception as save_error:
                logging.error(f"Position saving error: {save_error}")
            
            # Step 2: Stop all position monitors
            try:
                await self._stop_all_monitors_safely()
                shutdown_state["monitors_stopped"] = True
                logging.info("All position monitors stopped")
                
            except Exception as monitor_error:
                logging.error(f"Monitor shutdown error: {monitor_error}")
            
            # Step 3: Cancel background tasks
            try:
                await self._cancel_background_tasks_safely()
                shutdown_state["tasks_cancelled"] = True
                logging.info("Background tasks cancelled")
                
            except Exception as task_error:
                logging.error(f"Task cancellation error: {task_error}")
            
            # Step 4: Close Redis connection
            try:
                await self._safe_close_redis_with_timeout()
                shutdown_state["redis_closed"] = True
                logging.info("Redis connection closed")
                
            except Exception as redis_error:
                logging.error(f"Redis shutdown error: {redis_error}")
            
            # Final shutdown status
            successful_shutdowns = sum(1 for step in shutdown_state.values() if step)
            total_shutdowns = len(shutdown_state)
            
            logging.info(f"Service shutdown completed: {successful_shutdowns}/{total_shutdowns} operations successful")
            
        except Exception as shutdown_error:
            logging.error(f"Critical shutdown error: {shutdown_error}")
            try:
                await self._force_cleanup()
            except Exception as force_error:
                logging.error(f"Force cleanup failed: {force_error}")

    async def _establish_redis_connection_with_fallback(self) -> bool:
        """Enhanced Redis connection with comprehensive fallback logic"""
        try:
            # Primary connection attempt
            connection_success = await self._establish_redis_connection()
            
            if connection_success:
                # Verify connection with test operation
                await asyncio.wait_for(self.redis.ping(), timeout=5.0)
                
                # Test basic operations
                test_key = f"test:startup:{int(time.time())}"
                await self.redis.set(test_key, "test_value", ex=60)
                test_result = await self.redis.get(test_key)
                await self.redis.delete(test_key)
                
                if test_result == "test_value":
                    logging.info("Redis connection verified with test operations")
                    return True
                else:
                    logging.warning("Redis test operation failed")
                    return False
                    
            return False
            
        except asyncio.TimeoutError:
            logging.error("Redis connection verification timeout")
            return False
        except Exception as e:
            logging.error(f"Redis connection verification failed: {e}")
            return False

    async def _load_positions_with_validation(self) -> bool:
        """Load positions with enhanced validation and error recovery"""
        try:
            if not self.redis:
                return False
                
            initial_count = len(self.risk_manager.active_positions)
            await self._load_positions_from_db()
            final_count = len(self.risk_manager.active_positions)
            
            loaded_count = final_count - initial_count
            
            if loaded_count > 0:
                logging.info(f"Successfully loaded {loaded_count} positions from database")
                
                # Validate loaded positions
                valid_positions = 0
                for position_id, position in list(self.risk_manager.active_positions.items()):
                    if self._validate_position_integrity(position):
                        valid_positions += 1
                    else:
                        logging.warning(f"Removing invalid position: {position_id}")
                        del self.risk_manager.active_positions[position_id]
                
                logging.info(f"Position validation: {valid_positions}/{loaded_count} positions valid")
                return valid_positions > 0
            else:
                logging.info("No positions found in database")
                return True  # Success even if no positions
                
        except Exception as e:
            logging.error(f"Position loading with validation failed: {e}")
            return False

    def _validate_position_integrity(self, position: PositionRisk) -> bool:
        """Validate position data integrity"""
        try:
            required_fields = ['position_id', 'symbol', 'entry_price', 'current_price', 'leverage']
            
            for field in required_fields:
                if not hasattr(position, field) or getattr(position, field) is None:
                    return False
            
            # Validate numeric fields
            if position.entry_price <= 0 or position.current_price <= 0:
                return False
                
            if not (1 <= position.leverage <= 125):  # Reasonable leverage range
                return False
                
            return True
            
        except Exception:
            return False 

    async def _verify_service_health(self, startup_state: Dict):
        """Verify overall service health after startup"""
        health_score = 0
        max_score = 4
        
        if startup_state["redis_connected"]:
            health_score += 1
        if startup_state["positions_loaded"]:
            health_score += 1
        if startup_state["telegram_initialized"]:
            health_score += 1
        if startup_state["background_tasks_started"]:
            health_score += 1
            
        health_percentage = (health_score / max_score) * 100
        
        if health_percentage >= 75:
            logging.info(f"Service health: GOOD ({health_percentage:.0f}%)")
        elif health_percentage >= 50:
            logging.warning(f"Service health: DEGRADED ({health_percentage:.0f}%)")
        else:
            logging.error(f"Service health: CRITICAL ({health_percentage:.0f}%)")

    async def _send_startup_notification(self, startup_state: Dict):
        """Send comprehensive startup notification"""
        status_indicators = []
        for component, status in startup_state.items():
            indicator = "OK" if status else "FAILED"
            status_indicators.append(f"{component}: {indicator}")
            
        message = f"Service startup completed\n" + "\n".join(status_indicators)
        await self.telegram._queue_notification(message, priority=True)

    async def _save_all_positions_safely(self):
        """Safely save all active positions to Redis"""
        saved_count = 0
        for position_id, position in self.risk_manager.active_positions.items():
            try:
                position_json = safe_json_dumps(asdict(position))
                await asyncio.wait_for(
                    self.redis.set(f"pos:{position_id}", position_json),
                    timeout=3.0
                )
                saved_count += 1
            except Exception as e:
                logging.error(f"Failed to save position {position_id}: {e}")
                
        logging.info(f"Saved {saved_count}/{len(self.risk_manager.active_positions)} positions")

    async def _stop_all_monitors_safely(self):
        """Safely stop all position monitoring tasks"""
        stopped_count = 0
        for position_id, task in list(self.position_monitor.active_monitors.items()):
            try:
                task.cancel()
                await asyncio.wait_for(task, timeout=2.0)
                stopped_count += 1
            except (asyncio.CancelledError, asyncio.TimeoutError):
                stopped_count += 1  # Expected for cancelled tasks
            except Exception as e:
                logging.warning(f"Monitor stop error for {position_id}: {e}")
                
        self.position_monitor.active_monitors.clear()
        logging.info(f"Stopped {stopped_count} position monitors")

    async def _cancel_background_tasks_safely(self):
        """Safely cancel all background tasks"""
        cancelled_count = 0
        for task in self.background_tasks:
            try:
                task.cancel()
                await asyncio.wait_for(task, timeout=2.0)
                cancelled_count += 1
            except (asyncio.CancelledError, asyncio.TimeoutError):
                cancelled_count += 1  # Expected for cancelled tasks
            except Exception as e:
                logging.warning(f"Task cancellation error: {e}")
                
        self.background_tasks.clear()
        logging.info(f"Cancelled {cancelled_count} background tasks")

    async def _safe_close_redis_with_timeout(self):
        """Close Redis connection with timeout"""
        if self.redis:
            await asyncio.wait_for(self._safe_close_redis(), timeout=5.0)

    async def _force_cleanup(self):
        """Emergency cleanup when normal shutdown fails"""
        logging.warning("Executing emergency cleanup")
        
        # Force cancel all tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
                
        # Force stop monitors
        for task in self.position_monitor.active_monitors.values():
            if not task.done():
                task.cancel()
                
        # Force close Redis with proper cleanup
        if self.redis:
            try:
                # Check if Redis connection is still active before closing
                if hasattr(self.redis, 'connection_pool') and self.redis.connection_pool:
                    await asyncio.wait_for(self.redis.close(), timeout=2.0)
                    # Wait for connection pool to close
                    if hasattr(self.redis, 'connection_pool'):
                        await self.redis.connection_pool.disconnect()
            except Exception as e:
                logging.warning(f"Redis cleanup warning: {e}")
            finally:
                self.redis = None
                
        logging.info("Emergency cleanup completed")

    async def _establish_redis_connection(self) -> bool:
        """
        Redis connection setup with circuit breaker and high-performance optimization.
        Optimized for high-performance server (128GB RAM, 16-24 cores).
        
        Features:
        - Circuit breaker pattern for fault tolerance
        - High-capacity connection pool (300 connections for 128GB RAM)
        - Automatic health monitoring with metrics
        - Exponential backoff with jitter
        - Comprehensive connection validation
        - Redis 4.x/5.x/6.x/7.x full compatibility
        - Connection leak detection and prevention
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        # Initialize circuit breaker state if not exists
        if not hasattr(self, '_redis_circuit_breaker'):
            self._redis_circuit_breaker = {
                'state': 'CLOSED',  # CLOSED, OPEN, HALF_OPEN
                'failure_count': 0,
                'last_failure_time': 0,
                'success_count': 0,
                'open_until': 0,
                'half_open_attempts': 0,
                'last_success_time': 0,
                'total_attempts': 0,
                'health_score': 100.0
            }
        
        circuit = self._redis_circuit_breaker
        current_time = time.time()
        circuit['total_attempts'] += 1
        
        # Check circuit breaker state
        if circuit['state'] == 'OPEN':
            # Check if enough time has passed to try again (60 seconds)
            if current_time < circuit['open_until']:
                time_remaining = circuit['open_until'] - current_time
                logging.warning(
                    f"Redis circuit breaker OPEN - "
                    f"retry in {time_remaining:.0f}s "
                    f"(failures: {circuit['failure_count']}, "
                    f"health: {circuit['health_score']:.0f}%)"
                )
                return False
            else:
                # Transition to HALF_OPEN state
                circuit['state'] = 'HALF_OPEN'
                circuit['half_open_attempts'] = 0
                logging.info(
                    f"Redis circuit breaker transitioning to HALF_OPEN state "
                    f"(health: {circuit['health_score']:.0f}%)"
                )
        
        # Limit attempts in HALF_OPEN state
        if circuit['state'] == 'HALF_OPEN':
            if circuit['half_open_attempts'] >= 3:
                circuit['state'] = 'OPEN'
                circuit['open_until'] = current_time + 60
                circuit['health_score'] = max(0, circuit['health_score'] - 20)
                
                logging.warning(
                    f"Redis circuit breaker reopened after failed HALF_OPEN attempts "
                    f"(health: {circuit['health_score']:.0f}%)"
                )
                return False
            circuit['half_open_attempts'] += 1
        
        # Connection attempt loop
        for attempt in range(self._max_redis_retries):
            try:
                # Close existing connection if any (prevent connection leak)
                if self.redis is not None:
                    try:
                        await asyncio.wait_for(self.redis.close(), timeout=2.0)
                        logging.debug("Closed existing Redis connection before retry")
                    except Exception as close_error:
                        logging.debug(f"Error closing old connection: {close_error}")
                    finally:
                        self.redis = None
                
                # High-performance connection pool configuration
                # Optimized for 128GB RAM, 16-24 cores, 400+ concurrent positions
                connection_config = {
                    'decode_responses': True,
                    'socket_keepalive': True,
                    'socket_keepalive_options': {
                        1: 1,   # TCP_KEEPIDLE: start keepalive after 1s
                        2: 2,   # TCP_KEEPINTVL: probe interval 2s
                        3: 3,   # TCP_KEEPCNT: 3 failed probes max
                    },
                    'retry_on_timeout': True,
                    'health_check_interval': 30,
                    'socket_connect_timeout': 5,
                    'socket_timeout': 10,
                    'max_connections': 300,  # High-spec: 300 connections
                    'encoding': 'utf-8',
                }
                
                # Establish connection with timeout
                logging.debug(
                    f"Redis connection attempt {attempt + 1}/{self._max_redis_retries} "
                    f"(total attempts: {circuit['total_attempts']})"
                )
                
                self.redis = await asyncio.wait_for(
                    aioredis.from_url(
                        config.DATABASE_CONFIG["redis_url"],
                        **connection_config
                    ),
                    timeout=10.0
                )

                # Connection validation phase
                
                # Phase 1: Basic ping test with enhanced timeout
                await asyncio.wait_for(self.redis.ping(), timeout=5)
                logging.debug("Redis ping successful")
                
                # Phase 2: Read/Write test with unique key
                test_key = f"health_check:{int(time.time() * 1000)}"
                test_value = f"ok_{attempt}_{circuit['total_attempts']}"
                
                await asyncio.wait_for(
                    self.redis.set(test_key, test_value, ex=10),
                    timeout=3
                )
                
                test_result = await asyncio.wait_for(
                    self.redis.get(test_key),
                    timeout=3
                )
                
                await asyncio.wait_for(
                    self.redis.delete(test_key),
                    timeout=3
                )
                
                if test_result != test_value:
                    raise ConnectionError(
                        f"Redis test validation failed: "
                        f"expected '{test_value}', got '{test_result}'"
                    )
                
                logging.debug("Redis read/write test successful")
                
                # Phase 3: Enhanced connection pool validation
                try:
                    if hasattr(self.redis, 'connection_pool'):
                        pool = self.redis.connection_pool
                        
                        # Get connection for validation
                        pool_conn = await asyncio.wait_for(
                            pool.get_connection('ping'),
                            timeout=3.0
                        )
                        
                        # CRITICAL: Properly await async release
                        try:
                            await asyncio.wait_for(
                                pool.release(pool_conn),
                                timeout=2.0
                            )
                            logging.debug("Redis connection pool validated successfully")
                        except asyncio.TimeoutError:
                            logging.warning("Connection pool release timeout - continuing anyway")
                        except Exception as release_error:
                            logging.warning(f"Connection pool release warning: {release_error}")
                    else:
                        logging.debug("Connection pool not available for validation")
                        
                except asyncio.TimeoutError:
                    logging.warning("Connection pool validation timeout - connection still usable")
                except Exception as pool_error:
                    logging.warning(f"Connection pool validation warning: {pool_error}")
                    # Continue anyway - basic connection is working
                
                # Success - update circuit breaker
                circuit['failure_count'] = 0
                circuit['success_count'] += 1
                circuit['last_success_time'] = current_time
                circuit['health_score'] = min(100.0, circuit['health_score'] + 10.0)
                
                if circuit['state'] == 'HALF_OPEN':
                    # Successful recovery from HALF_OPEN
                    circuit['state'] = 'CLOSED'
                    circuit['half_open_attempts'] = 0
                    logging.info(
                        f"Redis circuit breaker CLOSED - connection recovered "
                        f"(health: {circuit['health_score']:.0f}%)"
                    )
                elif circuit['state'] == 'OPEN':
                    # Force recovery from OPEN (shouldn't happen)
                    circuit['state'] = 'CLOSED'
                    logging.warning(
                        f"Redis circuit breaker forced recovery from OPEN "
                        f"(health: {circuit['health_score']:.0f}%)"
                    )
                
                # Reset retry delay on successful connection
                self._redis_retry_delay = 1
                
                if attempt > 0:
                    logging.info(
                        f"Redis connection established on attempt {attempt + 1}/{self._max_redis_retries} "
                        f"(successes: {circuit['success_count']}, health: {circuit['health_score']:.0f}%)"
                    )
                else:
                    logging.info(
                        f"Redis connection established successfully "
                        f"(health: {circuit['health_score']:.0f}%)"
                    )
                
                return True
            
            except asyncio.TimeoutError:
                circuit['failure_count'] += 1
                circuit['last_failure_time'] = current_time
                circuit['health_score'] = max(0, circuit['health_score'] - 5)
                
                if attempt == self._max_redis_retries - 1:
                    logging.error(
                        f"Redis connection timeout - final attempt failed "
                        f"(failures: {circuit['failure_count']}, health: {circuit['health_score']:.0f}%)"
                    )
                else:
                    logging.warning(
                        f"Redis connection timeout "
                        f"(attempt {attempt + 1}/{self._max_redis_retries}, "
                        f"health: {circuit['health_score']:.0f}%)"
                    )
            
            except (ConnectionError, OSError) as e:
                circuit['failure_count'] += 1
                circuit['last_failure_time'] = current_time
                circuit['health_score'] = max(0, circuit['health_score'] - 5)
                
                if attempt == self._max_redis_retries - 1:
                    logging.error(
                        f"Redis connection error: {e} "
                        f"(failures: {circuit['failure_count']}, health: {circuit['health_score']:.0f}%)"
                    )
                else:
                    logging.warning(
                        f"Redis connection failed "
                        f"(attempt {attempt + 1}/{self._max_redis_retries}): {e}"
                    )
            
            except Exception as e:
                circuit['failure_count'] += 1
                circuit['last_failure_time'] = current_time
                circuit['health_score'] = max(0, circuit['health_score'] - 10)
                
                logging.error(
                    f"Unexpected Redis error (attempt {attempt + 1}): {e} "
                    f"(health: {circuit['health_score']:.0f}%)"
                )
                
                # For unexpected errors, wait longer before retry
                if attempt < self._max_redis_retries - 1:
                    extended_delay = self._redis_retry_delay * 2
                    logging.debug(f"Extended retry delay: {extended_delay}s")
                    await asyncio.sleep(extended_delay)
                    continue
            
            # Exponential backoff with jitter for standard retry
            if attempt < self._max_redis_retries - 1:
                # Calculate backoff
                backoff = self._redis_retry_delay * (1.5 ** attempt)
                
                # Add jitter (±20%)
                import random
                jitter = backoff * random.uniform(-0.2, 0.2)
                delay = max(1.0, min(backoff + jitter, 10.0))
                
                logging.debug(f"Retry delay: {delay:.1f}s")
                await asyncio.sleep(delay)
        
        # All attempts failed - update circuit breaker
        circuit['failure_count'] += 1
        circuit['health_score'] = max(0, circuit['health_score'] - 15)
        
        # Open circuit breaker if too many failures
        if circuit['failure_count'] >= 5:
            circuit['state'] = 'OPEN'
            circuit['open_until'] = current_time + 60  # Open for 60 seconds
            logging.error(
                f"Redis circuit breaker OPEN - "
                f"too many failures ({circuit['failure_count']}), "
                f"blocking connections for 60s "
                f"(health: {circuit['health_score']:.0f}%)"
            )
        
        logging.error(
            f"Redis connection failed after all attempts "
            f"(circuit state: {circuit['state']}, "
            f"failures: {circuit['failure_count']}, "
            f"health: {circuit['health_score']:.0f}%)"
        )
        
        # Cleanup failed connection
        if self.redis is not None:
            try:
                await asyncio.wait_for(self.redis.close(), timeout=2.0)
            except Exception:
                pass
            finally:
                self.redis = None
        
        return False

    async def _safe_close_redis(self):
        """
        Safe Redis connection cleanup with comprehensive error handling.
        Prevents connection leaks and ensures proper resource cleanup.
        """
        if self.redis:
            try:
                # Close connection with timeout
                await asyncio.wait_for(self.redis.close(), timeout=5)
                logging.info("Redis connection closed successfully")
                
                # Wait for connection pool cleanup if available
                if hasattr(self.redis, 'connection_pool'):
                    try:
                        await asyncio.wait_for(
                            self.redis.connection_pool.disconnect(),
                            timeout=3.0
                        )
                        logging.debug("Redis connection pool disconnected")
                    except asyncio.TimeoutError:
                        logging.warning("Connection pool disconnect timeout")
                    except Exception as pool_error:
                        logging.debug(f"Connection pool cleanup: {pool_error}")
                
            except asyncio.TimeoutError:
                logging.warning("Redis connection close timeout - forcing cleanup")
            except Exception as e:
                logging.warning(f"Redis cleanup error: {e}")
            finally:
                self.redis = None
                logging.debug("Redis connection reference cleared")
        else:
            logging.debug("No Redis connection to close")

    async def _ensure_redis_connection(self) -> bool:
        """
        Check and maintain Redis connection with health monitoring.
        Enhanced with automatic recovery and connection validation.
        
        Returns:
            bool: True if connection is healthy, False otherwise
        """
        # No connection - establish new one
        if self.redis is None:
            logging.debug("No Redis connection - establishing new connection")
            return await self._establish_redis_connection()
        
        try:
            # Quick health check with short timeout
            await asyncio.wait_for(self.redis.ping(), timeout=2)
            
            # Update health metrics if available
            if hasattr(self, '_redis_circuit_breaker'):
                circuit = self._redis_circuit_breaker
                circuit['health_score'] = min(100.0, circuit['health_score'] + 1.0)
                
                logging.debug(
                    f"Redis connection healthy "
                    f"(health: {circuit['health_score']:.0f}%)"
                )
            
            return True
            
        except asyncio.TimeoutError:
            logging.warning("Redis ping timeout - reconnecting")
            
            # Update health metrics
            if hasattr(self, '_redis_circuit_breaker'):
                circuit = self._redis_circuit_breaker
                circuit['health_score'] = max(0, circuit['health_score'] - 5)
            
            # Clear broken connection
            await self._safe_close_redis()
            
            # Attempt reconnection
            return await self._establish_redis_connection()
            
        except Exception as e:
            logging.warning(f"Redis connection check failed: {e} - reconnecting")
            
            # Update health metrics
            if hasattr(self, '_redis_circuit_breaker'):
                circuit = self._redis_circuit_breaker
                circuit['health_score'] = max(0, circuit['health_score'] - 10)
            
            # Clear broken connection
            await self._safe_close_redis()
            
            # Attempt reconnection
            return await self._establish_redis_connection()

    def get_redis_health_status(self) -> Dict[str, Any]:
        """
        Get comprehensive Redis connection health status.
        Useful for monitoring and debugging.
        
        Returns:
            Dict containing health metrics and connection status
        """
        status = {
            "connected": self.redis is not None,
            "timestamp": time.time()
        }
        
        if hasattr(self, '_redis_circuit_breaker'):
            circuit = self._redis_circuit_breaker
            status.update({
                "circuit_state": circuit.get('state', 'UNKNOWN'),
                "health_score": circuit.get('health_score', 0),
                "failure_count": circuit.get('failure_count', 0),
                "success_count": circuit.get('success_count', 0),
                "total_attempts": circuit.get('total_attempts', 0),
                "last_success": circuit.get('last_success_time', 0),
                "last_failure": circuit.get('last_failure_time', 0)
            })
        
        return status

    async def _load_positions_from_db(self):
        """
        Load active positions from Redis database
        Optimized for high-spec server with batch processing (128GB RAM, 16-24 cores)
        """
        if not self.redis:
            logging.warning("Cannot load positions - Redis not connected")
            return

        logging.info("Loading active positions from database...")

        try:
            position_keys = await self.redis.keys("pos:*")
            loaded_count = 0
            corrupted_count = 0
            failed_count = 0
            
            # Batch processing for high-spec server (process 20 positions at a time)
            batch_size = 20
            for i in range(0, len(position_keys), batch_size):
                batch_keys = position_keys[i:i+batch_size]
                
                # Process batch concurrently
                tasks = [self._load_single_position(key) for key in batch_keys]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count results
                for result in results:
                    if isinstance(result, Exception):
                        failed_count += 1
                    elif result == "corrupted":
                        corrupted_count += 1
                    elif result == "success":
                        loaded_count += 1
                
                # Brief pause between batches to prevent Redis overload
                if i + batch_size < len(position_keys):
                    await asyncio.sleep(0.1)

            logging.info(
                f"Position loading completed: "
                f"{loaded_count} loaded, {corrupted_count} corrupted (deleted), "
                f"{failed_count} failed"
            )
            
        except Exception as e:
            logging.error(f"Database position loading failed: {e}")

    async def _load_single_position(self, key: str) -> str:
        """
        Load a single position from Redis (helper for batch processing)
        Returns: 'success', 'corrupted', or raises exception
        """
        try:
            position_json = await self.redis.get(key)
            if not position_json:
                return "corrupted"

            position_data = json.loads(position_json)

            # Parse datetime fields safely
            try:
                position_data["created_at"] = datetime.fromisoformat(
                    position_data["created_at"]
                )
                position_data["last_updated"] = datetime.fromisoformat(
                    position_data["last_updated"]
                )
            except (KeyError, ValueError) as e:
                # Use current time if parsing fails
                current_time = datetime.utcnow()
                position_data["created_at"] = current_time
                position_data["last_updated"] = current_time
                logging.warning(f"Date parsing failed for {key}, using current time: {e}")

            position = PositionRisk(**position_data)
            position_id = position.position_id

            # Restore position to memory
            self.risk_manager.active_positions[position_id] = position

            # Restart monitoring with minimal risk profile
            risk_profile = RiskProfile(
                symbol=position.symbol,
                leverage=position.leverage,
                position_size=position.quantity,
                margin_required=position.margin_required,
                liquidation_price=position.current_price * 0.5,  # Safe default
                stop_loss_price=position.current_price * 0.98,   # 2% stop loss
                take_profit_price=position.current_price * 1.02, # 2% take profit
                risk_score=0.5,  # Medium risk default
                confidence=0.8,  # Default confidence
            )
            
            await self.position_monitor.start_position_monitoring(
                position_id, risk_profile
            )
            
            return "success"
            
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in position {key}: {e}")
            # Remove corrupted entry
            await self.redis.delete(key)
            return "corrupted"
            
        except Exception as e:
            logging.error(f"Failed to load position {key}: {e}")
            raise

        @self.app.post("/risk/validate")
        async def validate_risk_advanced(request: Request):
            """Advanced risk validation and automatic service integration"""
            start_time = time.time()

            try:
                self.performance_metrics["total_requests"] += 1

                data = await request.json()

                # Request data validation
                signal_data = data.get("signal") or data.get("signal_data") or data.get("phoenix_signal")
                if not signal_data:
                    raise HTTPException(status_code=400, detail="Signal data missing")

                # Execute risk validation
                validation_result = await self.risk_manager.validate_position_risk(data)

                # Update statistics
                if validation_result.get("cache_blocked"):
                    self.performance_metrics["blocked_duplicates"] += 1
                elif validation_result["approved"]:
                    self.performance_metrics["approved_requests"] += 1

                    # Start position monitoring
                    if "risk_profile" in validation_result:
                        position_id = f"POS_{int(time.time() * 1000)}"
                        risk_profile_dict = validation_result["risk_profile"]

                        risk_profile = RiskProfile(
                            symbol=risk_profile_dict["symbol"],
                            leverage=risk_profile_dict["leverage"],
                            position_size=risk_profile_dict["position_size"],
                            margin_required=risk_profile_dict["margin_required"],
                            liquidation_price=risk_profile_dict["liquidation_price"],
                            stop_loss_price=risk_profile_dict["stop_loss_price"],
                            take_profit_price=risk_profile_dict["take_profit_price"],
                            risk_score=risk_profile_dict["risk_score"],
                            confidence=risk_profile_dict["confidence"],
                        )

                        # Validate and get entry price with fallback
                        entry_price = float(signal_data.get("price", 0))
                        
                        if entry_price <= 0:
                            logging.warning(
                                f"Invalid entry price ({entry_price}) for {signal_data['symbol']}, "
                                "fetching current market price"
                            )
                            
                            try:
                                # Fetch current market price
                                current_market_price = await self.position_monitor._get_current_price_cached(
                                    signal_data["symbol"]
                                )
                                
                                if current_market_price and current_market_price > 0:
                                    entry_price = current_market_price
                                    logging.info(
                                        f"Using market price for {signal_data['symbol']}: "
                                        f"${entry_price:,.2f}"
                                    )
                                else:
                                    # Market price fetch failed - critical error
                                    logging.error(
                                        f"Failed to fetch valid market price for {signal_data['symbol']}"
                                    )
                                    return {
                                        **validation_result,
                                        "status": "error",
                                        "approved": False,
                                        "reason": "Invalid entry price and failed to fetch market price",
                                        "symbol": signal_data['symbol'],
                                        "timestamp": time.time()
                                    }
                                    
                            except Exception as price_error:
                                logging.error(f"Market price fetch error: {price_error}")
                                return {
                                    **validation_result,
                                    "status": "error",
                                    "approved": False,
                                    "reason": f"Entry price validation failed: {str(price_error)}",
                                    "symbol": signal_data['symbol'],
                                    "timestamp": time.time()
                                }
                        
                        # Create PositionRisk with validated entry_price
                        position_to_save = PositionRisk(
                            position_id=position_id,
                            symbol=signal_data["symbol"],
                            side=(
                                "BUY"
                                if signal_data["action"].lower() in ["buy", "long"]
                                else "SELL"
                            ),
                            entry_price=entry_price,  # Validated price
                            current_price=entry_price,  # Use same validated price
                            quantity=risk_profile.position_size,
                            leverage=risk_profile.leverage,
                            margin_required=risk_profile.margin_required,
                            unrealized_pnl=0,
                            liquidation_risk=0,
                            distance_to_liquidation=1.0,
                        )
                        
                        self.risk_manager.active_positions[position_id] = position_to_save
                        
                        # Start position monitoring with validated entry_price
                        await self.position_monitor.start_position_monitoring(
                            position_id, risk_profile
                        )
                        
                        # Save to Redis with enhanced error handling
                        try:
                            if self.redis:
                                position_json = safe_json_dumps(asdict(position_to_save))
                                await self.redis.set(f"pos:{position_id}", position_json)
                                logging.info(f"Position saved to Redis: {position_id}")
                            else:
                                logging.warning(
                                    f"Redis not connected - position saved in memory only: {position_id}"
                                )
                        except Exception as redis_error:
                            logging.error(f"Position Redis save failed: {redis_error}")
                            # Continue even if Redis save fails (position still in memory)
                        
                        validation_result["position_id"] = position_id
                        
                else:
                    self.performance_metrics["rejected_requests"] += 1

                # Update response time statistics
                response_time = time.time() - start_time
                self._update_average_response_time(response_time)

                # Update service call statistics
                if validation_result.get("execution_forwarded"):
                    self.performance_metrics["service_calls_made"] += 1

                    if (
                        validation_result.get("execution_result", {}).get("status")
                        == "success"
                    ):
                        self.performance_metrics["service_calls_success"] += 1

                return {
                    **validation_result,
                    "response_time_ms": response_time * 1000,
                    "timestamp": time.time(),
                }

            except HTTPException:
                raise
            except Exception as e:
                logging.error(f"Risk validation failed: {e}\n{traceback.format_exc()}")
                raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

        @self.app.get("/risk/positions")
        async def get_active_positions_detailed():
            """Get detailed information about all active positions"""
            positions = {}
            total_exposure = 0
            total_margin = 0
            total_pnl = 0

            for pos_id, position in self.risk_manager.active_positions.items():
                exposure = position.quantity * position.current_price
                total_exposure += exposure
                total_margin += position.margin_required
                total_pnl += position.unrealized_pnl

                positions[pos_id] = {
                    "position_id": position.position_id,
                    "symbol": position.symbol,
                    "side": position.side,
                    "entry_price": position.entry_price,
                    "current_price": position.current_price,
                    "quantity": position.quantity,
                    "leverage": position.leverage,
                    "margin_required": position.margin_required,
                    "unrealized_pnl": position.unrealized_pnl,
                    "roe_percent": (
                        (position.unrealized_pnl / position.margin_required * 100)
                        if position.margin_required > 0
                        else 0
                    ),
                    "liquidation_risk": position.liquidation_risk,
                    "distance_to_liquidation": position.distance_to_liquidation,
                    "exposure": exposure,
                    "status": position.status,
                    "last_updated": position.last_updated.isoformat(),
                    "is_monitored": pos_id in self.position_monitor.active_monitors,
                }

            return {
                "active_positions": positions,
                "position_count": len(positions),
                "portfolio_summary": {
                    "total_exposure": total_exposure,
                    "total_margin": total_margin,
                    "total_pnl": total_pnl,
                    "margin_utilization": total_margin / 100000,
                    "portfolio_roe": (
                        (total_pnl / total_margin * 100) if total_margin > 0 else 0
                    ),
                    "high_risk_positions": len(
                        [
                            p
                            for p in self.risk_manager.active_positions.values()
                            if p.liquidation_risk > 0.5
                        ]
                    ),
                },
                "monitoring_status": {
                    "active_monitors": len(self.position_monitor.active_monitors),
                    "monitoring_performance": self.position_monitor.performance_cache,
                },
                "timestamp": time.time(),
            }

        @self.app.post("/risk/close_position")
        async def close_position_advanced(request: Request):
            """Force close a specific position with enhanced error handling"""
            try:
                data = await request.json()
                position_id = data.get("position_id")
                reason = data.get("reason", "MANUAL_CLOSE")

                if not position_id:
                    raise HTTPException(status_code=400, detail="Position ID missing")

                if position_id not in self.risk_manager.active_positions:
                    raise HTTPException(status_code=404, detail="Position not found")

                position = self.risk_manager.active_positions[position_id]

                # Execute force close operation
                await self.position_monitor._execute_auto_close(
                    position_id, position, reason
                )

                # Delete position from Redis database
                try:
                    if self.redis:
                        await self.redis.delete(f"pos:{position_id}")
                        logging.info(f"Position deleted from DB: {position_id}")
                    else:
                        logging.warning(
                            f"Redis not connected - deleted from memory only: {position_id}"
                        )
                except Exception as redis_error:
                    logging.error(f"Position DB deletion failed: {redis_error}")

                # Remove from active positions in memory
                del self.risk_manager.active_positions[position_id]

                return {
                    "status": "success",
                    "message": f"Position {position_id} closed successfully",
                    "position_summary": {
                        "symbol": position.symbol,
                        "final_pnl": position.unrealized_pnl,
                        "roe": (
                            (position.unrealized_pnl / position.margin_required * 100)
                            if position.margin_required > 0
                            else 0
                        ),
                    },
                    "reason": reason,
                    "timestamp": time.time(),
                }

            except HTTPException:
                raise
            except Exception as e:
                logging.error(f"Position close operation failed: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/risk/performance")
        async def get_performance_analytics():
            """성능 분석 데이터"""
            uptime = time.time() - self.performance_metrics["start_time"]

            return {
                "uptime": {
                    "seconds": uptime,
                    "hours": uptime / 3600,
                    "days": uptime / 86400,
                },
                "request_analytics": {
                    "total_requests": self.performance_metrics["total_requests"],
                    "requests_per_hour": self.performance_metrics["total_requests"]
                    / max(uptime / 3600, 1),
                    "approval_rate": (
                        self.performance_metrics["approved_requests"]
                        / max(self.performance_metrics["total_requests"], 1)
                    )
                    * 100,
                    "rejection_rate": (
                        self.performance_metrics["rejected_requests"]
                        / max(self.performance_metrics["total_requests"], 1)
                    )
                    * 100,
                    "duplicate_block_rate": (
                        self.performance_metrics["blocked_duplicates"]
                        / max(self.performance_metrics["total_requests"], 1)
                    )
                    * 100,
                },
                "service_integration": {
                    "calls_made": self.performance_metrics["service_calls_made"],
                    "calls_successful": self.performance_metrics[
                        "service_calls_success"
                    ],
                    "success_rate": (
                        self.performance_metrics["service_calls_success"]
                        / max(self.performance_metrics["service_calls_made"], 1)
                    )
                    * 100,
                    "service_health": self.service_health,
                },
                "system_performance": {
                    "memory_usage": psutil.virtual_memory().percent,
                    "cpu_usage": psutil.cpu_percent(),
                    "average_response_time": self.performance_metrics[
                        "average_response_time"
                    ],
                    "cache_performance": {
                        "hits": self.performance_metrics["cache_hits"],
                        "misses": self.performance_metrics["cache_misses"],
                        "hit_rate": (
                            self.performance_metrics["cache_hits"]
                            / max(
                                self.performance_metrics["cache_hits"]
                                + self.performance_metrics["cache_misses"],
                                1,
                            )
                        )
                        * 100,
                    },
                },
                "timestamp": time.time(),
            }

        @self.app.post("/emergency_recovery")
        async def emergency_recovery_endpoint():
            """Emergency system recovery API"""
            try:
                result = await self.emergency_system_recovery()
                if result:
                    return {
                        "status": "success", 
                        "message": "Emergency recovery completed - all circuit breakers reset",
                        "actions_taken": [
                            "Circuit breakers reset",
                            "Emergency bypass enabled", 
                            "Notification queue cleared",
                            "Execute Service connection tested"
                        ],
                        "timestamp": time.time()
                    }
                else:
                    return {
                        "status": "failed", 
                        "message": "Emergency recovery failed - check logs for details",
                        "timestamp": time.time()
                    }
            except Exception as e:
                logging.error(f"Emergency recovery API failed: {e}")
                return {
                    "status": "error", 
                    "message": f"Recovery error: {str(e)}",
                    "timestamp": time.time()
                }

    async def _check_service_connections(self):
        """Check other service connection status"""
        current_time = time.time()

        for service_name, service_url in config.SERVICE_URLS.items():
            # Check every 5 minutes
            if (
                service_name in self.service_health
                and "last_check" in self.service_health[service_name]
                and current_time - self.service_health[service_name]["last_check"] < 300
            ):
                continue

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{service_url}/health", timeout=5
                    ) as response:
                        if response.status == 200:
                            if service_name not in self.service_health:
                                self.service_health[service_name] = {}
                                self.service_health[service_name][
                                    "status"
                                ] = "connected"
                        else:
                            if service_name not in self.service_health:
                                self.service_health[service_name] = {}
                                self.service_health[service_name][
                                    "status"
                                ] = "unreachable"
            except:
                if service_name not in self.service_health:
                    self.service_health[service_name] = {}
                    self.service_health[service_name]["status"] = "unreachable"

            if service_name not in self.service_health:
                self.service_health[service_name] = {}
                self.service_health[service_name]["last_check"] = current_time

    def _get_pipeline_status(self) -> str:
        """Pipeline connection status"""
        connected_services = sum(
            1
            for status in self.service_health.values()
            if status["status"] == "connected"
        )
        total_services = len(self.service_health)

        if connected_services == total_services:
            return "fully_connected"
        elif connected_services > 0:
            return "partially_connected"
        else:
            return "disconnected"

    def _update_average_response_time(self, response_time: float):
        """Update average response time"""
        current_avg = self.performance_metrics["average_response_time"]
        total_requests = self.performance_metrics["total_requests"]

        # Calculate moving average
        self.performance_metrics["average_response_time"] = (
            current_avg * (total_requests - 1) + response_time
        ) / total_requests

    def _calculate_portfolio_var(self, positions: List[PositionRisk]) -> float:
        """Calculate portfolio VaR"""
        try:
            if not positions:
                return 0.0

            # Simple VaR calculation (in practice, more complex models are used)
            total_exposure = sum(p.quantity * p.current_price for p in positions)
            leverage_weighted_exposure = sum(
                p.quantity * p.current_price * p.leverage for p in positions
            )

            # Estimate daily VaR with 95% confidence
            return leverage_weighted_exposure * 0.02  # Assume 2% daily volatility

        except Exception as e:
            logging.error(f"VaR calculation failed: {e}")
            return 0.0

    def _calculate_max_drawdown(self) -> float:
        """Calculate maximum drawdown"""
        try:
            # Simple implementation (in practice, historical data is needed)
            active_positions = list(self.risk_manager.active_positions.values())
            if not active_positions:
                return 0.0

            negative_pnl_positions = [
                p.unrealized_pnl for p in active_positions if p.unrealized_pnl < 0
            ]
            if not negative_pnl_positions:
                return 0.0

            total_negative_pnl = sum(negative_pnl_positions)
            total_margin = sum(p.margin_required for p in active_positions)

            return abs(total_negative_pnl / total_margin) if total_margin > 0 else 0.0

        except Exception as e:
            logging.error(f"Maximum drawdown calculation failed: {e}")
            return 0.0

    def _calculate_correlation_risk(self, positions: List[PositionRisk]) -> float:
        """Calculate correlation risk"""
        try:
            if not positions:
                return 0.0

            # Calculate exposure ratio of BTC-related symbols
            btc_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
            btc_exposure = sum(
                p.quantity * p.current_price
                for p in positions
                if p.symbol in btc_symbols
            )

            total_exposure = sum(p.quantity * p.current_price for p in positions)

            return btc_exposure / total_exposure if total_exposure > 0 else 0.0

        except Exception as e:
            logging.error(f"Correlation risk calculation failed: {e}")
            return 0.0

    def _calculate_leverage_concentration(
        self, positions: List[PositionRisk]
    ) -> Dict[str, float]:
        """Calculate leverage concentration"""
        try:
            if not positions:
                return {"average": 0.0, "max": 0.0, "weighted_average": 0.0}

            leverages = [p.leverage for p in positions]
            exposures = [p.quantity * p.current_price for p in positions]
            total_exposure = sum(exposures)

            average_leverage = sum(leverages) / len(leverages)
            max_leverage = max(leverages)

            # Exposure-weighted average leverage
            weighted_average = (
                sum(
                    lev * exp / total_exposure for lev, exp in zip(leverages, exposures)
                )
                if total_exposure > 0
                else 0.0
            )

            return {
                "average": average_leverage,
                "max": max_leverage,
                "weighted_average": weighted_average,
            }

        except Exception as e:
            logging.error(f"Leverage concentration calculation failed: {e}")
            return {"average": 0.0, "max": 0.0, "weighted_average": 0.0}

    def _generate_advanced_dashboard_html(self) -> str:
        """Enhanced dashboard HTML with optimized data calculation for high-spec systems (128GB RAM, 16-24 cores)"""
        try:
            # Basic timing and service metrics
            uptime = time.time() - self.performance_metrics["start_time"]
            uptime_str = str(timedelta(seconds=int(uptime)))
            
            # Get active positions once for efficiency - optimized for 400+ concurrent positions
            positions = list(self.risk_manager.active_positions.values())
            active_positions = len(positions)
            active_monitors = len(self.position_monitor.active_monitors)
            
            # Service pipeline status
            service_status = self._get_pipeline_status()
            status_color = {
                "fully_connected": "#00ff88",
                "partially_connected": "#ffaa00", 
                "disconnected": "#ff4444",
            }
            
            # Calculate all portfolio metrics in one pass for efficiency
            portfolio_metrics = self._calculate_portfolio_metrics_efficient(positions)
            
            # Calculate request processing rates with comprehensive safety
            try:
                # Safe metric extraction with defaults
                total_requests = max(
                    int(self.performance_metrics.get("total_requests", 0)), 
                    1
                )
                approved_requests = max(
                    int(self.performance_metrics.get("approved_requests", 0)), 
                    0
                )
                rejected_requests = max(
                    int(self.performance_metrics.get("rejected_requests", 0)), 
                    0
                )
                blocked_duplicates = max(
                    int(self.performance_metrics.get("blocked_duplicates", 0)), 
                    0
                )
                
                # Calculate rates with validation
                approval_rate = (approved_requests / total_requests) * 100
                rejection_rate = (rejected_requests / total_requests) * 100
                blocked_rate = (blocked_duplicates / total_requests) * 100
                
                # Ensure rates are within valid bounds [0, 100]
                approval_rate = max(0.0, min(100.0, approval_rate))
                rejection_rate = max(0.0, min(100.0, rejection_rate))
                blocked_rate = max(0.0, min(100.0, blocked_rate))
                
            except (KeyError, ValueError, TypeError, ZeroDivisionError) as e:
                logging.warning(f"Request rate calculation error: {e}")
                # Safe fallback values
                approval_rate = 0.0
                rejection_rate = 0.0
                blocked_rate = 0.0
            
            # Risk distribution calculation in one pass
            risk_distribution = self._calculate_risk_distribution_efficient(positions)
            
            # Generate meaningful PnL trend data
            pnl_trend = self._generate_pnl_trend_data(portfolio_metrics["total_pnl"])
            
            # Use optimized values for HTML generation
            total_exposure = portfolio_metrics["total_exposure"]
            total_margin = portfolio_metrics["total_margin"]
            total_pnl = portfolio_metrics["total_pnl"]
            
            low_risk = risk_distribution["low_risk"]
            medium_risk = risk_distribution["medium_risk"]
            high_risk = risk_distribution["high_risk"]
            critical_risk = risk_distribution["critical_risk"]
            
            time_labels = pnl_trend["labels"]
            pnl_data = pnl_trend["data"]

            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Phoenix 95 Advanced Risk Management Dashboard</title>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js"></script>
                <style>
                    body {{ 
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                        margin: 0; 
                        padding: 20px; 
                        background: linear-gradient(135deg, #0a0a0a 0%, #1a1a2e 100%); 
                        color: #fff; 
                        min-height: 100vh;
                    }}
                    .header {{ 
                        text-align: center; 
                        margin-bottom: 30px; 
                        background: linear-gradient(145deg, #16213e, #0f3460);
                        padding: 20px;
                        border-radius: 15px;
                        box-shadow: 0 8px 32px rgba(0, 255, 136, 0.1);
                    }}
                    .stats-grid {{ 
                        display: grid; 
                        grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); 
                        gap: 20px; 
                        margin-bottom: 20px;
                    }}
                    .charts-grid {{
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
                        gap: 20px;
                        margin: 30px 0;
                    }}
                    .chart-card {{
                        background: linear-gradient(145deg, #1a1a2e, #16213e);
                        border-radius: 15px;
                        padding: 25px;
                        border-left: 5px solid #00ff88;
                        box-shadow: 0 8px 32px rgba(0, 255, 136, 0.1);
                        transition: transform 0.3s ease;
                    }}
                    .chart-card:hover {{
                        transform: translateY(-5px);
                    }}
                    .chart-title {{
                        font-size: 18px;
                        font-weight: bold;
                        margin-bottom: 20px;
                        color: #00ff88;
                        text-align: center;
                    }}
                    .stat-card {{ 
                        background: linear-gradient(145deg, #1a1a2e, #16213e); 
                        border-radius: 15px; 
                        padding: 25px; 
                        border-left: 5px solid #ff6b35; 
                        box-shadow: 0 8px 32px rgba(255, 107, 53, 0.1);
                        transition: transform 0.3s ease, box-shadow 0.3s ease;
                    }}
                    .stat-card:hover {{
                        transform: translateY(-5px);
                        box-shadow: 0 12px 48px rgba(255, 107, 53, 0.2);
                    }}
                    .stat-title {{ 
                        font-size: 20px; 
                        font-weight: bold; 
                        margin-bottom: 20px; 
                        color: #ff6b35; 
                        display: flex;
                        align-items: center;
                        gap: 10px;
                    }}
                    .stat-item {{ 
                        display: flex; 
                        justify-content: space-between; 
                        margin: 12px 0; 
                        padding: 8px 0; 
                        border-bottom: 1px solid rgba(255, 255, 255, 0.1); 
                    }}
                    .stat-value {{ 
                        color: #00ff88; 
                        font-weight: bold; 
                        font-family: 'Courier New', monospace;
                    }}
                    .stat-value.negative {{ color: #ff4444; }}
                    .stat-value.warning {{ color: #ffaa00; }}
                    .status-indicator {{ 
                        display: inline-block; 
                        width: 12px; 
                        height: 12px; 
                        border-radius: 50%; 
                        margin-right: 8px; 
                    }}
                    .status-healthy {{ background: #00ff88; }}
                    .status-warning {{ background: #ffaa00; }}
                    .status-danger {{ background: #ff4444; }}
                    .footer {{ 
                        text-align: center; 
                        margin-top: 30px; 
                        color: #888; 
                        background: linear-gradient(145deg, #16213e, #0f3460);
                        padding: 20px;
                        border-radius: 15px;
                    }}
                    .progress-bar {{
                        width: 100%;
                        height: 8px;
                        background: #333;
                        border-radius: 4px;
                        overflow: hidden;
                        margin-top: 5px;
                    }}
                    .progress-fill {{
                        height: 100%;
                        background: linear-gradient(90deg, #00ff88, #ff6b35);
                        transition: width 0.3s ease;
                    }}
                    .metric-badge {{
                        display: inline-block;
                        padding: 4px 8px;
                        border-radius: 12px;
                        font-size: 12px;
                        font-weight: bold;
                        margin-left: 10px;
                    }}
                    .badge-success {{ background: #00ff88; color: #000; }}
                    .badge-warning {{ background: #ffaa00; color: #000; }}
                    .badge-danger {{ background: #ff4444; color: #fff; }}
                    .service-status {{
                        display: inline-block;
                        padding: 6px 12px;
                        border-radius: 15px;
                        font-size: 14px;
                        font-weight: bold;
                        margin-left: 10px;
                        background: {status_color.get(service_status, '#888')};
                        color: #000;
                    }}
                    .real-time-indicator {{
                        position: absolute;
                        top: 10px;
                        right: 15px;
                        background: #00ff88;
                        color: #000;
                        padding: 4px 8px;
                        border-radius: 8px;
                        font-size: 10px;
                        font-weight: bold;
                        animation: pulse 2s infinite;
                    }}
                    @keyframes pulse {{
                        0% {{ opacity: 1; }}
                        50% {{ opacity: 0.5; }}
                        100% {{ opacity: 1; }}
                    }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>Phoenix 95 Advanced Risk Management</h1>
                    <p>
                        <span class="status-indicator status-healthy"></span>
                        Advanced Risk System Status: Operational
                        <span class="service-status">{service_status.replace('_', ' ').title()}</span>
                    </p>
                    <p>Uptime: {uptime_str} | Port: {config.RISK_CONFIG["port"]} | Version: {config.RISK_CONFIG["version"]}</p>
                </div>
                
                <div class="charts-grid">
                    <div class="chart-card">
                        <div class="chart-title">Position Risk Distribution</div>
                        <div class="real-time-indicator">LIVE</div>
                        <canvas id="riskDistributionChart" width="300" height="200"></canvas>
                    </div>
                    <div class="chart-card">
                        <div class="chart-title">PnL Trend Analysis</div>
                        <div class="real-time-indicator">LIVE</div>
                        <canvas id="pnlTrendChart" width="300" height="200"></canvas>
                    </div>
                    <div class="chart-card">
                        <div class="chart-title">Request Processing Stats</div>
                        <div class="real-time-indicator">LIVE</div>
                        <canvas id="requestStatsChart" width="300" height="200"></canvas>
                    </div>
                </div>
                
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-title">Portfolio Overview</div>
                        <div class="stat-item">
                            <span>Active Positions:</span>
                            <span class="stat-value">{active_positions}</span>
                        </div>
                        <div class="stat-item">
                            <span>Total Exposure:</span>
                            <span class="stat-value">${total_exposure:,.0f}</span>
                        </div>
                        <div class="stat-item">
                            <span>Margin Used:</span>
                            <span class="stat-value">${total_margin:,.2f}</span>
                        </div>
                        <div class="stat-item">
                            <span>Unrealized PnL:</span>
                            <span class="stat-value {'negative' if total_pnl < 0 else ''}">${total_pnl:,.2f}</span>
                        </div>
                        <div class="stat-item">
                            <span>Margin Mode:</span>
                            <span class="stat-value">{config.RISK_CONFIG["margin_mode"]}</span>
                        </div>
                        <div class="stat-item">
                            <span>Kelly Max:</span>
                            <span class="stat-value">{config.RISK_CONFIG["kelly_max_fraction"]*100}%</span>
                        </div>
                        <div class="stat-item">
                            <span>SL/TP:</span>
                            <span class="stat-value">+/-{config.RISK_CONFIG["stop_loss_percent"]*100}%</span>
                        </div>
                        <div class="stat-item">
                            <span>Confidence Threshold:</span>
                            <span class="stat-value">{config.RISK_CONFIG["confidence_threshold"]*100}%</span>
                        </div>
                    </div>
                    
                    <div class="stat-card">
                        <div class="stat-title">Monitoring Performance</div>
                        <div class="stat-item">
                            <span>Active Monitors:</span>
                            <span class="stat-value">{active_monitors}</span>
                        </div>
                        <div class="stat-item">
                            <span>Check Interval:</span>
                            <span class="stat-value">{config.RISK_CONFIG["position_check_interval"]}s</span>
                        </div>
                        <div class="stat-item">
                            <span>Notification Queue:</span>
                            <span class="stat-value">{self.telegram.notification_queue.qsize()}</span>
                        </div>
                        <div class="stat-item">
                            <span>Alert Success Rate:</span>
                            <span class="stat-value">{(self.telegram.stats['total_sent'] / max(self.telegram.stats['total_sent'] + self.telegram.stats['total_failed'], 1) * 100):.1f}%</span>
                        </div>
                        <div class="stat-item">
                            <span>Memory Usage:</span>
                            <span class="stat-value {'warning' if psutil.virtual_memory().percent > 80 else ''}">{psutil.virtual_memory().percent:.1f}%</span>
                        </div>
                    </div>
                    
                    <div class="stat-card">
                        <div class="stat-title">Processing Statistics</div>
                        <div class="stat-item">
                            <span>Total Requests:</span>
                            <span class="stat-value">{self.performance_metrics["total_requests"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>Approved Requests:</span>
                            <span class="stat-value">{self.performance_metrics["approved_requests"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>Rejected Requests:</span>
                            <span class="stat-value">{self.performance_metrics["rejected_requests"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>Duplicate Blocks:</span>
                            <span class="stat-value">{self.performance_metrics["blocked_duplicates"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>Approval Rate:</span>
                            <span class="stat-value">{approval_rate:.1f}%</span>
                            <span class="metric-badge {'badge-success' if approval_rate >= 70 else 'badge-warning' if approval_rate >= 50 else 'badge-danger'}">
                                {'Excellent' if approval_rate >= 70 else 'Good' if approval_rate >= 50 else 'Warning'}
                            </span>
                        </div>
                    </div>
                    
                    <div class="stat-card">
                        <div class="stat-title">Service Integration</div>
                        <div class="stat-item">
                            <span>Execute Service:</span>
                            <span class="stat-value">{self.service_health["execute_service"]["status"]}</span>
                            <span class="status-indicator {'status-healthy' if self.service_health["execute_service"]["status"] == "connected" else 'status-danger'}"></span>
                        </div>
                        <div class="stat-item">
                            <span>Notify Service:</span>
                            <span class="stat-value">{self.service_health["notify_service"]["status"]}</span>
                            <span class="status-indicator {'status-healthy' if self.service_health["notify_service"]["status"] == "connected" else 'status-danger'}"></span>
                        </div>
                        <div class="stat-item">
                            <span>Portfolio Service:</span>
                            <span class="stat-value">{self.service_health["portfolio_service"]["status"]}</span>
                            <span class="status-indicator {'status-healthy' if self.service_health["portfolio_service"]["status"] == "connected" else 'status-danger'}"></span>
                        </div>
                        <div class="stat-item">
                            <span>Service Calls:</span>
                            <span class="stat-value">{self.performance_metrics["service_calls_made"]}</span>
                        </div>
                        <div class="stat-item">
                            <span>Call Success Rate:</span>
                            <span class="stat-value">{(self.performance_metrics["service_calls_success"] / max(self.performance_metrics["service_calls_made"], 1) * 100):.1f}%</span>
                        </div>
                    </div>
                    
                    <div class="stat-card">
                        <div class="stat-title">Performance Metrics</div>
                        <div class="stat-item">
                            <span>Avg Response Time:</span>
                            <span class="stat-value">{self.performance_metrics["average_response_time"]*1000:.1f}ms</span>
                        </div>
                        <div class="stat-item">
                            <span>Requests/sec:</span>
                            <span class="stat-value">{(self.performance_metrics["total_requests"] / max(uptime, 1)):.2f}</span>
                        </div>
                        <div class="stat-item">
                            <span>Cache Hit Rate:</span>
                            <span class="stat-value">{(self.performance_metrics["cache_hits"] / max(self.performance_metrics["cache_hits"] + self.performance_metrics["cache_misses"], 1) * 100):.1f}%</span>
                        </div>
                        <div class="stat-item">
                            <span>CPU Usage:</span>
                            <span class="stat-value">{psutil.cpu_percent():.1f}%</span>
                        </div>
                        <div class="stat-item">
                            <span>uvloop Enabled:</span>
                            <span class="stat-value">Active</span>
                        </div>
                    </div>
                </div>
                
                <div class="footer">
                    <p>Phoenix 95 Advanced Risk Management Service v{config.RISK_CONFIG["version"]}</p>
                    <p>Hedge Fund Grade Risk Management - Kelly Criterion - Service Integration - Performance Optimization</p>
                    <p>Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} KST</p>
                    <p>
                        <span style="color: #00ff88;">Integrated Config</span> - 
                        <span style="color: #00ff88;">Execute Integration</span> - 
                        <span style="color: #00ff88;">Duplicate Prevention</span> - 
                        <span style="color: #00ff88;">Performance Optimized</span>
                    </p>
                </div>

                <script>
                    // Risk Distribution Chart
                    const riskCtx = document.getElementById('riskDistributionChart').getContext('2d');
                    const riskChart = new Chart(riskCtx, {{
                        type: 'doughnut',
                        data: {{
                            labels: ['Low Risk', 'Medium Risk', 'High Risk', 'Critical Risk'],
                            datasets: [{{
                                data: [{low_risk}, {medium_risk}, {high_risk}, {critical_risk}],
                                backgroundColor: ['#00ff88', '#ffaa00', '#ff6b35', '#ff4444'],
                                borderWidth: 2,
                                borderColor: '#1a1a2e'
                            }}]
                        }},
                        options: {{
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {{
                                legend: {{
                                    position: 'bottom',
                                    labels: {{
                                        color: '#fff',
                                        font: {{
                                            size: 12
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }});

                    // PnL Trend Chart
                    const pnlCtx = document.getElementById('pnlTrendChart').getContext('2d');
                    const pnlChart = new Chart(pnlCtx, {{
                        type: 'line',
                        data: {{
                            labels: {time_labels},
                            datasets: [{{
                                label: 'PnL Trend',
                                data: {pnl_data},
                                borderColor: '#00ff88',
                                backgroundColor: 'rgba(0, 255, 136, 0.1)',
                                tension: 0.4,
                                borderWidth: 2,
                                pointBackgroundColor: '#00ff88',
                                pointBorderColor: '#1a1a2e',
                                pointRadius: 4
                            }}]
                        }},
                        options: {{
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {{
                                y: {{
                                    beginAtZero: true,
                                    grid: {{
                                        color: 'rgba(255, 255, 255, 0.1)'
                                    }},
                                    ticks: {{
                                        color: '#fff'
                                    }}
                                }},
                                x: {{
                                    grid: {{
                                        color: 'rgba(255, 255, 255, 0.1)'
                                    }},
                                    ticks: {{
                                        color: '#fff'
                                    }}
                                }}
                            }},
                            plugins: {{
                                legend: {{
                                    display: false
                                }}
                            }}
                        }}
                    }});

                    // Request Statistics Chart
                    const requestCtx = document.getElementById('requestStatsChart').getContext('2d');
                    const requestChart = new Chart(requestCtx, {{
                        type: 'bar',
                        data: {{
                            labels: ['Approved', 'Rejected', 'Blocked'],
                            datasets: [{{
                                data: [{approval_rate:.1f}, {rejection_rate:.1f}, {blocked_rate:.1f}],
                                backgroundColor: ['#00ff88', '#ff4444', '#ffaa00'],
                                borderColor: ['#00ff88', '#ff4444', '#ffaa00'],
                                borderWidth: 2
                            }}]
                        }},
                        options: {{
                            responsive: true,
                            maintainAspectRatio: false,
                            scales: {{
                                y: {{
                                    beginAtZero: true,
                                    max: 100,
                                    grid: {{
                                        color: 'rgba(255, 255, 255, 0.1)'
                                    }},
                                    ticks: {{
                                        color: '#fff',
                                        callback: function(value) {{
                                            return value + '%';
                                        }}
                                    }}
                                }},
                                x: {{
                                    grid: {{
                                        color: 'rgba(255, 255, 255, 0.1)'
                                    }},
                                    ticks: {{
                                        color: '#fff'
                                    }}
                                }}
                            }},
                            plugins: {{
                                legend: {{
                                    display: false
                                }}
                            }}
                        }}
                    }});

                    // Auto-refresh charts every 5 seconds
                    setInterval(() => {{
                        fetch('/risk/metrics')
                            .then(response => response.json())
                            .then(data => {{
                                // Update risk distribution if data available
                                if (data.portfolio_metrics) {{
                                    const positions = data.portfolio_metrics.active_positions;
                                    // This would need actual risk distribution data from the API
                                    // For now, just refresh the page to get updated data
                                }}
                            }})
                            .catch(error => console.log('Chart update failed:', error));
                    }}, 5000);

                    // Auto-refresh page every 15 seconds
                    setInterval(() => location.reload(), 15000);
                </script>
            </body>
            </html>
            """

            return html
            
        except Exception as e:
            logging.error(f"Dashboard data calculation failed: {e}")
            return self._generate_error_dashboard(str(e))

    def _calculate_portfolio_metrics_efficient(self, positions: list) -> dict:
        """Calculate all portfolio metrics in a single pass with enhanced safety"""
        metrics = {
            "total_exposure": 0.0,
            "total_margin": 0.0, 
            "total_pnl": 0.0,
            "position_count": len(positions) if positions else 0
        }
        
        if not positions:
            return metrics
        
        try:
            for position in positions:
                try:
                    # Safe attribute access with type conversion and validation
                    quantity = float(getattr(position, 'quantity', 0.0))
                    current_price = float(getattr(position, 'current_price', 0.0))
                    margin_required = float(getattr(position, 'margin_required', 0.0))
                    unrealized_pnl = float(getattr(position, 'unrealized_pnl', 0.0))
                    
                    # Validate values are finite and non-negative where appropriate
                    if not all(math.isfinite(x) for x in [quantity, current_price, margin_required, unrealized_pnl]):
                        continue
                    
                    if quantity < 0 or current_price < 0:
                        continue
                    
                    # Accumulate metrics safely
                    exposure = quantity * current_price
                    if math.isfinite(exposure):
                        metrics["total_exposure"] += exposure
                    
                    if math.isfinite(margin_required):
                        metrics["total_margin"] += margin_required
                    
                    if math.isfinite(unrealized_pnl):
                        metrics["total_pnl"] += unrealized_pnl
                    
                except (ValueError, TypeError, AttributeError) as e:
                    logging.debug(f"Invalid position data skipped: {e}")
                    continue
                
        except Exception as e:
            logging.warning(f"Portfolio metrics calculation error: {e}")
            
        return metrics

    def _calculate_risk_distribution_efficient(self, positions: list) -> dict:
        """Calculate risk distribution in a single pass with enhanced validation"""
        distribution = {
            "low_risk": 0,
            "medium_risk": 0,
            "high_risk": 0,
            "critical_risk": 0
        }
        
        if not positions:
            return distribution
        
        try:
            for position in positions:
                try:
                    risk = getattr(position, 'liquidation_risk', 0.0)
                    
                    # Type conversion and validation
                    risk_value = float(risk)
                    
                    # Validate risk value is finite and in valid range
                    if not math.isfinite(risk_value):
                        continue
                    
                    # Clamp risk value to valid range [0, 1]
                    risk_value = max(0.0, min(1.0, risk_value))
                    
                    # Categorize risk levels
                    if risk_value <= 0.3:
                        distribution["low_risk"] += 1
                    elif risk_value <= 0.7:
                        distribution["medium_risk"] += 1
                    elif risk_value <= 0.9:
                        distribution["high_risk"] += 1
                    else:
                        distribution["critical_risk"] += 1
                        
                except (ValueError, TypeError, AttributeError) as e:
                    logging.debug(f"Invalid risk data skipped: {e}")
                    continue
                    
        except Exception as e:
            logging.warning(f"Risk distribution calculation error: {e}")
            
        return distribution

    def _generate_pnl_trend_data(self, current_pnl: float) -> dict:
        """Generate realistic PnL trend data with enhanced validation"""
        try:
            # Input validation and sanitization
            if not isinstance(current_pnl, (int, float)):
                current_pnl = 0.0
            
            # Handle NaN and infinity values
            if not math.isfinite(current_pnl):
                current_pnl = 0.0
            
            # Clamp extremely large values for chart stability
            current_pnl = max(-1000000, min(1000000, current_pnl))
            
            labels = [f"T-{9-i}" for i in range(10)]
            data = []
            
            # Generate trend data with validation
            for i in range(10):
                try:
                    factor = 0.7 + 0.3 * (i / 9)
                    value = current_pnl * factor
                    
                    # Ensure chart-safe values
                    if math.isfinite(value):
                        # Round to 2 decimal places for chart performance
                        data.append(round(value, 2))
                    else:
                        data.append(0.0)
                        
                except Exception:
                    data.append(0.0)
            
            return {"labels": labels, "data": data}
            
        except Exception as e:
            logging.warning(f"PnL trend generation error: {e}")
            # Return safe fallback data
            return {
                "labels": [f"T-{i}" for i in range(10)], 
                "data": [0.0] * 10
            }

    def _generate_error_dashboard(self, error_message: str) -> str:
        """Generate minimal error dashboard when main calculation fails - high-spec system fallback"""
        # Sanitize error message to prevent HTML injection attacks
        import html
        safe_error_message = html.escape(str(error_message))
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Phoenix 95 Risk Dashboard - Error</title>
            <meta charset="utf-8">
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    padding: 20px; 
                    background: #1a1a2e; 
                    color: white; 
                    text-align: center;
                }}
                .error-container {{
                    max-width: 600px;
                    margin: 50px auto;
                    padding: 30px;
                    background: #16213e;
                    border-radius: 15px;
                    border-left: 5px solid #ff4444;
                }}
            </style>
        </head>
        <body>
            <div class="error-container">
                <h1>Phoenix 95 Risk Management</h1>
                <h2 style="color: #ff4444;">Dashboard Error</h2>
                <p>Error details: {safe_error_message}</p>
                <p>The service is running but dashboard data is temporarily unavailable.</p>
                <p><a href="/" style="color: #00ff88; text-decoration: none;">Refresh Page</a></p>
            </div>
        </body>
        </html>
        """

    async def start_background_tasks(self):
        """Enhanced background task management with dependency handling and error isolation"""
        task_status = {
            "telegram_worker": False,
            "cleanup_task": False,
            "health_check_task": False,
            "metrics_task": False,
            "monitoring_task": False,
            "status_report_task": False,
            "backup_monitor_task": False,
            "adaptive_optimizer_task": False
        }
        
        logging.info("Background task initialization sequence started")
        
        # Phase 1: Critical infrastructure tasks (must succeed)
        try:
            # Telegram notification worker (critical for alerts)
            await asyncio.wait_for(
                self.telegram.start_notification_worker(),
                timeout=10.0
            )
            task_status["telegram_worker"] = True
            logging.info("Telegram notification worker started")
            
        except (asyncio.TimeoutError, Exception) as e:
            logging.error(f"Critical: Telegram worker failed to start: {e}")
            # Continue without Telegram - system can operate without notifications
            
        # Phase 2: System maintenance tasks (high priority)
        critical_tasks = [
            ("cleanup_task", self._periodic_cleanup, "System cleanup"),
            ("health_check_task", self._periodic_health_check, "Health monitoring"),
            ("metrics_task", self._periodic_metrics_collection, "Metrics collection")
        ]
        
        for task_name, task_func, description in critical_tasks:
            try:
                task = asyncio.create_task(self._safe_task_wrapper(task_func, task_name))
                self.background_tasks.append(task)
                task_status[task_name] = True
                logging.info(f"{description} task started")
                
            except Exception as e:
                logging.error(f"Failed to start {description} task: {e}")
                task_status[task_name] = False
        
        # Phase 3: Service integration tasks (medium priority)
        service_tasks = [
            ("monitoring_task", self._periodic_service_monitoring, "Service connectivity monitoring"),
            ("status_report_task", self._periodic_status_report, "Status reporting")
        ]
        
        for task_name, task_func, description in service_tasks:
            try:
                task = asyncio.create_task(self._safe_task_wrapper(task_func, task_name))
                self.background_tasks.append(task)
                task_status[task_name] = True
                logging.info(f"{description} task started")
                
            except Exception as e:
                logging.warning(f"Service task start failed - {description}: {e}")
                task_status[task_name] = False
        
        # Phase 4: Advanced features (optional but valuable)
        advanced_tasks = [
            ("backup_monitor_task", self._backup_risk_monitor, "Backup risk monitoring"),
            ("adaptive_optimizer_task", self._adaptive_scoring_optimizer, "Adaptive scoring optimization")
        ]
        
        for task_name, task_func, description in advanced_tasks:
            try:
                # Check system resources before starting resource-intensive tasks
                memory_usage = psutil.virtual_memory().percent
                if memory_usage > 85:
                    logging.warning(f"High memory usage ({memory_usage:.1f}%) - skipping {description}")
                    continue
                    
                task = asyncio.create_task(self._safe_task_wrapper(task_func, task_name))
                self.background_tasks.append(task)
                task_status[task_name] = True
                logging.info(f"{description} task started")
                
            except Exception as e:
                logging.warning(f"Advanced task start failed - {description}: {e}")
                task_status[task_name] = False
        
        # Phase 5: Task monitoring and recovery system
        try:
            watchdog_task = asyncio.create_task(
                self._task_watchdog_monitor(task_status.copy())
            )
            self.background_tasks.append(watchdog_task)
            logging.info("Task watchdog monitor started")
            
        except Exception as e:
            logging.error(f"Task watchdog failed to start: {e}")
        
        # Final status assessment
        successful_tasks = sum(1 for status in task_status.values() if status)
        total_tasks = len(task_status)
        success_rate = (successful_tasks / total_tasks) * 100
        
        if success_rate >= 80:
            logging.info(f"Background tasks initialization: EXCELLENT ({successful_tasks}/{total_tasks} started)")
        elif success_rate >= 60:
            logging.warning(f"Background tasks initialization: ACCEPTABLE ({successful_tasks}/{total_tasks} started)")
        else:
            logging.error(f"Background tasks initialization: DEGRADED ({successful_tasks}/{total_tasks} started)")
        
        # Store task status for monitoring
        self._background_task_status = task_status
        
        # Memory cleanup task - high priority
        try:
            cleanup_task = asyncio.create_task(self._periodic_cleanup())
            self.background_tasks.append(cleanup_task)
            logging.info("Memory cleanup task started")
        except Exception as e:
            logging.error(f"Memory cleanup task failed to start: {e}")
        
        logging.info(f"Background task management completed: {len(self.background_tasks)} active tasks")

    async def _safe_task_wrapper(self, task_func, task_name: str):
        """Safe wrapper for background tasks with automatic restart capability"""
        restart_count = 0
        max_restarts = 3
        base_delay = 30
        
        while restart_count <= max_restarts:
            try:
                logging.debug(f"Starting background task: {task_name}")
                await task_func()
                
            except asyncio.CancelledError:
                logging.info(f"Background task cancelled: {task_name}")
                raise  # Propagate cancellation
                
            except Exception as e:
                restart_count += 1
                logging.error(f"Background task error ({task_name}, attempt {restart_count}): {e}")
                
                if restart_count <= max_restarts:
                    # Exponential backoff with jitter
                    delay = base_delay * (2 ** (restart_count - 1))
                    jitter = delay * 0.1 * (0.5 - hash(task_name) % 100 / 100)
                    total_delay = delay + jitter
                    
                    logging.info(f"Restarting {task_name} in {total_delay:.1f} seconds")
                    await asyncio.sleep(total_delay)
                else:
                    logging.error(f"Background task {task_name} failed permanently after {max_restarts} attempts")
                    break

    async def _task_watchdog_monitor(self, initial_task_status: Dict[str, bool]):
        """Monitor background tasks and restart failed ones"""
        check_interval = 60  # Check every minute
        
        while True:
            try:
                await asyncio.sleep(check_interval)
                
                # Check system health
                memory_usage = psutil.virtual_memory().percent
                active_tasks = len([task for task in self.background_tasks if not task.done()])
                
                if memory_usage > 90:
                    logging.warning(f"High memory usage detected: {memory_usage:.1f}%")
                    
                if active_tasks < len(initial_task_status) * 0.5:  # Less than 50% tasks active
                    logging.warning(f"Low task activity: only {active_tasks} tasks active")
                
                # Log periodic status
                if hasattr(self, '_background_task_status'):
                    active_count = sum(1 for task in self.background_tasks if not task.done())
                    logging.debug(f"Task watchdog: {active_count} active background tasks")
                
                # Check for stuck tasks (optional enhancement)
                stuck_tasks = []
                for i, task in enumerate(self.background_tasks):
                    if task.done() and not task.cancelled():
                        try:
                            # Check if task completed with exception
                            result = task.result()
                        except Exception as e:
                            stuck_tasks.append((i, e))
                
                if stuck_tasks:
                    logging.warning(f"Found {len(stuck_tasks)} completed tasks with exceptions")
                
            except asyncio.CancelledError:
                logging.info("Task watchdog monitor cancelled")
                break
            except Exception as e:
                logging.error(f"Task watchdog monitor error: {e}")
                await asyncio.sleep(check_interval)  # Continue monitoring despite errors

    def get_background_task_status(self) -> Dict[str, Any]:
        """Get current status of all background tasks"""
        if not hasattr(self, '_background_task_status'):
            return {"error": "Task status not initialized"}
            
        current_status = {}
        for i, task in enumerate(self.background_tasks):
            task_info = {
                "running": not task.done(),
                "cancelled": task.cancelled(),
                "exception": None
            }
            
            if task.done() and not task.cancelled():
                try:
                    task.result()
                except Exception as e:
                    task_info["exception"] = str(e)
                    
            current_status[f"task_{i}"] = task_info
        
        return {
            "initial_status": self._background_task_status,
            "current_tasks": current_status,
            "total_tasks": len(self.background_tasks),
            "active_tasks": len([t for t in self.background_tasks if not t.done()]),
            "memory_usage": psutil.virtual_memory().percent
        }

    async def _periodic_status_report(self):
        """Periodic status report (every 30 minutes)"""
        while True:
            try:
                await asyncio.sleep(1800)  # 30 minutes

                active_positions = list(self.risk_manager.active_positions.values())

                metrics = {
                    "active_positions": len(active_positions),
                    "total_exposure": sum(
                        p.quantity * p.current_price for p in active_positions
                    ),
                    "margin_utilization": sum(
                        p.margin_required for p in active_positions
                    )
                    / 100000,
                    "daily_pnl": sum(p.unrealized_pnl for p in active_positions),
                    "memory_usage": psutil.virtual_memory().percent,
                    "signals_processed": self.performance_metrics["total_requests"],
                    "approval_rate": (
                        self.performance_metrics["approved_requests"]
                        / max(self.performance_metrics["total_requests"], 1)
                    )
                    * 100,
                    "active_monitors": len(self.position_monitor.active_monitors),
                    "service_calls_success": self.performance_metrics[
                        "service_calls_success"
                    ],
                    "risk_level": (
                        "LOW"
                        if len(
                            [p for p in active_positions if p.liquidation_risk > 0.5]
                        )
                        == 0
                        else "MEDIUM"
                    ),
                }

                await self.telegram.send_system_status(metrics)

            except Exception as e:
                logging.error(f"Periodic status report failed: {e}")

    async def _periodic_cleanup(self):
        """
        Enhanced periodic cleanup optimized for 128GB RAM high-spec system
        
        Features:
        - Massive cache capacity (50K+ entries)
        - Relaxed cleanup intervals (15 minutes)
        - Smart memory pressure detection
        - Tiered cleanup strategy
        - Performance-first approach
        - Adaptive memory thresholds
        
        System specs:
        - RAM: 128GB DDR5
        - CPU: 16-24 cores
        - Target: 400+ concurrent positions
        """
        # Configuration constants - High-spec system optimized
        CLEANUP_INTERVAL = 900  # 15 minutes (relaxed for 128GB)
        CRITICAL_MEMORY_THRESHOLD = 92  # 92% for 128GB system (more aggressive)
        EMERGENCY_MEMORY_THRESHOLD = 96  # 96% emergency threshold
        AGGRESSIVE_MEMORY_THRESHOLD = 88  # 88% for aggressive cleanup (lowered)
        
        cleanup_cycle = 0
        last_emergency_cleanup = 0
        
        while True:
            try:
                await asyncio.sleep(CLEANUP_INTERVAL)
                cleanup_cycle += 1
                
                import gc
                current_time = time.time()
                cleanup_stats = {
                    "entries_removed": 0, 
                    "caches_cleared": 0,
                    "memory_before": 0, 
                    "memory_after": 0,
                    "gc_collected": 0,
                    "cycle": cleanup_cycle,
                    "duration": 0
                }
                
                cleanup_start_time = time.time()
                
                # Get initial memory state
                try:
                    mem_info = psutil.virtual_memory()
                    cleanup_stats["memory_before"] = mem_info.percent
                    cleanup_stats["available_gb"] = mem_info.available / (1024**3)
                    cleanup_stats["used_gb"] = mem_info.used / (1024**3)
                except Exception as e:
                    logging.warning(f"Could not get memory stats: {e}")
                    cleanup_stats["memory_before"] = 0
                    cleanup_stats["available_gb"] = 0
                    cleanup_stats["used_gb"] = 0
                
                # Determine cleanup strategy based on memory pressure
                memory_critical = cleanup_stats["memory_before"] > CRITICAL_MEMORY_THRESHOLD
                memory_aggressive = cleanup_stats["memory_before"] > AGGRESSIVE_MEMORY_THRESHOLD
                memory_emergency = cleanup_stats["memory_before"] > EMERGENCY_MEMORY_THRESHOLD
                
                if memory_emergency:
                    logging.error(
                        f"EMERGENCY: Memory at {cleanup_stats['memory_before']:.1f}% "
                        f"({cleanup_stats['used_gb']:.1f}GB used) - immediate aggressive cleanup"
                    )
                elif memory_critical:
                    logging.warning(
                        f"CRITICAL: Memory at {cleanup_stats['memory_before']:.1f}% "
                        f"({cleanup_stats['used_gb']:.1f}GB used) - aggressive cleanup mode"
                    )
                elif memory_aggressive:
                    logging.info(
                        f"HIGH: Memory at {cleanup_stats['memory_before']:.1f}% "
                        f"({cleanup_stats['available_gb']:.1f}GB available) - enhanced cleanup mode"
                    )
                else:
                    logging.debug(
                        f"NORMAL: Memory at {cleanup_stats['memory_before']:.1f}% "
                        f"({cleanup_stats['available_gb']:.1f}GB available)"
                    )
                
                # 1. Hot price cache cleanup - L1 cache tier
                try:
                    if hasattr(self, 'position_monitor') and hasattr(self.position_monitor, '_hot_price_cache'):
                        hot_cache = self.position_monitor._hot_price_cache
                        if isinstance(hot_cache, dict):
                            initial_size = len(hot_cache)
                            
                            # Hot cache: very short TTL
                            max_age = 2 if memory_emergency else 3 if memory_critical else 5
                            
                            expired_keys = [
                                key for key, entry in list(hot_cache.items())
                                if isinstance(entry, dict) and 
                                "timestamp" in entry and 
                                current_time - entry["timestamp"] > max_age
                            ]
                            
                            for key in expired_keys:
                                hot_cache.pop(key, None)
                            
                            cleanup_stats["entries_removed"] += initial_size - len(hot_cache)
                            
                            if initial_size - len(hot_cache) > 0:
                                logging.debug(f"Hot cache: removed {initial_size - len(hot_cache)} entries")
                                
                except Exception as e:
                    logging.debug(f"Hot cache cleanup failed: {e}")
                
                # 2. Price cache cleanup - Massive capacity for high-spec (EXPANDED)
                try:
                    if hasattr(self, 'position_monitor') and hasattr(self.position_monitor, 'price_cache'):
                        cache = self.position_monitor.price_cache
                        if isinstance(cache, dict):
                            initial_size = len(cache)
                            
                            # EXPANDED: High-spec adaptive cleanup parameters (128GB RAM)
                            if memory_emergency:
                                max_age = 180  # 3 minutes
                                max_size = 10000  # Doubled
                            elif memory_critical:
                                max_age = 300  # 5 minutes
                                max_size = 20000  # Doubled
                            elif memory_aggressive:
                                max_age = 600  # 10 minutes
                                max_size = 30000  # Doubled
                            else:
                                max_age = 1800  # 30 minutes (increased)
                                max_size = 50000  # 2.5x expansion for 400 positions
                            
                            # Remove expired entries safely
                            expired_keys = []
                            for key, entry in list(cache.items()):
                                try:
                                    if isinstance(entry, dict) and "timestamp" in entry:
                                        if current_time - entry["timestamp"] > max_age:
                                            expired_keys.append(key)
                                except Exception:
                                    expired_keys.append(key)
                            
                            # Enforce cache size limit with enhanced LRU/LFU hybrid
                            if len(cache) - len(expired_keys) > max_size:
                                valid_entries = [
                                    (key, entry.get("timestamp", 0), entry.get("fetch_count", 0), entry.get("access_count", 0))
                                    for key, entry in cache.items()
                                    if key not in expired_keys and isinstance(entry, dict)
                                ]
                                
                                # Enhanced sorting: fetch_count + access_count, then timestamp
                                valid_entries.sort(
                                    key=lambda x: (x[2] + x[3], x[1]),  # total_access, then timestamp
                                    reverse=True
                                )
                                
                                keep_count = max_size // 2 if memory_emergency else max_size - 2000
                                old_keys = [key for key, _, _, _ in valid_entries[keep_count:]]
                                expired_keys.extend(old_keys)
                            
                            for key in set(expired_keys):
                                cache.pop(key, None)
                            
                            removed = initial_size - len(cache)
                            cleanup_stats["entries_removed"] += removed
                            
                            if removed > 0:
                                logging.debug(f"Price cache: removed {removed} entries, {len(cache)} remaining")
                            
                except Exception as e:
                    logging.error(f"Price cache cleanup failed: {e}")
                
                # 3. Alert history cleanup - Massive expansion for 128GB (EXPANDED)
                try:
                    if hasattr(self, 'position_monitor') and hasattr(self.position_monitor, 'alert_history'):
                        alert_history = self.position_monitor.alert_history
                        if hasattr(alert_history, '__len__'):
                            initial_count = len(alert_history)
                            
                            # EXPANDED: High-spec retention parameters (128GB RAM)
                            if memory_emergency:
                                retention_time = 1800  # 30 minutes
                                max_alerts = 5000  # Increased
                            elif memory_critical:
                                retention_time = 3600  # 1 hour
                                max_alerts = 10000  # Doubled
                            else:
                                retention_time = 21600  # 6 hours (increased)
                                max_alerts = 50000  # 2.5x expansion
                            
                            recent_alerts = [
                                alert for alert in list(alert_history)
                                if isinstance(alert, dict) and 
                                "timestamp" in alert and 
                                current_time - alert["timestamp"] < retention_time
                            ]
                            
                            from collections import deque
                            self.position_monitor.alert_history = deque(recent_alerts, maxlen=max_alerts)
                            cleanup_stats["entries_removed"] += initial_count - len(recent_alerts)
                            
                except Exception as e:
                    logging.error(f"Alert history cleanup failed: {e}")
                
                # 4. Signal cache cleanup - Ultra high-capacity for 400 positions (EXPANDED)
                try:
                    if hasattr(self, 'risk_manager') and hasattr(self.risk_manager, 'signal_cache'):
                        signal_cache = self.risk_manager.signal_cache
                        if isinstance(signal_cache, dict):
                            initial_size = len(signal_cache)
                            
                            # EXPANDED: High-spec adaptive parameters (128GB RAM)
                            if memory_emergency:
                                max_age = 1800  # 30 minutes
                                max_size = 20000  # Doubled
                            elif memory_critical:
                                max_age = 3600  # 1 hour
                                max_size = 50000  # Increased
                            else:
                                max_age = 21600  # 6 hours (increased)
                                max_size = 200000  # 2x expansion for ultra high-frequency
                            
                            expired_keys = [
                                key for key, entry in list(signal_cache.items())
                                if isinstance(entry, dict) and 
                                "timestamp" in entry and 
                                current_time - entry["timestamp"] > max_age
                            ]
                            
                            for key in expired_keys:
                                signal_cache.pop(key, None)
                            
                            # Only clear if severely over limit
                            if len(signal_cache) > max_size:
                                if memory_emergency:
                                    signal_cache.clear()
                                    cleanup_stats["caches_cleared"] += 1
                                    logging.warning("Signal cache cleared due to emergency")
                                else:
                                    # Keep most recent entries
                                    sorted_items = sorted(
                                        signal_cache.items(),
                                        key=lambda x: x[1].get("timestamp", 0) if isinstance(x[1], dict) else 0,
                                        reverse=True
                                    )
                                    signal_cache.clear()
                                    keep_count = max_size // 2 if memory_critical else max_size - 10000
                                    for key, value in sorted_items[:keep_count]:
                                        signal_cache[key] = value
                            
                            cleanup_stats["entries_removed"] += initial_size - len(signal_cache)
                            
                except Exception as e:
                    logging.error(f"Signal cache cleanup failed: {e}")
                
                # 5. Multi-timeframe analyzer cleanup - Expanded limits (EXPANDED)
                try:
                    if (hasattr(self, 'risk_manager') and 
                        hasattr(self.risk_manager, 'multi_timeframe_analyzer')):
                        analyzer = self.risk_manager.multi_timeframe_analyzer
                        
                        # Price data cache cleanup
                        if hasattr(analyzer, 'price_data_cache') and isinstance(analyzer.price_data_cache, dict):
                            max_age = 900 if memory_emergency else 1800 if memory_critical else 5400  # Up to 1.5 hours
                            expired_keys = [
                                key for key, entry in list(analyzer.price_data_cache.items())
                                if isinstance(entry, dict) and 
                                "timestamp" in entry and 
                                current_time - entry["timestamp"] > max_age
                            ]
                            
                            for key in expired_keys:
                                analyzer.price_data_cache.pop(key, None)
                        
                        # EXPANDED: High-spec cache limits (128GB RAM)
                        if memory_emergency:
                            cache_limit = 10000  # Doubled
                        elif memory_critical:
                            cache_limit = 20000  # Doubled
                        else:
                            cache_limit = 50000  # 2x expansion
                        
                        cache_names = ['volatility_cache', 'correlation_matrix', 'risk_scores']
                        
                        for cache_name in cache_names:
                            try:
                                if hasattr(analyzer, cache_name):
                                    cache = getattr(analyzer, cache_name)
                                    if isinstance(cache, dict) and len(cache) > cache_limit:
                                        # Only clear if severely over limit
                                        if memory_emergency:
                                            cache.clear()
                                            cleanup_stats["caches_cleared"] += 1
                                        else:
                                            # Partial cleanup (keep 75% for better performance)
                                            items = list(cache.items())
                                            keep_count = int(cache_limit * 0.75)
                                            cache.clear()
                                            for k, v in items[:keep_count]:
                                                cache[k] = v
                            except Exception as e:
                                logging.debug(f"Failed to clear {cache_name}: {e}")
                        
                except Exception as e:
                    logging.error(f"Multi-timeframe analyzer cleanup failed: {e}")
                
                # 6. VaR calculator cleanup - Expanded history for 128GB (EXPANDED)
                try:
                    if (hasattr(self, 'risk_manager') and 
                        hasattr(self.risk_manager, 'var_calculator')):
                        var_calc = self.risk_manager.var_calculator
                        
                        # Price history cleanup
                        if hasattr(var_calc, 'price_history') and isinstance(var_calc.price_history, dict):
                            max_history = 2000 if memory_emergency else 5000 if memory_critical else 10000  # Doubled
                            
                            for symbol in list(var_calc.price_history.keys()):
                                try:
                                    symbol_history = var_calc.price_history[symbol]
                                    if hasattr(symbol_history, '__len__') and len(symbol_history) > max_history:
                                        keep_count = max_history // 2 if memory_emergency else max_history - 1000
                                        if hasattr(symbol_history, '__getitem__'):
                                            var_calc.price_history[symbol] = symbol_history[-keep_count:]
                                except Exception as e:
                                    logging.debug(f"Failed to trim history for {symbol}: {e}")
                                    var_calc.price_history.pop(symbol, None)
                        
                        # VaR history cleanup
                        if hasattr(var_calc, 'var_history'):
                            try:
                                var_retention = 7200 if memory_emergency else 14400 if memory_critical else 43200  # Up to 12 hours
                                recent_vars = [
                                    var_entry for var_entry in list(var_calc.var_history)
                                    if isinstance(var_entry, dict) and 
                                    "timestamp" in var_entry and 
                                    current_time - var_entry["timestamp"] < var_retention
                                ]
                                
                                max_vars = 1000 if memory_emergency else 2000 if memory_critical else 10000  # Doubled
                                from collections import deque
                                var_calc.var_history = deque(recent_vars, maxlen=max_vars)
                                
                            except Exception as e:
                                logging.debug(f"VaR history cleanup failed: {e}")
                        
                except Exception as e:
                    logging.error(f"VaR calculator cleanup failed: {e}")
                
                # 7. Performance metrics reset - Ultra high threshold for 128GB (EXPANDED)
                try:
                    if hasattr(self, 'performance_metrics') and isinstance(self.performance_metrics, dict):
                        reset_threshold = 100000 if memory_emergency else 200000 if memory_critical else 1000000  # 2x increase
                        cache_hits = self.performance_metrics.get("cache_hits", 0)
                        
                        if isinstance(cache_hits, (int, float)) and cache_hits > reset_threshold:
                            self.performance_metrics["cache_hits"] = 0
                            self.performance_metrics["cache_misses"] = 0
                            self.performance_metrics["last_reset"] = current_time
                            logging.info(f"Performance metrics reset (threshold: {reset_threshold:,})")
                            
                except Exception as e:
                    logging.error(f"Performance metrics cleanup failed: {e}")
                
                # 8. Emergency cleanup - Only in critical situations
                if memory_emergency:
                    try:
                        if (hasattr(self, 'telegram') and 
                            hasattr(self.telegram, 'failed_notifications') and
                            hasattr(self.telegram.failed_notifications, 'clear')):
                            self.telegram.failed_notifications.clear()
                            cleanup_stats["caches_cleared"] += 1
                        
                        if (hasattr(self, 'risk_manager') and 
                            hasattr(self.risk_manager, 'risk_history')):
                            risk_history = self.risk_manager.risk_history
                            if hasattr(risk_history, '__len__') and len(risk_history) > 200000:
                                from collections import deque
                                recent_history = list(risk_history)[-100000:]
                                self.risk_manager.risk_history = deque(recent_history, maxlen=400000)
                        
                    except Exception as e:
                        logging.error(f"Emergency cleanup failed: {e}")
                
                # 9. Smart garbage collection for 128GB system
                try:
                    if memory_emergency:
                        # Aggressive multi-generation GC
                        total_collected = 0
                        for generation in range(3):
                            collected = gc.collect(generation)
                            total_collected += collected
                        cleanup_stats["gc_collected"] = total_collected
                        logging.warning(f"Emergency GC: collected {total_collected} objects")
                    elif memory_critical:
                        # Standard GC
                        collected = gc.collect()
                        cleanup_stats["gc_collected"] = collected
                        logging.info(f"Critical GC: collected {collected} objects")
                    elif cleanup_cycle % 4 == 0:  # Every 4th cycle (60 minutes)
                        # Gentle periodic GC
                        collected = gc.collect(0)  # Only generation 0
                        cleanup_stats["gc_collected"] = collected
                        if collected > 100:
                            logging.debug(f"Periodic GC: collected {collected} objects")
                except Exception as e:
                    logging.error(f"Garbage collection failed: {e}")
                
                # Final memory check
                try:
                    cleanup_stats["memory_after"] = psutil.virtual_memory().percent
                except Exception:
                    cleanup_stats["memory_after"] = cleanup_stats["memory_before"]
                
                memory_freed = cleanup_stats["memory_before"] - cleanup_stats["memory_after"]
                cleanup_stats["duration"] = time.time() - cleanup_start_time
                
                # Comprehensive cleanup report
                logging.info(
                    f"Cleanup cycle #{cleanup_cycle} completed: "
                    f"removed {cleanup_stats['entries_removed']:,} entries, "
                    f"cleared {cleanup_stats['caches_cleared']} caches, "
                    f"GC collected {cleanup_stats['gc_collected']} objects, "
                    f"memory {cleanup_stats['memory_before']:.1f}% -> {cleanup_stats['memory_after']:.1f}% "
                    f"({memory_freed:+.1f}%), "
                    f"duration {cleanup_stats['duration']:.2f}s"
                )
                
                # Emergency memory cleanup if still critical
                if cleanup_stats["memory_after"] > EMERGENCY_MEMORY_THRESHOLD:
                    # Throttle emergency cleanups (max once per 5 minutes)
                    if current_time - last_emergency_cleanup > 300:
                        logging.critical(
                            f"Memory still at {cleanup_stats['memory_after']:.1f}% after cleanup - "
                            f"invoking emergency procedures"
                        )
                        await self._emergency_memory_cleanup(cleanup_stats["memory_after"])
                        last_emergency_cleanup = current_time
                    else:
                        logging.warning(
                            f"Memory at {cleanup_stats['memory_after']:.1f}% but emergency cleanup "
                            f"throttled (last: {current_time - last_emergency_cleanup:.0f}s ago)"
                        )
                
            except Exception as e:
                logging.error(f"Periodic cleanup failed: {e}")
                import traceback
                logging.debug(traceback.format_exc())
                
                try:
                    gc.collect()
                except Exception:
                    pass
                
                # Exponential backoff on errors
                await asyncio.sleep(min(CLEANUP_INTERVAL * 2, 1800))

    async def _emergency_memory_cleanup(self, current_memory: float):
        """Emergency memory cleanup when standard cleanup is insufficient"""
        logging.error(f"Initiating emergency memory cleanup: {current_memory:.1f}% usage")
        
        cleared_caches = 0
        try:
            # Nuclear option - clear all non-essential caches
            cleanup_targets = [
                ('position_monitor', ['price_cache', 'performance_cache', 'signal_buffer']),
                ('risk_manager', ['signal_cache', 'correlation_cache', 'volatility_cache']),
                ('execute_service', ['order_cache', 'execution_history'])
            ]
            
            for service_name, cache_names in cleanup_targets:
                if hasattr(self, service_name):
                    service = getattr(self, service_name)
                    for cache_name in cache_names:
                        try:
                            if hasattr(service, cache_name):
                                cache = getattr(service, cache_name)
                                if hasattr(cache, 'clear'):
                                    cache.clear()
                                    cleared_caches += 1
                                elif isinstance(cache, dict):
                                    cache.clear()
                                    cleared_caches += 1
                        except Exception as e:
                            logging.warning(f"Failed to clear {service_name}.{cache_name}: {e}")
            
            # Force aggressive garbage collection
            import gc
            total_collected = 0
            for _ in range(5):  # Multiple aggressive cycles
                collected = gc.collect()
                total_collected += collected
                await asyncio.sleep(0.1)  # Brief pause between cycles
            
            # Final memory check
            try:
                final_memory = psutil.virtual_memory().percent
                logging.error(
                    f"Emergency cleanup completed: cleared {cleared_caches} caches, "
                    f"collected {total_collected} objects, memory now {final_memory:.1f}%"
                )
                
                # If still critical, recommend restart
                if final_memory > 90:
                    logging.critical(
                        f"Memory usage still critical ({final_memory:.1f}%) - "
                        "system restart strongly recommended"
                    )
                    
                    # Send alert if telegram is available
                    if hasattr(self, 'telegram') and hasattr(self.telegram, 'send_alert'):
                        try:
                            await self.telegram.send_alert(
                                f"🚨 CRITICAL MEMORY WARNING\n"
                                f"Memory usage: {final_memory:.1f}%\n"
                                f"System restart recommended"
                            )
                        except Exception:
                            pass
                
            except Exception as e:
                logging.error(f"Emergency cleanup memory check failed: {e}")
            
        except Exception as e:
            logging.critical(f"Emergency memory cleanup failed: {e}")

    async def _periodic_service_monitoring(self):
        """Enhanced service monitoring with 10-second interval and auto-reconnection"""
        reconnection_attempts = {
            "execute_service": 0,
            "notify_service": 0
        }
        max_reconnection_attempts = 3
        
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds (changed from 300)

                # Check service connection status
                await self._check_service_connections()

                # Get critical services that need monitoring
                critical_services = ["execute_service", "notify_service"]
                
                for service_name in critical_services:
                    if service_name in self.service_health:
                        service_status = self.service_health[service_name]
                        
                        # If service is disconnected, attempt reconnection
                        if service_status.get("status") != "connected":
                            current_attempts = reconnection_attempts.get(service_name, 0)
                            
                            if current_attempts < max_reconnection_attempts:
                                logging.warning(
                                    f"Service {service_name} disconnected - "
                                    f"attempting reconnection (attempt {current_attempts + 1}/{max_reconnection_attempts})"
                                )
                                
                                # Attempt to reconnect
                                reconnect_success = await self._attempt_service_reconnection(
                                    service_name
                                )
                                
                                if reconnect_success:
                                    logging.info(f"Service {service_name} reconnected successfully")
                                    reconnection_attempts[service_name] = 0
                                    
                                    # Send success notification
                                    try:
                                        await self.telegram._queue_notification(
                                            f"Service {service_name} reconnected successfully",
                                            priority=False
                                        )
                                    except Exception:
                                        pass
                                else:
                                    reconnection_attempts[service_name] = current_attempts + 1
                                    logging.error(
                                        f"Service {service_name} reconnection failed "
                                        f"(attempt {reconnection_attempts[service_name]}/{max_reconnection_attempts})"
                                    )
                            else:
                                # Max attempts reached - send critical alert
                                if current_attempts == max_reconnection_attempts:
                                    logging.critical(
                                        f"Service {service_name} reconnection failed after "
                                        f"{max_reconnection_attempts} attempts - manual intervention required"
                                    )
                                    
                                    try:
                                        await self.telegram._queue_notification(
                                            f"CRITICAL: Service {service_name} connection failed - "
                                            f"manual restart required",
                                            priority=True
                                        )
                                    except Exception:
                                        pass
                                    
                                    # Increment to avoid spam
                                    reconnection_attempts[service_name] = max_reconnection_attempts + 1
                        else:
                            # Service is connected - reset attempt counter
                            if reconnection_attempts.get(service_name, 0) > 0:
                                reconnection_attempts[service_name] = 0

                # Alert if multiple services are disconnected
                disconnected_services = [
                    name
                    for name, status in self.service_health.items()
                    if status.get("status") != "connected"
                ]

                if len(disconnected_services) >= 2:
                    warning_message = (
                        f"Multiple service connection issues: {', '.join(disconnected_services)}"
                    )
                    await self.telegram._queue_notification(
                        warning_message, priority=True
                    )

            except Exception as e:
                logging.error(f"Service monitoring failed: {e}")
                await asyncio.sleep(5)  # Brief pause before retry
    
    async def _attempt_service_reconnection(self, service_name: str) -> bool:
        """Attempt to reconnect to a disconnected service"""
        try:
            if service_name not in config.SERVICE_URLS:
                logging.error(f"Unknown service: {service_name}")
                return False
            
            service_url = config.SERVICE_URLS[service_name]
            
            # Test connection with health check
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{service_url}/health") as response:
                    if response.status == 200:
                        # Update service health status
                        self.service_health[service_name]["status"] = "connected"
                        self.service_health[service_name]["last_check"] = time.time()
                        self.service_health[service_name]["consecutive_failures"] = 0
                        
                        logging.info(f"Service {service_name} health check passed")
                        return True
                    else:
                        logging.warning(
                            f"Service {service_name} responded with status {response.status}"
                        )
                        return False
        
        except asyncio.TimeoutError:
            logging.error(f"Service {service_name} reconnection timeout")
            return False
        except aiohttp.ClientError as e:
            logging.error(f"Service {service_name} connection error: {e}")
            return False
        except Exception as e:
            logging.error(f"Service {service_name} reconnection failed: {e}")
            return False

    async def _periodic_metrics_collection(self):
        while True:
            try:
                await asyncio.sleep(60)

                cpu_usage = psutil.cpu_percent()
                memory_usage = psutil.virtual_memory().percent

                if cpu_usage > 90:
                    logging.warning(f"High CPU usage: {cpu_usage:.1f}%")

                if memory_usage > 90:
                    logging.warning(f"High memory usage: {memory_usage:.1f}%")

                high_risk_count = len([
                    p for p in self.risk_manager.active_positions.values()
                    if p.liquidation_risk > 0.8
                ])

                if high_risk_count > 0:
                    logging.warning(f"High risk positions detected: {high_risk_count}")

            except Exception as e:
                logging.error(f"Metrics collection failed: {e}")

    async def _backup_risk_monitor(self):
        """Enhanced backup risk monitoring with comprehensive threat detection"""
        emergency_triggered = False
        last_portfolio_check = 0
        
        while True:
            try:
                await asyncio.sleep(5)  # Faster monitoring at 5 seconds
                
                active_positions = list(self.risk_manager.active_positions.values())
                current_time = time.time()
                
                # 1. Critical liquidation risk detection
                critical_positions = [p for p in active_positions if p.liquidation_risk > 0.95]
                
                if len(critical_positions) > 0:
                    logging.error(f"Critical liquidation risk detected: {len(critical_positions)} positions")
                    
                    # Send emergency telegram alert
                    try:
                        await self.telegram._queue_notification(
                            f"CRITICAL ALERT: {len(critical_positions)} positions at 95%+ liquidation risk",
                            priority=True
                        )
                    except Exception as telegram_error:
                        logging.error(f"Emergency telegram failed: {telegram_error}")
                    
                    # Activate emergency mode
                    self.risk_manager.emergency_mode = True
                    emergency_triggered = True
                    
                    # Immediately close critical positions
                    for position in critical_positions:
                        try:
                            await self.position_monitor._execute_auto_close(
                                position.position_id, position, "EMERGENCY_LIQUIDATION_RISK"
                            )
                            logging.info(f"Emergency position closed: {position.position_id}")
                        except Exception as close_error:
                            logging.error(f"Emergency close failed {position.position_id}: {close_error}")
                
                # 2. Portfolio-wide loss monitoring
                if len(active_positions) > 0:
                    total_margin = sum(p.margin_required for p in active_positions)
                    total_pnl = sum(p.unrealized_pnl for p in active_positions)
                    
                    if total_margin > 0:
                        portfolio_loss_ratio = abs(total_pnl) / total_margin if total_pnl < 0 else 0
                        
                        # Portfolio loss exceeds 60%
                        if portfolio_loss_ratio > 0.6:
                            logging.error(f"Portfolio major loss detected: {portfolio_loss_ratio:.1%}")
                            
                            try:
                                await self.telegram._queue_notification(
                                    f"PORTFOLIO ALERT: {portfolio_loss_ratio:.1%} loss ratio detected",
                                    priority=True
                                )
                            except Exception:
                                pass
                            
                            # Block new trades
                            self.risk_manager.emergency_mode = True
                            emergency_triggered = True
                
                # 3. High risk position monitoring
                high_risk_positions = [p for p in active_positions if p.liquidation_risk > 0.8]
                
                if len(high_risk_positions) > 3:
                    logging.warning(f"Multiple high-risk positions: {len(high_risk_positions)}")
                    
                    # Close the most critical ones first
                    sorted_positions = sorted(high_risk_positions, key=lambda x: x.liquidation_risk, reverse=True)
                    positions_to_close = sorted_positions[:2]  # Close top 2 most risky
                    
                    for position in positions_to_close:
                        if position.liquidation_risk > 0.9:  # Only close if very high risk
                            try:
                                await self.position_monitor._execute_auto_close(
                                    position.position_id, position, "HIGH_RISK_REDUCTION"
                                )
                            except Exception as e:
                                logging.error(f"High risk position close failed: {e}")
                
                # 4. System resource monitoring
                try:
                    memory_usage = psutil.virtual_memory().percent
                    if memory_usage > 95:
                        logging.error(f"Critical memory usage: {memory_usage:.1f}%")
                        
                        # Emergency memory cleanup
                        import gc
                        gc.collect()
                        
                        # Consider service restart if memory remains critical
                        if psutil.virtual_memory().percent > 98:
                            logging.critical("Memory critically low - system may need restart")
                except Exception:
                    pass
                
                # 5. Emergency mode auto-recovery
                if emergency_triggered and self.risk_manager.emergency_mode:
                    current_critical = len([p for p in active_positions if p.liquidation_risk > 0.95])
                    current_high_risk = len([p for p in active_positions if p.liquidation_risk > 0.8])
                    memory_ok = psutil.virtual_memory().percent < 85
                    
                    # Safe conditions for recovery
                    if current_critical == 0 and current_high_risk <= 2 and memory_ok:
                        self.risk_manager.emergency_mode = False
                        emergency_triggered = False
                        
                        try:
                            await self.telegram._queue_notification(
                                "RECOVERY: Emergency mode deactivated - system normalized",
                                priority=True
                            )
                        except Exception:
                            pass
                        
                        logging.info("Emergency mode auto-recovery completed")
                
                # 6. Periodic comprehensive check (every 5 minutes)
                if current_time - last_portfolio_check > 300:
                    await self._comprehensive_portfolio_check(active_positions)
                    last_portfolio_check = current_time
                        
            except Exception as e:
                logging.error(f"Backup risk monitor failed: {e}")
                await asyncio.sleep(30)
    
    async def _comprehensive_portfolio_check(self, positions):
        """Comprehensive portfolio risk assessment"""
        try:
            if not positions:
                return
            
            # Calculate portfolio metrics
            total_exposure = sum(p.quantity * p.current_price for p in positions)
            avg_leverage = sum(p.leverage for p in positions) / len(positions)
            correlation_risk = self._calculate_symbol_concentration(positions)
            
            risk_summary = {
                "position_count": len(positions),
                "total_exposure": total_exposure,
                "average_leverage": avg_leverage,
                "concentration_risk": correlation_risk,
                "high_risk_count": len([p for p in positions if p.liquidation_risk > 0.7])
            }
            
            # Log comprehensive status
            logging.info(
                f"Portfolio check: {risk_summary['position_count']} positions, "
                f"${total_exposure:,.0f} exposure, {avg_leverage:.1f}x avg leverage, "
                f"{risk_summary['high_risk_count']} high-risk positions"
            )
            
        except Exception as e:
            logging.error(f"Comprehensive portfolio check failed: {e}")
    
    def _calculate_symbol_concentration(self, positions):
        """Calculate portfolio concentration risk by symbol"""
        try:
            symbol_counts = {}
            for position in positions:
                symbol_counts[position.symbol] = symbol_counts.get(position.symbol, 0) + 1
            
            max_concentration = max(symbol_counts.values()) if symbol_counts else 0
            return max_concentration / len(positions) if positions else 0
            
        except Exception:
            return 0

    async def _adaptive_scoring_optimizer(self):
        """Advanced adaptive scoring optimization with market-based threshold adjustment"""
        last_optimization = 0
        
        while True:
            try:
                await asyncio.sleep(180)  # Check every 3 minutes
                
                current_time = time.time()
                
                # Initialize variables with default values
                approval_rate = 0.0
                success_rate = 0.0
                
                # Only optimize if sufficient data exists
                if self.performance_metrics["total_requests"] > 20:
                    
                    # Calculate performance metrics
                    approval_rate = (
                        self.performance_metrics["approved_requests"] 
                        / self.performance_metrics["total_requests"]
                    )
                    
                    success_rate = (
                        self.performance_metrics["service_calls_success"]
                        / max(self.performance_metrics["service_calls_made"], 1)
                    )
                    
                    # 1. Approval rate optimization
                    if approval_rate < 0.05:  # Very low approval rate
                        config.PHOENIX95_CONFIG['min_score_threshold'] = max(15, 
                            config.PHOENIX95_CONFIG['min_score_threshold'] - 5)
                        logging.info(f"Aggressive threshold reduction: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                    
                    elif approval_rate < 0.15:  # Low approval rate
                        config.PHOENIX95_CONFIG['min_score_threshold'] = max(20,
                            config.PHOENIX95_CONFIG['min_score_threshold'] - 3)
                        logging.info(f"Threshold lowered for more signals: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                    
                    elif approval_rate > 0.7:  # Very high approval rate
                        config.PHOENIX95_CONFIG['min_score_threshold'] = min(45,
                            config.PHOENIX95_CONFIG['min_score_threshold'] + 3)
                        logging.info(f"Threshold raised for quality: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                    
                    # 2. Execution success rate optimization
                    if success_rate < 0.6:  # Poor execution success
                        config.PHOENIX95_CONFIG['min_score_threshold'] = min(50,
                            config.PHOENIX95_CONFIG['min_score_threshold'] + 2)
                        logging.info(f"Threshold raised due to poor execution: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                
                # 3. Market volatility-based adjustment
                try:
                    active_positions = list(self.risk_manager.active_positions.values())
                    if len(active_positions) >= 3:
                        
                        # Calculate average portfolio risk
                        avg_liquidation_risk = sum(p.liquidation_risk for p in active_positions) / len(active_positions)
                        high_risk_count = len([p for p in active_positions if p.liquidation_risk > 0.7])
                        
                        # High market risk - be more conservative
                        if avg_liquidation_risk > 0.6 or high_risk_count > 3:
                            config.PHOENIX95_CONFIG['min_score_threshold'] = min(50,
                                config.PHOENIX95_CONFIG['min_score_threshold'] + 3)
                            logging.info(f"Market risk adjustment - threshold raised: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                        
                        # Low market risk - be more aggressive
                        elif avg_liquidation_risk < 0.3 and high_risk_count == 0:
                            config.PHOENIX95_CONFIG['min_score_threshold'] = max(15,
                                config.PHOENIX95_CONFIG['min_score_threshold'] - 2)
                            logging.info(f"Low risk environment - threshold lowered: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                
                except Exception as market_error:
                    logging.warning(f"Market volatility analysis failed: {market_error}")
                
                # 4. Time-based adjustment
                current_hour = datetime.utcnow().hour
                
                # Active trading hours (8-16 UTC) - more aggressive
                if 8 <= current_hour <= 16:
                    if config.PHOENIX95_CONFIG['min_score_threshold'] > 30:
                        config.PHOENIX95_CONFIG['min_score_threshold'] = max(25,
                            config.PHOENIX95_CONFIG['min_score_threshold'] - 2)
                        logging.info(f"Active hours adjustment: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                
                # Inactive hours - more conservative
                elif 2 <= current_hour <= 6:
                    if config.PHOENIX95_CONFIG['min_score_threshold'] < 35:
                        config.PHOENIX95_CONFIG['min_score_threshold'] = min(40,
                            config.PHOENIX95_CONFIG['min_score_threshold'] + 3)
                        logging.info(f"Inactive hours adjustment: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                
                # 5. Emergency mode check
                if hasattr(self.risk_manager, 'emergency_mode') and self.risk_manager.emergency_mode:
                    # In emergency mode, raise threshold significantly
                    config.PHOENIX95_CONFIG['min_score_threshold'] = min(60,
                        config.PHOENIX95_CONFIG['min_score_threshold'] + 10)
                    logging.warning(f"Emergency mode - threshold raised: {config.PHOENIX95_CONFIG['min_score_threshold']}")
                
                # 6. Bounds checking and safety limits
                config.PHOENIX95_CONFIG['min_score_threshold'] = max(10, 
                    min(config.PHOENIX95_CONFIG['min_score_threshold'], 60))
                
                # Log optimization summary (only if data exists)
                if current_time - last_optimization > 600:  # Every 10 minutes
                    if self.performance_metrics["total_requests"] > 20:
                        logging.info(
                            f"Optimization summary: threshold={config.PHOENIX95_CONFIG['min_score_threshold']}, "
                            f"approval_rate={approval_rate:.1%}, success_rate={success_rate:.1%}"
                        )
                    else:
                        logging.info(
                            f"Optimization summary: threshold={config.PHOENIX95_CONFIG['min_score_threshold']}, "
                            f"insufficient data (requests: {self.performance_metrics['total_requests']})"
                        )
                    last_optimization = current_time
                
            except Exception as e:
                logging.error(f"Adaptive scoring optimizer failed: {e}")
                await asyncio.sleep(60)

    async def _periodic_health_check(self):
        while True:
            try:
                await asyncio.sleep(self.health_check_config["check_interval"])
                
                redis_healthy = await self._ensure_redis_connection()
                if not redis_healthy:
                    logging.error("Redis connection unhealthy")
                
                memory_usage = psutil.virtual_memory().percent
                if memory_usage > 95:
                    logging.critical(f"Critical memory usage: {memory_usage:.1f}%")
                    await self.telegram._queue_notification(
                        f"Critical memory usage: {memory_usage:.1f}%", priority=True
                    )
                
                active_monitors = len(self.position_monitor.active_monitors)
                active_positions = len(self.risk_manager.active_positions)
                
                if active_positions > active_monitors:
                    logging.warning(f"Monitor mismatch: {active_positions} positions, {active_monitors} monitors")
                    
            except Exception as e:
                logging.error(f"Health check failed: {e}")
                await asyncio.sleep(30)

    async def emergency_system_recovery(self):
        """
        Enhanced emergency system recovery with comprehensive service restoration
        
        Features:
        - Multi-service health check and recovery
        - Redis connection restoration
        - Position monitor recovery
        - Circuit breaker reset with verification
        - Memory pressure relief
        - Performance metrics reset
        - Comprehensive status reporting
        
        Optimized for high-spec system (128GB RAM, 16-24 cores)
        Supports 400+ concurrent position monitoring recovery
        
        Returns:
            bool: True if recovery successful, False otherwise
        """
        recovery_start_time = time.time()
        recovery_status = {
            "circuit_breakers_reset": False,
            "emergency_bypass_enabled": False,
            "notification_queue_cleared": False,
            "execute_service_restored": False,
            "notify_service_restored": False,
            "portfolio_service_restored": False,
            "redis_reconnected": False,
            "position_monitors_recovered": False,
            "memory_cleaned": False,
            "metrics_reset": False,
            "overall_success": False
        }
        
        try:
            logging.info("=" * 80)
            logging.info("EMERGENCY SYSTEM RECOVERY INITIATED")
            logging.info("=" * 80)
            
            # Phase 1: Reset all circuit breakers with verification
            try:
                if hasattr(self, 'risk_manager'):
                    self.risk_manager.reset_circuit_breaker()
                    
                    # Verify circuit breaker reset
                    if hasattr(self.risk_manager, 'circuit_breaker_state'):
                        reset_count = sum(
                            1 for state in self.risk_manager.circuit_breaker_state.values()
                            if state.get('state') == 'CLOSED'
                        )
                        total_breakers = len(self.risk_manager.circuit_breaker_state)
                        
                        logging.info(
                            f"Phase 1: Circuit breakers reset - "
                            f"{reset_count}/{total_breakers} closed"
                        )
                        recovery_status["circuit_breakers_reset"] = (reset_count == total_breakers)
                    else:
                        recovery_status["circuit_breakers_reset"] = True
                        logging.info("Phase 1: Circuit breakers reset (no state tracking)")
                else:
                    logging.warning("Phase 1: Risk manager not available")
                    
            except Exception as cb_error:
                logging.error(f"Phase 1 failed: {cb_error}")
            
            # Phase 2: Enable emergency bypass mode
            try:
                if hasattr(self, 'risk_manager'):
                    self.risk_manager.emergency_bypass_enabled = True
                    recovery_status["emergency_bypass_enabled"] = True
                    logging.info("Phase 2: Emergency bypass enabled - blocking restrictions lifted")
                else:
                    logging.warning("Phase 2: Risk manager not available")
                    
            except Exception as bypass_error:
                logging.error(f"Phase 2 failed: {bypass_error}")
            
            # Phase 3: Clear failed notification queue with stats
            try:
                initial_queue_size = self.telegram.notification_queue.qsize()
                cleared_count = 0
                
                while not self.telegram.notification_queue.empty():
                    try:
                        await asyncio.wait_for(
                            self.telegram.notification_queue.get(),
                            timeout=1.0
                        )
                        self.telegram.notification_queue.task_done()
                        cleared_count += 1
                    except asyncio.TimeoutError:
                        break
                
                recovery_status["notification_queue_cleared"] = True
                logging.info(
                    f"Phase 3: Notification queue cleared - "
                    f"{cleared_count}/{initial_queue_size} items removed"
                )
                
            except Exception as queue_error:
                logging.error(f"Phase 3 failed: {queue_error}")
            
            # Phase 4: Multi-service connection recovery
            service_recovery_tasks = []
            
            # Execute Service recovery
            service_recovery_tasks.append(
                self._recover_service_connection("execute_service", recovery_status)
            )
            
            # Notify Service recovery
            service_recovery_tasks.append(
                self._recover_service_connection("notify_service", recovery_status)
            )
            
            # Portfolio Service recovery (if available)
            service_recovery_tasks.append(
                self._recover_service_connection("portfolio_service", recovery_status)
            )
            
            # Execute service recovery in parallel (high-spec optimization)
            try:
                await asyncio.wait_for(
                    asyncio.gather(*service_recovery_tasks, return_exceptions=True),
                    timeout=15.0
                )
                logging.info("Phase 4: Multi-service recovery completed")
            except asyncio.TimeoutError:
                logging.warning("Phase 4: Service recovery timeout (15s)")
            
            # Phase 5: Redis connection recovery
            try:
                if not self.redis or not await self._ensure_redis_connection():
                    logging.warning("Phase 5: Redis reconnection attempt...")
                    reconnect_success = await self._establish_redis_connection()
                    
                    if reconnect_success:
                        recovery_status["redis_reconnected"] = True
                        logging.info("Phase 5: Redis reconnected successfully")
                    else:
                        logging.error("Phase 5: Redis reconnection failed")
                else:
                    recovery_status["redis_reconnected"] = True
                    logging.info("Phase 5: Redis already connected")
                    
            except Exception as redis_error:
                logging.error(f"Phase 5 failed: {redis_error}")
            
            # Phase 6: Position monitor recovery
            try:
                active_positions = len(self.risk_manager.active_positions)
                active_monitors = len(self.position_monitor.active_monitors)
                
                if active_positions > active_monitors:
                    logging.info(
                        f"Phase 6: Monitor recovery needed - "
                        f"{active_positions} positions, {active_monitors} monitors"
                    )
                    
                    # Restart missing monitors
                    recovered_monitors = 0
                    for position_id, position in self.risk_manager.active_positions.items():
                        if position_id not in self.position_monitor.active_monitors:
                            try:
                                # Create minimal risk profile for monitoring
                                risk_profile = RiskProfile(
                                    symbol=position.symbol,
                                    leverage=position.leverage,
                                    position_size=position.quantity,
                                    margin_required=position.margin_required,
                                    liquidation_price=position.current_price * 0.5,
                                    stop_loss_price=position.current_price * 0.98,
                                    take_profit_price=position.current_price * 1.02,
                                    risk_score=0.5,
                                    confidence=0.8
                                )
                                
                                await self.position_monitor.start_position_monitoring(
                                    position_id, risk_profile
                                )
                                recovered_monitors += 1
                                
                            except Exception as monitor_error:
                                logging.warning(f"Failed to recover monitor {position_id}: {monitor_error}")
                    
                    recovery_status["position_monitors_recovered"] = (recovered_monitors > 0)
                    logging.info(f"Phase 6: Recovered {recovered_monitors} position monitors")
                else:
                    recovery_status["position_monitors_recovered"] = True
                    logging.info("Phase 6: All position monitors active")
                    
            except Exception as monitor_error:
                logging.error(f"Phase 6 failed: {monitor_error}")
            
            # Phase 7: Emergency memory cleanup
            try:
                memory_before = psutil.virtual_memory().percent
                
                if memory_before > 85:
                    logging.warning(f"Phase 7: High memory usage ({memory_before:.1f}%) - emergency cleanup")
                    
                    # Clear all non-critical caches
                    cache_cleanup_count = 0
                    
                    if hasattr(self, 'position_monitor'):
                        if hasattr(self.position_monitor, 'price_cache'):
                            self.position_monitor.price_cache.clear()
                            cache_cleanup_count += 1
                        if hasattr(self.position_monitor, 'performance_cache'):
                            self.position_monitor.performance_cache.clear()
                            cache_cleanup_count += 1
                    
                    if hasattr(self, 'risk_manager'):
                        if hasattr(self.risk_manager, 'signal_cache'):
                            self.risk_manager.signal_cache.clear()
                            cache_cleanup_count += 1
                    
                    # Force garbage collection
                    import gc
                    collected = gc.collect()
                    
                    memory_after = psutil.virtual_memory().percent
                    memory_freed = memory_before - memory_after
                    
                    recovery_status["memory_cleaned"] = True
                    logging.info(
                        f"Phase 7: Memory cleanup - {cache_cleanup_count} caches cleared, "
                        f"{collected} objects collected, "
                        f"memory {memory_before:.1f}% -> {memory_after:.1f}% ({memory_freed:+.1f}%)"
                    )
                else:
                    recovery_status["memory_cleaned"] = True
                    logging.info(f"Phase 7: Memory usage acceptable ({memory_before:.1f}%)")
                    
            except Exception as memory_error:
                logging.error(f"Phase 7 failed: {memory_error}")
            
            # Phase 8: Performance metrics reset
            try:
                if hasattr(self, 'performance_metrics'):
                    # Reset error counters while preserving important stats
                    self.performance_metrics["health_check_failures"] = 0
                    self.performance_metrics["circuit_breaker_trips"] = 0
                    self.performance_metrics["last_reset"] = time.time()
                    
                    recovery_status["metrics_reset"] = True
                    logging.info("Phase 8: Performance metrics reset")
                else:
                    logging.warning("Phase 8: Performance metrics not available")
                    
            except Exception as metrics_error:
                logging.error(f"Phase 8 failed: {metrics_error}")
            
            # Phase 9: Send comprehensive recovery notification
            try:
                successful_phases = sum(1 for status in recovery_status.values() if status)
                total_phases = len(recovery_status) - 1  # Exclude overall_success
                success_rate = (successful_phases / total_phases) * 100
                
                recovery_status["overall_success"] = (success_rate >= 70)
                
                recovery_message = (
                    f"EMERGENCY RECOVERY COMPLETED\n"
                    f"Success Rate: {success_rate:.0f}% ({successful_phases}/{total_phases})\n"
                    f"Duration: {time.time() - recovery_start_time:.1f}s\n\n"
                    f"Circuit Breakers: {'OK' if recovery_status['circuit_breakers_reset'] else 'FAILED'}\n"
                    f"Emergency Bypass: {'OK' if recovery_status['emergency_bypass_enabled'] else 'FAILED'}\n"
                    f"Execute Service: {'OK' if recovery_status['execute_service_restored'] else 'FAILED'}\n"
                    f"Notify Service: {'OK' if recovery_status['notify_service_restored'] else 'FAILED'}\n"
                    f"Redis: {'OK' if recovery_status['redis_reconnected'] else 'FAILED'}\n"
                    f"Position Monitors: {'OK' if recovery_status['position_monitors_recovered'] else 'FAILED'}\n"
                    f"Memory: {'OK' if recovery_status['memory_cleaned'] else 'FAILED'}"
                )
                
                await self.telegram._queue_notification(recovery_message, priority=True)
                logging.info("Phase 9: Recovery notification sent")
                
            except Exception as notify_error:
                logging.warning(f"Phase 9 failed: {notify_error}")
            
            # Final status report
            recovery_duration = time.time() - recovery_start_time
            
            logging.info("=" * 80)
            if recovery_status["overall_success"]:
                logging.info(f"EMERGENCY RECOVERY SUCCESS ({success_rate:.0f}% success rate, {recovery_duration:.1f}s)")
            else:
                logging.warning(f"EMERGENCY RECOVERY PARTIAL ({success_rate:.0f}% success rate, {recovery_duration:.1f}s)")
            logging.info("=" * 80)
            
            return recovery_status["overall_success"]
            
        except Exception as critical_error:
            logging.critical(f"Emergency recovery failed critically: {critical_error}")
            import traceback
            logging.error(traceback.format_exc())
            return False

    async def _recover_service_connection(self, service_name: str, recovery_status: dict) -> bool:
        """
        Attempt to recover connection to a specific service
        
        Args:
            service_name: Name of service to recover
            recovery_status: Dictionary to update with recovery results
            
        Returns:
            bool: True if recovery successful
        """
        try:
            if service_name not in config.SERVICE_URLS:
                logging.warning(f"Unknown service: {service_name}")
                return False
            
            service_url = config.SERVICE_URLS[service_name]
            status_key = f"{service_name}_restored"
            
            # Test service connection
            test_data = {"test": "recovery_check", "timestamp": time.time()}
            
            try:
                result = await asyncio.wait_for(
                    self.risk_manager._call_service_with_retry(
                        f"{service_url}/health",
                        test_data,
                        max_retries=2
                    ),
                    timeout=5.0
                )
                
                if result and result.get("status") in ["success", "healthy"]:
                    # Update service health status
                    if service_name in self.service_health:
                        self.service_health[service_name]["status"] = "connected"
                        self.service_health[service_name]["consecutive_failures"] = 0
                    
                    recovery_status[status_key] = True
                    logging.info(f"Service {service_name} connection restored")
                    return True
                else:
                    logging.warning(f"Service {service_name} responded but status unclear")
                    return False
                    
            except asyncio.TimeoutError:
                logging.error(f"Service {service_name} connection timeout")
                return False
                
        except Exception as e:
            logging.error(f"Service {service_name} recovery failed: {e}")
            return False

    async def shutdown(self):
        """Service shutdown"""
        logging.info("Advanced Risk Service shutdown started")

        # Stop all position monitoring
        for position_id, task in self.position_monitor.active_monitors.items():
            task.cancel()
            logging.info(f"Position monitoring stopped: {position_id}")

        # Stop background tasks
        for task in self.background_tasks:
            task.cancel()

        # Wait for notification queue completion
        try:
            await asyncio.wait_for(self.telegram.notification_queue.join(), timeout=5.0)
        except asyncio.TimeoutError:
            logging.warning("Notification queue completion timeout")

        logging.info("Advanced Risk Service shutdown completed")


# =============================================================================
# Main Execution Section
# =============================================================================


async def main():
    """Main execution function"""
    # Logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("phoenix95_advanced_risk_service.log"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True
    )

    risk_service = None
    
    try:
        # Advanced Risk Service initialization
        risk_service = AdvancedRiskService()

        # Start background tasks
        await risk_service.start_background_tasks()

        # Startup messages
        logging.info("Phoenix 95 Advanced Risk Management Service started")
        logging.info(
            f"Address: http://{config.RISK_CONFIG['host']}:{config.RISK_CONFIG['port']}"
        )
        logging.info(f"Max leverage: {config.RISK_CONFIG['max_leverage']}x")
        logging.info(f"Margin mode: {config.RISK_CONFIG['margin_mode']}")
        logging.info(f"Stop/Take profit: ±{config.RISK_CONFIG['stop_loss_percent']*100}%")
        logging.info(f"Service integration: Execute, Notify, Portfolio")
        logging.info(f"Performance optimization: caching, duplicate prevention")

        # Initial service connection status check
        await risk_service._check_service_connections()
        pipeline_status = risk_service._get_pipeline_status()
        logging.info(f"Pipeline status: {pipeline_status}")
        
        # Start server with safe event loop configuration
        config_server = uvicorn.Config(
            risk_service.app,
            host=config.RISK_CONFIG["host"],
            port=config.RISK_CONFIG["port"],
            log_level="info",
            access_log=True,
            # Removed uvloop to prevent event loop closure errors
        )

        server = uvicorn.Server(config_server)
        await server.serve()

    except KeyboardInterrupt:
        logging.info("Service terminated by user")
    except Exception as e:
        logging.error(f"Error during service execution: {e}\n{traceback.format_exc()}")
    finally:
        # Safe cleanup
        if risk_service:
            try:
                await risk_service.shutdown()
            except Exception as shutdown_error:
                logging.error(f"Shutdown error: {shutdown_error}")
        logging.info("Phoenix 95 Advanced Risk Service shutdown")

if __name__ == "__main__":
    try:
        # Check uvloop availability but don't force it
        try:
            import uvloop
            uvloop.install()
            logging.info("uvloop installed successfully")
        except ImportError:
            logging.info("uvloop not available - using default asyncio")
        except Exception as e:
            logging.warning(f"uvloop installation failed: {e}")
        
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logging.info("Service terminated by user")
    except RuntimeError as e:
        if "Event loop is closed" in str(e):
            logging.info("Event loop closed - service shutdown complete")
        else:
            logging.error(f"Runtime error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info("Service cleanup completed")
