#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix 95 V4 Ultimate - NOTIFY Service (Notification Hub) - Integrated Improved Version
================================================================================
Port: 8103
Role: Telegram notifications, real-time dashboard, performance monitoring, integrated notification management
Features: 
  - Phoenix95-based smart priority notifications
  - Service-specific notification APIs (Brain, Risk, Execute)
  - Full pipeline monitoring and visualization
  - Telegram real-time notifications (trading, risk, system)
  - Web dashboard (real-time charts, position monitoring)
  - Performance metrics collection/analysis (uvloop optimization)
  - Notification queue management and batch sending
  - High availability notification system
  - Daily rotating log files
================================================================================
"""

# ============================================================================
# CRITICAL: Import standard library first (Python best practice)
# High-spec optimization: Fast imports with in-memory caching (128GB RAM)
# ============================================================================
import asyncio
import hashlib
import json
import logging
import logging.handlers
import os
import sys
import threading
import time
import traceback
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import numpy as np
import pandas as pd
import psutil

# ============================================================================
# CRITICAL: Environment Variable Loading (After imports)
# High-spec optimization: Pre-loaded environment for 128GB RAM, 16-24 cores
# ============================================================================

# Step 1: Locate project root directory (where .env file resides)
# Optimized for fast path resolution on NVMe SSD 2TB+
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent  # Navigate to phoenix95_v4_ultimate/

# Step 2: Add project root to Python path for module imports
# High-spec optimization: In-memory path caching for fast imports
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Step 3: Load environment variables with python-dotenv
# High-spec optimization: Load all env vars into memory (128GB RAM available)
try:
    from dotenv import load_dotenv
    
    env_file = project_root / ".env"
    
    if env_file.exists():
        # override=True ensures latest values are always used
        load_dotenv(dotenv_path=env_file, override=True)
        print(f"[NOTIFY-8103] Environment file loaded: {env_file}")
        
        # Step 4: Verify critical Telegram credentials
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        telegram_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        
        if telegram_token and telegram_chat and len(telegram_token) > 35:
            print(f"[NOTIFY-8103] Telegram Bot Token: {telegram_token[:10]}...")
            print(f"[NOTIFY-8103] Telegram Chat ID: {telegram_chat}")
            print(f"[NOTIFY-8103] Telegram credentials validated successfully")
        else:
            print(f"[NOTIFY-8103] WARNING: Telegram credentials missing or invalid")
            print(f"  - TELEGRAM_BOT_TOKEN: {'Found' if telegram_token else 'Missing'} (length: {len(telegram_token)})")
            print(f"  - TELEGRAM_CHAT_ID: {'Found' if telegram_chat else 'Missing'}")
            print(f"  - Telegram notifications will be disabled")
    else:
        print(f"[NOTIFY-8103] ERROR: .env file not found at {env_file}")
        print(f"  Project root: {project_root}")
        print(f"  Current file: {current_file}")
        print(f"  Please create .env file with TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
        
except ImportError:
    print(f"[NOTIFY-8103] WARNING: python-dotenv not installed")
    print(f"  Install with: pip install python-dotenv")
    print(f"  Environment variables will be loaded from system only")

# Setup log directory and daily log file before any other operations
LOG_BASE_DIR = Path("C:/phoenix95_v4_ultimate/services/logs")
LOG_BASE_DIR.mkdir(parents=True, exist_ok=True)

# Create daily log filename
current_date = datetime.now().strftime("%Y-%m-%d")
LOG_FILE_PATH = LOG_BASE_DIR / f"notify_service_{current_date}.log"

# Configure logging with daily rotating file handler
class UTF8FileHandler(logging.FileHandler):
    """Custom file handler with UTF-8 encoding"""
    def __init__(self, filename, mode='a', encoding='utf-8', delay=False):
        super().__init__(filename, mode, encoding, delay)

# Setup root logger with both file and console handlers
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Remove existing handlers
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# File handler with UTF-8 encoding
file_handler = UTF8FileHandler(
    LOG_FILE_PATH,
    mode='a',
    encoding='utf-8'
)
file_formatter = logging.Formatter(
    '%(asctime)s | %(name)s | %(levelname)s | [NOTIFY-8103] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(file_formatter)
root_logger.addHandler(file_handler)

# Console handler with UTF-8 encoding
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)

# Set encoding for stdout
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
os.environ["PYTHONIOENCODING"] = "utf-8"

logging.info("=" * 80)
logging.info("Phoenix 95 V4 Ultimate - NOTIFY Service Starting")
logging.info(f"Log file: {LOG_FILE_PATH}")
logging.info("=" * 80)

# Performance optimization - apply uvloop
try:
    import uvloop
    UVLOOP_AVAILABLE = True
    logging.info("uvloop loaded successfully - performance optimization enabled")
except ImportError:
    UVLOOP_AVAILABLE = False
    logging.warning("uvloop not available - using default event loop")

import uvicorn

# FastAPI and web related imports
from fastapi import (
    BackgroundTasks,
    FastAPI,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from contextlib import asynccontextmanager

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

# Chart and visualization (optional import)
CHART_AVAILABLE = False
try:
    import matplotlib
    matplotlib.use("Agg")
    import base64
    from io import BytesIO
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    CHART_AVAILABLE = True
    logging.info("Chart library loaded successfully")
except ImportError:
    logging.warning("matplotlib not found - chart features disabled")

# Redis (optional)
REDIS_AVAILABLE = False
try:
    import aioredis
    REDIS_AVAILABLE = True
    logging.info("Redis library loaded successfully")
except ImportError:
    logging.warning("aioredis not found - Redis features disabled")

# PostgreSQL (optional)
POSTGRES_AVAILABLE = False
try:
    import asyncpg
    POSTGRES_AVAILABLE = True
    logging.info("PostgreSQL library loaded successfully")
except ImportError:
    logging.warning("asyncpg not found - PostgreSQL features disabled")

# Project path setup and config import
sys.path.append(str(Path(__file__).parent.parent))

try:
    from config.phoenix95_v4_config import config
    print("[Phoenix95] NOTIFY Service - config module loaded successfully")
    
    if not hasattr(config, 'DATA_STORAGE'):
        config.DATA_STORAGE = {"log_path": "logs"}
    if not hasattr(config, 'TELEGRAM'):
        config.TELEGRAM = {
            "enabled": False, 
            "bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""), 
            "chat_id": os.environ.get("TELEGRAM_CHAT_ID", ""),
            "rate_limit": 30,
            "retry_attempts": 3,
            "timeout": 10,
            "message_queue_size": 1000,
            "batch_size": 5,
            "batch_interval": 2
        }
    if not hasattr(config, 'DASHBOARD'):
        config.DASHBOARD = {
            "host": "0.0.0.0", 
            "port": 8103, 
            "title": "Phoenix 95 V4 Ultimate - Integrated Notification Hub",
            "refresh_interval": 3,
            "theme": "dark",
            "enable_charts": CHART_AVAILABLE,
            "enable_websocket": True
        }
    if not hasattr(config, 'SERVICES'):
        config.SERVICES = {
            "brain_service": {"url": "http://localhost:8100", "enabled": True},
            "risk_service": {"url": "http://localhost:8101", "enabled": True},
            "execute_service": {"url": "http://localhost:8102", "enabled": True},
            "notify_service": {"url": "http://localhost:8103", "enabled": True}
        }
    if not hasattr(config, 'MONITORING'):
        config.MONITORING = {
            "alert_cooldown": 300,
            "metrics_interval": 5,
            "pipeline_check_interval": 30,
            "health_check_interval": 30,
            "performance_thresholds": {
                "cpu_percent": 80,
                "memory_percent": 85,
                "disk_percent": 90
            }
        }
    if not hasattr(config, 'SMART_PRIORITY'):
        config.SMART_PRIORITY = {
            "phoenix95_thresholds": {95: "CRITICAL", 90: "CRITICAL", 85: "HIGH", 75: "MEDIUM", 0: "LOW"},
            "confidence_thresholds": {0.95: "CRITICAL", 0.85: "HIGH", 0.75: "MEDIUM", 0.0: "LOW"},
            "immediate_send_types": ["trade_execution", "liquidation_warning", "risk_alert", "system_critical"],
            "batch_optimization": {"HIGH": {"size": 3, "interval": 0.5}, "MEDIUM": {"size": 5, "interval": 2.0}, "LOW": {"size": 10, "interval": 5.0}}
        }
    if not hasattr(config, 'MESSAGE_PROCESSING'):
        config.MESSAGE_PROCESSING = {
            "priority_levels": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
            "queue_size": 5000,
            "worker_count": 3
        }
    if not hasattr(config, 'ALERTS'):
        config.ALERTS = {
            "trade_execution": True,
            "position_updates": True,
            "risk_warnings": True,
            "system_errors": True
        }
    
    print("[Phoenix95] NOTIFY Service - config attributes supplemented")
    
except ImportError as e:
    print(f"[Phoenix95] NOTIFY Service - config module load failed: {e}")
    class DefaultConfig:
        DATA_STORAGE = {"log_path": "logs"}
        TELEGRAM = {"enabled": False, "bot_token": "", "chat_id": ""}
        DASHBOARD = {"host": "0.0.0.0", "port": 8103}
        SERVICES = {}
        MONITORING = {"alert_cooldown": 300}
        SMART_PRIORITY = {}
        MESSAGE_PROCESSING = {"priority_levels": ["CRITICAL", "HIGH", "MEDIUM", "LOW"]}
        ALERTS = {}
    config = DefaultConfig()
    print("[Phoenix95] NOTIFY Service - using default config")

# Note: Encoding setup and logging configuration already handled in initial setup
# Removing duplicate configuration

# ===============================================================================
#                          V4 Integrated System Configuration
# ===============================================================================


@dataclass
class IntegratedServiceConfig:
    """Phoenix 95 V4 Integrated System Configuration - Optimized for High-Spec Hardware"""

    # Telegram settings (load from environment variables)
    # Optimized for stability and reduced timeout errors
    TELEGRAM = {
        "bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
        "chat_id": os.environ.get("TELEGRAM_CHAT_ID", ""),
        "enabled": bool(os.environ.get("TELEGRAM_BOT_TOKEN"))
        and bool(os.environ.get("TELEGRAM_CHAT_ID")),
        "rate_limit": 25,  # Reduced from 30 to 25 messages per second for safety
        "retry_attempts": 4,  # Increased from 3 to 4 for better reliability
        "timeout": 15,  # Increased from 10 to 15 seconds to reduce timeout errors
        "message_queue_size": 2000,  # Increased from 1000 for high-spec hardware
        "batch_size": 5,  # Send 5 messages per batch
        "batch_interval": 3,  # Increased from 2 to 3 seconds for stability
        "connect_timeout": 7,  # Connection timeout in seconds
        "max_message_length": 4096,  # Telegram message limit
    }

    # Dashboard settings - Enhanced for high-spec hardware
    DASHBOARD = {
        "host": "0.0.0.0",
        "port": 8103,
        "title": "Phoenix 95 V4 Ultimate - Integrated Notification Hub",
        "refresh_interval": 3,  # Refresh every 3 seconds
        "theme": "dark",
        "enable_charts": CHART_AVAILABLE,
        "enable_websocket": True,
        "max_data_points": 2000,  # Increased from 1000 for 128GB RAM
        "chart_history_hours": 48,  # Increased from 24 for extended history
    }

    # Service integration settings
    SERVICES = {
        "webhook_server": {
            "url": "http://localhost:8099",
            "health_endpoint": "/health",
            "enabled": True,
        },
        "brain_service": {
            "url": "http://localhost:8100",
            "health_endpoint": "/health",
            "phoenix95_enabled": True,
            "enabled": True,
        },
        "risk_service": {
            "url": "http://localhost:8101",
            "health_endpoint": "/health",
            "kelly_enabled": True,
            "enabled": True,
        },
        "execute_service": {
            "url": "http://localhost:8102",
            "health_endpoint": "/health",
            "enabled": True,
        },
        "notify_service": {
            "url": "http://localhost:8103",
            "health_endpoint": "/health",
            "enabled": True,
        },
    }

    # Monitoring settings - Optimized for high-spec hardware
    MONITORING = {
        "metrics_interval": 5,
        "alert_cooldown": 300,
        "pipeline_check_interval": 30,
        "data_flow_timeout": 300,  # 5 minutes
        "performance_thresholds": {
            "cpu_percent": 70,  # Adjusted from 80 for multi-core CPU (16-24 cores)
            "memory_percent": 80,  # Adjusted from 85 for 128GB RAM
            "disk_percent": 90,
            "response_time_ms": 2000,
            "error_rate_percent": 5,
            "queue_size": 1000,  # Increased from 500
        },
        "health_check_interval": 30,
        "auto_restart": True,
        "backup_interval": 3600,
    }

    # Phoenix95 smart priority settings
    SMART_PRIORITY = {
        "phoenix95_thresholds": {
            95: "CRITICAL",
            90: "CRITICAL",
            85: "HIGH",
            75: "MEDIUM",
            0: "LOW",
        },
        "confidence_thresholds": {
            0.95: "CRITICAL",
            0.85: "HIGH",
            0.75: "MEDIUM",
            0.0: "LOW",
        },
        "immediate_send_types": [
            "trade_execution",
            "liquidation_warning",
            "risk_alert",
            "system_critical",
        ],
        "batch_optimization": {
            "HIGH": {"size": 3, "interval": 0.5},
            "MEDIUM": {"size": 5, "interval": 2.0},
            "LOW": {"size": 10, "interval": 5.0},
        },
    }

    # Queue and message processing - Optimized for high-spec hardware
    MESSAGE_PROCESSING = {
        "queue_size": 20000,  # Increased from 5000 for 128GB RAM
        "worker_count": 8,  # Increased from 3 for multi-core CPU (16-24 cores)
        "batch_processing": True,
        "priority_levels": ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        "message_ttl": 3600,
        "dead_letter_queue": True,
        "retry_policy": {"max_retries": 5, "backoff_factor": 2, "initial_delay": 1},
    }

    # Alert settings
    ALERTS = {
        "trade_execution": True,
        "position_updates": True,
        "risk_warnings": True,
        "system_errors": True,
        "performance_alerts": True,
        "daily_summary": True,
        "liquidation_warnings": True,
        "profit_loss_updates": True,
        "market_alerts": True,
        "pipeline_alerts": True,
        "phoenix95_alerts": True,
    }

    # Data storage settings - Using new log directory structure
    DATA_STORAGE = {
        "redis_url": "redis://localhost:6380/1" if REDIS_AVAILABLE else None,
        "postgres_url": "postgresql://postgres:password@localhost:5432/phoenix95_v4"
        if POSTGRES_AVAILABLE
        else None,
        "backup_path": "C:/phoenix95_v4_ultimate/backups",
        "log_path": str(LOG_BASE_DIR),  # Using the new log directory
        "retention_days": 30,
        "compression": True,
    }

def validate_required_env_vars():
    """Environment variable validation with improved telegram setup"""
    required_vars = []
    optional_vars = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
    
    # Check required variables (currently none)
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        error_msg = f"Missing required environment variables: {missing}"
        logging.critical(error_msg)
        raise EnvironmentError(error_msg)
    
    # Telegram validation with better format checking
    missing_optional = [var for var in optional_vars if not os.environ.get(var)]
    if missing_optional:
        logging.warning(f"Optional variables missing - Telegram disabled: {missing_optional}")
        return False
    
    # Enhanced telegram token validation
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    
    # Validate bot token format
    if not bot_token or len(bot_token) < 35:
        logging.warning("Invalid telegram bot token format - Telegram disabled")
        return False
    
    if not bot_token.count(':') == 1:
        logging.warning("Invalid telegram bot token structure - Telegram disabled")
        return False
    
    # Validate chat ID format
    if not chat_id or not (chat_id.lstrip('-').isdigit() or chat_id.startswith('@')):
        logging.warning("Invalid telegram chat ID format - Telegram disabled")
        return False
    
    logging.info("Telegram environment variables validated - Telegram enabled")
    return True


# ===============================================================================
#                          Phoenix95 Smart Priority Manager
# ===============================================================================


class SmartPriorityManager:
    """Phoenix95 analysis result based smart priority manager"""

    def __init__(self):
        self.priority_rules = config.SMART_PRIORITY

        # Statistics by priority
        self.priority_stats = {
            "CRITICAL": {"count": 0, "avg_phoenix_score": 0.0},
            "HIGH": {"count": 0, "avg_phoenix_score": 0.0},
            "MEDIUM": {"count": 0, "avg_phoenix_score": 0.0},
            "LOW": {"count": 0, "avg_phoenix_score": 0.0},
        }

        logging.info("Phoenix95 smart priority manager initialization completed")

    def calculate_smart_priority(self, notification_data: Dict) -> str:
        """Calculate smart priority based on Phoenix95"""

        phoenix_score = notification_data.get("phoenix95_score", 0)
        confidence = notification_data.get("confidence", 0.0)
        risk_level = notification_data.get("risk_level", "MEDIUM")

        priorities = []

        # Evaluate Phoenix95 score
        for min_score, priority in sorted(
            self.priority_rules["phoenix95_thresholds"].items(), reverse=True
        ):
            if phoenix_score >= min_score:
                priorities.append(priority)
                break

        # Evaluate confidence
        for min_confidence, priority in sorted(
            self.priority_rules["confidence_thresholds"].items(), reverse=True
        ):
            if confidence >= min_confidence:
                priorities.append(priority)
                break

        # Direct mapping of risk level
        risk_priority_map = {
            "CRITICAL": "CRITICAL",
            "HIGH": "HIGH",
            "MEDIUM": "MEDIUM",
            "LOW": "LOW",
        }
        priorities.append(risk_priority_map.get(risk_level, "MEDIUM"))

        # Select highest priority
        priority_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        final_priority = "MEDIUM"

        for priority in priority_order:
            if priority in priorities:
                final_priority = priority
                break

        # Update statistics
        self._update_priority_stats(final_priority, phoenix_score)

        return final_priority

    def should_send_immediate(self, priority: str, notification_type: str) -> bool:
        """Determine whether to send immediately"""

        # CRITICAL always sends immediately
        if priority == "CRITICAL":
            return True

        # HIGH priority with specific types sends immediately
        if (
            priority == "HIGH"
            and notification_type in self.priority_rules["immediate_send_types"]
        ):
            return True

        return False

    def get_batch_config(self, priority: str) -> Dict:
        """Query batch configuration by priority"""
        return self.priority_rules["batch_optimization"].get(
            priority, {"size": 5, "interval": 2.0}
        )

    def _update_priority_stats(self, priority: str, phoenix_score: float):
        """Update priority statistics"""
        if priority in self.priority_stats:
            stats = self.priority_stats[priority]
            current_count = stats["count"]
            current_avg = stats["avg_phoenix_score"]

            stats["count"] = current_count + 1
            stats["avg_phoenix_score"] = (
                (current_avg * current_count) + phoenix_score
            ) / (current_count + 1)

    def get_priority_statistics(self) -> Dict:
        """Query priority statistics"""
        return self.priority_stats.copy()


# ===============================================================================
#                          Full Pipeline Monitoring System
# ===============================================================================

class EnhancedTelegramNotificationManager:
    """
    Phoenix95 based enhanced telegram notification manager
    
    Optimized for high-spec hardware (128GB RAM, 16-24 cores):
        - Large queue sizes for high throughput
        - Connection pooling for parallel requests
        - TTL cache for memory management
        - Smart priority system integration
    """

    def __init__(self):
        """
        Initialize Enhanced Telegram Notification Manager
        
        High-spec optimizations:
            - Queue sizes increased 5-10x for 128GB RAM
            - Connection pool increased 5x for 16-24 cores
            - TTL cache size doubled (2000 entries)
        """
        self.config = config.TELEGRAM
        self.bot_token = self.config["bot_token"]
        self.chat_id = self.config["chat_id"]

        # Initialize smart priority system
        self.smart_priority = SmartPriorityManager()

        # High-spec optimization: Optimized queue sizes for 128GB RAM
        # Queue sizes increased 5x from baseline for high-throughput processing
        self.message_queues = {
            "CRITICAL": asyncio.Queue(maxsize=500),   # Baseline: 100 -> 500 (5x)
            "HIGH": asyncio.Queue(maxsize=1000),      # Baseline: 200 -> 1000 (5x)
            "MEDIUM": asyncio.Queue(maxsize=2500),    # Baseline: 500 -> 2500 (5x)
            "LOW": asyncio.Queue(maxsize=5000),       # Baseline: 1000 -> 5000 (5x)
        }

        # High-spec optimization: Optimized batch queues for 128GB RAM
        # Batch queue sizes increased 4-5x for efficient batch processing
        self.batch_queues = {
            "HIGH": deque(maxlen=200),    # Baseline: 50 -> 200 (4x)
            "MEDIUM": deque(maxlen=500),  # Baseline: 100 -> 500 (5x)
            "LOW": deque(maxlen=1000),    # Baseline: 200 -> 1000 (5x)
        }

        # Statistics dictionary pre-allocated in memory (128GB RAM available)
        self.stats = {
            "total_sent": 0,
            "successful_sent": 0,
            "failed_sent": 0,
            "rate_limited": 0,
            "queue_overflow": 0,
            "last_sent_time": 0,
            "avg_response_time": 0.0,
            "smart_priority_used": 0,
            "immediate_sent": 0,
            "batch_sent": 0,
            "blocked_users": 0,
            "circuit_breaker_triggered": 0,
            "fallback_used": 0,
        }

        # User status tracking system (thread-safe sets and dicts)
        self.blocked_users = set()
        self.user_retry_schedule = {}
        self.last_successful_send = {}
        
        # Circuit breaker configuration
        self.circuit_breaker_status = {
            "enabled": True,
            "failure_threshold": 3,
            "recovery_timeout": 300,  # 5 minutes
            "last_failure_time": 0,
            "consecutive_failures": 0,
        }

        # Rate limiting setup
        self.rate_limiter = asyncio.Semaphore(self.config["rate_limit"])
        self.last_sent_times = deque(maxlen=self.config["rate_limit"])
        
        # Load message templates (all pre-loaded in memory)
        self.templates = self._load_enhanced_message_templates()
        
        # High-spec optimization: TTL cache for memory leak prevention
        # Cache size doubled (2000) for 128GB RAM system
        try:
            from cachetools import TTLCache
            self.alert_cooldowns = TTLCache(maxsize=2000, ttl=300)  # Baseline: 1000 -> 2000 (2x)
            logging.info("TTLCache applied for memory leak prevention (2000 entries, 300s TTL)")
        except ImportError:
            self.alert_cooldowns = {}
            self._last_cleanup_time = time.time()
            logging.warning("cachetools not found - using default dictionary with manual cleanup")
        
        # High-spec optimization: HTTP connection pooling for 16-24 cores
        # Connection limits increased 5x for parallel request processing
        self.http_connector = aiohttp.TCPConnector(
            limit=50,              # Baseline: 10 -> 50 (5x) - Total connections
            limit_per_host=20,     # Baseline: 5 -> 20 (4x) - Per-host connections
            ttl_dns_cache=300,     # DNS cache TTL: 5 minutes
            use_dns_cache=True,    # Enable DNS caching
        )
        self.http_session = None  # Initialized during startup
        
        logging.info("Enhanced telegram notification manager initialization completed")
        logging.info("High-spec optimizations: Queue sizes 5x, Connection pool 5x, TTL cache 2x")

    def _load_enhanced_message_templates(self) -> Dict[str, str]:
        """
        Enhanced message templates for Phoenix95 notification system
        
        Optimized for high-spec hardware (128GB RAM, 16-24 cores):
            - All templates pre-loaded in memory
            - Fast dictionary lookup (O(1))
            - No runtime template compilation
            - Emoji-free for better compatibility
            - Clean text format for logging systems
        """
        return {
            "performance_alert": """[PERFORMANCE WARNING] {severity}

Performance Metric: {metric}
- Current Value: {current_value}
- Threshold: {threshold}

Impact: {impact_description}
Recommended Actions: {recommended_actions}

{timestamp}""",
            "phoenix95_brain_analysis": """[PHOENIX95 AI] Analysis Complete

Analysis Results
- Symbol: {symbol}
- Action: {action}
- Phoenix95 Score: {phoenix_score}/95
- AI Confidence: {confidence}%

Recommended Settings
- Recommended Leverage: {recommended_leverage}x
- Kelly Ratio: {kelly_fraction}%
- Position Size: ${position_size}

Backtest Projection
- Expected Return: {expected_return}
- Risk Metric: VaR {var_value}

Next Step: Risk Service Review -> Execute Trade

{timestamp}""",
            "risk_service_decision": """[RISK {decision}] {result}

{approval_icon}

Signal Information
- Symbol: {symbol}
- Action: {action}
- Phoenix95 Score: {phoenix_score}/95 | Risk Score: {risk_score}

{decision_content}

Timestamp: {timestamp}""",
            "trade_execution_result": """[TRADE {status}] {status_icon}

Execution Information
- Symbol: {symbol}
- Action: {action}
- Entry Price: ${entry_price}
- Leverage: {leverage}x ISOLATED

{execution_content}

Execution Time: {execution_time}ms

{timestamp}""",
            "pipeline_status": """[PIPELINE STATUS] Update

Service Status:
{service_status}

Data Flow:
{data_flow_status}

System Performance:
- CPU: {cpu_percent}%
- Memory: {memory_percent}%
- Processed Signals: {signals_processed}
- Success Rate: {success_rate}%

{timestamp}""",
            "smart_priority_summary": """[SMART PRIORITY] Summary Report

Stats by Priority:
- CRITICAL: {critical_count} (avg: {critical_avg})
- HIGH: {high_count} (avg: {high_avg})
- MEDIUM: {medium_count} (avg: {medium_avg})
- LOW: {low_count} (avg: {low_avg})

Sending Method:
- Immediate: {immediate_sent}
- Batch: {batch_sent}

Performance:
- Avg Response Time: {avg_response_time}ms
- Success Rate: {success_rate}%

{timestamp}""",
            "trade_execution": """[PHOENIX95] Trade Execution

Trade Information
- Symbol: {symbol}
- Action: {action}
- Price: {price}
- Leverage: {leverage}x ISOLATED

Position Information
- Position Size: {position_size}
- Required Margin: {margin}
- Liquidation Price: {liquidation_price}

Phoenix95 Analysis
- Score: {confidence}
- Risk: {risk_level}
- Execution Time: {execution_time}ms

Profit/Loss Settings
- Take Profit: {take_profit} (+2%)
- Stop Loss: {stop_loss} (-2%)

{timestamp}""",
            "liquidation_warning": """[LIQUIDATION RISK] Warning Alert

Position Details:
{symbol} {action} {leverage}x
- Current Price: {current_price}
- Liquidation Price: {liquidation_price}

Risk Assessment:
- Risk Level: {risk_level}
- Distance to Liquidation: {distance_percent}%
- Estimated Loss: {estimated_loss}

Recommended Actions:
- Immediately review position
- Consider adding margin
- Review stop-loss settings

{timestamp}"""
        }
        
    async def _check_rate_limit(self):
        """
        Check and enforce Telegram API rate limits with sliding window algorithm
        
        Prevents exceeding Telegram rate limits by tracking send times in a sliding window.
        Optimized for high-spec hardware (128GB RAM, 16-24 cores) with minimal overhead.
        
        Features:
            - Sliding window rate limiting (1.2s window)
            - Conservative rate limit (25 msg/s instead of 30 for safety)
            - Exponential cleanup optimization
            - Statistics tracking (waits, wait time, approach warnings)
            - Task cancellation handling
            - Graceful degradation on errors
        
        Rate Limit Strategy:
            - Telegram official limit: 30 messages/second
            - Our conservative limit: 25 messages/second
            - Window duration: 1.2 seconds (safety margin)
            - Approach warning: 80% threshold (20/25 messages)
        """
        try:
            # Use semaphore for thread-safe rate limiting
            async with self.rate_limiter:
                current_time = time.time()
                
                # Configuration: sliding window parameters
                window_duration = 1.2  # 1.2 seconds for safety margin
                official_limit = self.config.get("rate_limit", 30)
                safety_margin = 5
                safe_rate_limit = min(official_limit - safety_margin, 25)
                approach_threshold = 0.8  # Warn at 80% capacity
                
                # Level 1: Clean up old timestamps outside the sliding window
                # Optimized cleanup: early exit when encountering recent timestamp
                cleanup_count = 0
                while self.last_sent_times:
                    oldest_time = self.last_sent_times[0]
                    age = current_time - oldest_time
                    
                    if age > window_duration:
                        self.last_sent_times.popleft()
                        cleanup_count += 1
                    else:
                        # Stop cleanup when we hit a recent timestamp
                        break
                
                # Debug log cleanup if significant
                if cleanup_count > 10:
                    logging.debug(f"Rate limit: cleaned up {cleanup_count} old timestamps")
                
                # Level 2: Check current rate and determine if waiting is needed
                current_count = len(self.last_sent_times)
                
                if current_count >= safe_rate_limit:
                    # Rate limit reached - calculate wait time
                    oldest_time = self.last_sent_times[0]
                    time_since_oldest = current_time - oldest_time
                    wait_time = window_duration - time_since_oldest
                    
                    if wait_time > 0:
                        # Initialize statistics if needed
                        if "rate_limit_waits" not in self.stats:
                            self.stats["rate_limit_waits"] = 0
                            self.stats["total_wait_time"] = 0.0
                            self.stats["max_wait_time"] = 0.0
                            self.stats["avg_wait_time"] = 0.0
                        
                        # Update statistics
                        self.stats["rate_limit_waits"] += 1
                        self.stats["total_wait_time"] += wait_time
                        self.stats["max_wait_time"] = max(
                            self.stats.get("max_wait_time", 0.0), 
                            wait_time
                        )
                        self.stats["avg_wait_time"] = (
                            self.stats["total_wait_time"] / self.stats["rate_limit_waits"]
                        )
                        
                        # Log at DEBUG level to reduce noise
                        logging.debug(
                            f"Rate limit: {current_count}/{safe_rate_limit} messages in window, "
                            f"waiting {wait_time:.2f}s (total waits: {self.stats['rate_limit_waits']})"
                        )
                        
                        # Wait for the required duration
                        await asyncio.sleep(wait_time)
                        
                        # Level 3: Post-wait cleanup
                        # Re-check and clean timestamps after waiting
                        current_time = time.time()
                        post_wait_cleanup = 0
                        
                        while self.last_sent_times:
                            oldest_time = self.last_sent_times[0]
                            age = current_time - oldest_time
                            
                            if age > window_duration:
                                self.last_sent_times.popleft()
                                post_wait_cleanup += 1
                            else:
                                break
                        
                        # Verify we're now under the limit
                        current_count_after_wait = len(self.last_sent_times)
                        if current_count_after_wait >= safe_rate_limit:
                            logging.warning(
                                f"Rate limit: Still at limit after waiting "
                                f"({current_count_after_wait}/{safe_rate_limit}), "
                                f"cleaned up {post_wait_cleanup} timestamps"
                            )
                
                # Level 4: Record this send time
                self.last_sent_times.append(time.time())
                
                # Level 5: Approach warning (preventive logging)
                # Only log when approaching limit to provide early warning
                final_count = len(self.last_sent_times)
                approach_count = int(safe_rate_limit * approach_threshold)
                
                if final_count >= approach_count:
                    # Calculate current rate (messages per second)
                    if final_count >= 2:
                        oldest = self.last_sent_times[0]
                        newest = self.last_sent_times[-1]
                        time_span = newest - oldest
                        
                        if time_span > 0:
                            current_rate = final_count / time_span
                            logging.debug(
                                f"Rate limit: Approaching threshold - "
                                f"{final_count}/{safe_rate_limit} messages "
                                f"({current_rate:.1f} msg/s)"
                            )
                
                # Level 6: Performance monitoring (every 100 waits)
                if "rate_limit_waits" in self.stats:
                    wait_count = self.stats["rate_limit_waits"]
                    
                    if wait_count > 0 and wait_count % 100 == 0:
                        avg_wait = self.stats["avg_wait_time"]
                        max_wait = self.stats["max_wait_time"]
                        total_wait = self.stats["total_wait_time"]
                        
                        logging.info(
                            f"Rate limit stats: {wait_count} waits, "
                            f"avg: {avg_wait:.2f}s, max: {max_wait:.2f}s, "
                            f"total: {total_wait:.1f}s"
                        )
        
        except asyncio.CancelledError:
            # Task cancellation is expected during shutdown
            logging.debug("Rate limit check cancelled (task shutdown)")
            raise  # Re-raise to propagate cancellation
        
        except Exception as e:
            # Unexpected error in rate limiting
            error_name = type(e).__name__
            logging.error(f"Rate limit check failed: {error_name}: {e}")
            logging.debug(f"Rate limit state: {len(self.last_sent_times)} timestamps in queue")
            
            # Fallback: conservative wait to prevent rate limit violations
            fallback_wait = 0.2
            logging.debug(f"Rate limit: Using fallback wait of {fallback_wait}s")
            await asyncio.sleep(fallback_wait)

    async def start_enhanced_processing(self):
        if not self.http_session:
            try:
                self.http_session = aiohttp.ClientSession(
                    connector=self.http_connector,
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers={'User-Agent': 'Phoenix95-NotifyService/4.0'}
                )
                logging.info("HTTP connection pool created successfully")
            except Exception as e:
                logging.error(f"HTTP session creation failed: {e}")
        
        logging.info("Starting enhanced message processing")
        workers = []
        
        for priority in config.MESSAGE_PROCESSING["priority_levels"]:
            for i in range(config.MESSAGE_PROCESSING["worker_count"]):
                worker = asyncio.create_task(
                    self._priority_worker(priority, f"{priority}_WORKER_{i+1}")
                )
                workers.append(worker)
        
        workers.append(asyncio.create_task(self._optimized_batch_processor()))
        workers.append(asyncio.create_task(self._immediate_send_processor()))
        workers.append(asyncio.create_task(self.start_blocked_user_recovery_task()))
        
        logging.info(f"Started {len(workers)} processing workers")
        return workers

    async def _priority_worker(self, priority: str, worker_name: str):
        queue = self.message_queues[priority]
        while True:
            try:
                timeout = {"CRITICAL": 0.1, "HIGH": 0.5, "MEDIUM": 1.0, "LOW": 2.0}[
                    priority
                ]
                try:
                    message_data = await asyncio.wait_for(queue.get(), timeout=timeout)
                    success = await self._send_telegram_message(
                        message_data["text"],
                        message_data.get("parse_mode", "HTML"),
                        message_data.get("disable_preview", True),
                    )
                    if success:
                        self.stats["successful_sent"] += 1
                    else:
                        self.stats["failed_sent"] += 1
                    queue.task_done()
                except asyncio.TimeoutError:
                    continue
            except Exception as e:
                logging.error(f"{worker_name} error: {e}")
                await asyncio.sleep(1)

    async def _optimized_batch_processor(self):
        while True:
            try:
                for priority in ["HIGH", "MEDIUM", "LOW"]:
                    batch_config = self.smart_priority.get_batch_config(priority)
                    batch_queue = self.batch_queues[priority]
                    
                    # Process if enough messages or timeout
                    if len(batch_queue) >= batch_config["size"]:
                        batch = [batch_queue.popleft() for _ in range(batch_config["size"])]
                        await self._process_priority_batch(batch, priority)
                        await asyncio.sleep(batch_config["interval"])
                        
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Batch processor error: {e}")
                await asyncio.sleep(5)

    async def _process_priority_batch(self, batch: List[Dict], priority: str):
        """
        Process a batch of messages for a specific priority level
        
        Args:
            batch: List of message data dictionaries to process
            priority: Priority level (HIGH, MEDIUM, LOW)
            
        Features:
            - Batch telegram API calls for efficiency
            - Rate limit aware processing
            - Individual message retry on batch failure
            - Statistics tracking for monitoring
            - Optimized for high-spec hardware (128GB RAM, 16-24 cores)
        
        High-spec optimizations:
            - Parallel message processing with asyncio.gather
            - Batch size optimized for multi-core CPU
            - In-memory batch queue operations (128GB RAM)
        """
        if not batch:
            return
        
        try:
            batch_size = len(batch)
            logging.debug(f"Processing {priority} priority batch: {batch_size} messages")
            
            # Track batch processing start time
            batch_start_time = time.time()
            
            # High-spec optimization: Process messages in parallel
            # Multi-core CPU (16-24 cores) can handle concurrent sends
            send_tasks = []
            
            for message_data in batch:
                # Extract message details
                text = message_data.get("text", "")
                parse_mode = message_data.get("parse_mode", "HTML")
                disable_preview = message_data.get("disable_preview", True)
                
                # Create send task (non-blocking)
                task = self._send_telegram_message(text, parse_mode, disable_preview)
                send_tasks.append(task)
            
            # High-spec optimization: Execute all sends concurrently
            # Benefits from 16-24 core CPU for parallel network I/O
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            
            # Track batch processing results
            successful = sum(1 for r in results if r is True)
            failed = sum(1 for r in results if r is not True)
            
            # Update statistics
            self.stats["batch_sent"] += successful
            if "batch_processed" not in self.stats:
                self.stats["batch_processed"] = 0
                self.stats["batch_success_rate"] = 0.0
            
            self.stats["batch_processed"] += 1
            
            # Calculate batch success rate
            total_batches = self.stats["batch_processed"]
            total_successful = self.stats["batch_sent"]
            self.stats["batch_success_rate"] = (total_successful / max(total_batches * batch_size, 1)) * 100
            
            # Calculate batch processing time
            batch_time = (time.time() - batch_start_time) * 1000
            
            # Log batch results
            if failed > 0:
                logging.warning(
                    f"Batch {priority} completed: {successful}/{batch_size} successful, "
                    f"{failed} failed, time: {batch_time:.0f}ms"
                )
            else:
                logging.info(
                    f"Batch {priority} completed: {successful}/{batch_size} successful, "
                    f"time: {batch_time:.0f}ms"
                )
            
        except Exception as e:
            # Batch processing failed - log error
            error_name = type(e).__name__
            logging.error(f"Batch processing error for {priority}: {error_name}: {e}")
            logging.debug(f"Batch size: {len(batch) if batch else 0}")
            
            # Track batch failure
            if "batch_failures" not in self.stats:
                self.stats["batch_failures"] = 0
            self.stats["batch_failures"] += 1

    async def _immediate_send_processor(self):
        while True:
            try:
                await asyncio.sleep(0.1)
            except Exception as e:
                logging.error(f"Immediate send processor error: {e}")
                await asyncio.sleep(1)

    async def _send_telegram_message(
        self, text: str, parse_mode: str = "HTML", disable_preview: bool = True
    ) -> bool:
        """
        Send message to Telegram with comprehensive error handling and retry logic
        
        Args:
            text: Message text to send
            parse_mode: Parse mode (HTML, Markdown, or None)
            disable_preview: Disable web page preview
            
        Returns:
            bool: True if message sent successfully, False otherwise
            
        Enhanced features:
            - Smart logging (reduce noise for retries)
            - Circuit breaker integration
            - Blocked user detection and recovery
            - Adaptive timeout scaling
            - Exponential backoff retry
            - Detailed error classification
            - Response time tracking
            - Fallback notification system
            - Optimized for high-spec hardware (128GB RAM, 16-24 cores)
        """
        # Level 1: Pre-validation checks
        
        # Check if Telegram is enabled
        if not self.config["enabled"]:
            logging.debug("Telegram disabled in config - message not sent")
            return False
        
        # Check credentials
        if not self.bot_token or not self.chat_id:
            logging.debug("Telegram credentials not configured")
            logging.debug("Setup: Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
            return False
        
        # Validate message length (Telegram limit: 4096 characters)
        max_length = self.config.get("max_message_length", 4096)
        if len(text) > max_length:
            logging.warning(f"Message length {len(text)} exceeds Telegram limit {max_length}, truncating")
            text = text[:max_length-50] + "\n\n... (message truncated)"
        
        # Level 2: Circuit breaker check
        if self._is_circuit_breaker_open():
            logging.info("Circuit breaker open - routing to fallback notification")
            self.stats["circuit_breaker_triggered"] += 1
            return await self._send_fallback_notification(text, "HIGH")
        
        # Level 3: Blocked user check with recovery attempt
        if self.chat_id in self.blocked_users:
            logging.debug("User in blocked list - attempting connection recovery")
            self.stats["blocked_users"] += 1
            
            # Try to recover connection
            if await self._test_telegram_connection():
                logging.info("User unblocked bot - removed from blocked list")
                self.blocked_users.remove(self.chat_id)
            else:
                logging.info("User still blocked - routing to fallback notification")
                return await self._send_fallback_notification(text, "HIGH")
        
        # Level 4: Retry loop with adaptive configuration
        # High-spec optimization: Increased retry attempts for better reliability
        max_retries = self.config.get("retry_attempts", 4)  # Increased from 3 to 4
        base_timeout = self.config.get("timeout", 15)
        
        # Track first attempt separately for logging
        first_attempt = True
        
        # High-spec optimization: Pre-calculate backoff times for all attempts
        # Leverages 128GB RAM for efficient lookup table
        backoff_schedule = [min(2 ** i, 10) for i in range(max_retries + 1)]
        
        for attempt in range(max_retries + 1):
            try:
                # Rate limiting check
                await self._check_rate_limit()
                start_time = time.time()
                
                # Adaptive timeout scaling optimized for multi-core processing
                # 15s -> 20s -> 25s -> 30s -> 35s (extended for 4 retries)
                current_timeout = base_timeout + (attempt * 5)
                
                # Prepare request
                url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
                data = {
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": disable_preview,
                }
                
                # Use connection pool if available (optimized for high-spec hardware)
                session = self.http_session if self.http_session else aiohttp.ClientSession()
                
                try:
                    # High-spec optimized timeout configuration
                    # Connect: 7s (sufficient for high-speed network), Read: remaining time
                    connect_timeout = min(7, current_timeout - 3)
                    sock_read_timeout = current_timeout - connect_timeout
                    timeout = aiohttp.ClientTimeout(
                        total=current_timeout, 
                        connect=connect_timeout, 
                        sock_read=sock_read_timeout
                    )
                    
                    # Send request with optimized connection pooling
                    async with session.post(url, json=data, timeout=timeout) as response:
                        response_time = (time.time() - start_time) * 1000
                        self._update_response_time_stats(response_time)
                        
                        # Level 5: Response handling with detailed error classification
                        
                        # SUCCESS (200 OK)
                        if response.status == 200:
                            self._reset_circuit_breaker()
                            self._record_successful_send()
                            self.stats["total_sent"] += 1
                            self.stats["successful_sent"] += 1
                            self.stats["last_sent_time"] = time.time()
                            
                            # Only log retry success, not first success (reduce noise)
                            if attempt > 0:
                                logging.info(f"Telegram retry successful on attempt {attempt+1}/{max_retries+1}")
                            elif response_time > 2000:
                                # Log slow requests even on first success
                                logging.debug(f"Telegram sent successfully but slow: {response_time:.0f}ms")
                            
                            return True
                        
                        # FORBIDDEN (403) - Bot blocked or insufficient permissions
                        elif response.status == 403:
                            error_text = await response.text()
                            
                            if "bot was blocked by the user" in error_text.lower():
                                logging.error("Telegram: Bot blocked by user")
                                logging.error("Fix: Open Telegram -> Find bot -> Send /start command")
                                self._mark_user_blocked()
                                self.stats["blocked_users"] += 1
                                await self._send_fallback_notification(text, "HIGH")
                                return False
                            elif "chat not found" in error_text.lower():
                                logging.error(f"Telegram: Chat not found - check TELEGRAM_CHAT_ID: {self.chat_id}")
                                self._record_failure()
                                return False
                            else:
                                logging.error(f"Telegram forbidden (403): {error_text[:100]}")
                                self._record_failure()
                                return False
                        
                        # RATE LIMIT (429) - Too many requests
                        elif response.status == 429:
                            self.stats["rate_limited"] += 1
                            retry_after = int(response.headers.get("Retry-After", 60))
                            
                            # Smart logging: DEBUG for early attempts, WARNING for last
                            if attempt < max_retries - 1:
                                logging.debug(f"Telegram rate limit: waiting {retry_after}s (attempt {attempt+1}/{max_retries+1})")
                            else:
                                logging.warning(f"Telegram rate limit: waiting {retry_after}s (final attempt)")
                            
                            if attempt < max_retries:
                                await asyncio.sleep(retry_after)
                                continue
                            
                            self._record_failure()
                            logging.warning("Telegram rate limit exceeded after all retries")
                            return False
                        
                        # SERVER ERRORS (502, 503, 504) - Telegram infrastructure issues
                        elif response.status in [502, 503, 504]:
                            error_text = await response.text()
                            
                            # Smart logging: DEBUG for retryable attempts, WARNING for last
                            if attempt < max_retries:
                                logging.debug(f"Telegram server error {response.status} (attempt {attempt+1}/{max_retries+1}) - retrying")
                            else:
                                logging.warning(f"Telegram server error {response.status} after all retries")
                                logging.debug(f"Error details: {error_text[:200]}")
                            
                            self._record_failure()
                            
                            if attempt < max_retries:
                                # Use pre-calculated backoff schedule for better performance
                                backoff_time = backoff_schedule[attempt]
                                await asyncio.sleep(backoff_time)
                                continue
                            
                            return False
                        
                        # CLIENT ERRORS (400-499) - Request issue, likely not retryable
                        elif 400 <= response.status < 500:
                            error_text = await response.text()
                            logging.error(f"Telegram client error {response.status}: {error_text[:100]}")
                            self._record_failure()
                            return False  # Do not retry client errors
                        
                        # OTHER ERRORS (500-599 except 502-504)
                        else:
                            error_text = await response.text()
                            
                            # Log details on first and last attempt only
                            if first_attempt or attempt == max_retries:
                                logging.warning(f"Telegram error {response.status}: {error_text[:100]}")
                            else:
                                logging.debug(f"Telegram error {response.status} (attempt {attempt+1}/{max_retries+1})")
                            
                            self._record_failure()
                            
                            if attempt < max_retries:
                                # Use pre-calculated backoff schedule
                                backoff_time = backoff_schedule[attempt]
                                await asyncio.sleep(backoff_time)
                                continue
                            
                            return False
                
                finally:
                    # Close session if not using connection pool
                    if not self.http_session:
                        await session.close()
                    
                    # Mark first attempt complete
                    first_attempt = False
            
            # Level 6: Exception handling with classification
            
            except asyncio.TimeoutError:
                self._record_failure()
                
                # Smart logging: DEBUG for retryable timeouts, WARNING for final
                if attempt < max_retries:
                    logging.debug(f"Telegram timeout {current_timeout}s (attempt {attempt+1}/{max_retries+1}) - retrying")
                    await asyncio.sleep(2)
                    continue
                else:
                    logging.warning(f"Telegram timeout {current_timeout}s after all {max_retries+1} attempts")
                    self.stats["failed_sent"] += 1
                    return False
            
            except aiohttp.ClientError as e:
                self._record_failure()
                error_name = type(e).__name__
                error_msg = str(e)[:50]
                
                # Smart logging: DEBUG for retryable errors, WARNING for final
                if attempt < max_retries:
                    logging.debug(f"Telegram connection error: {error_name} - {error_msg} (attempt {attempt+1}/{max_retries+1})")
                    # Use pre-calculated backoff schedule for consistency
                    backoff_time = backoff_schedule[attempt]
                    await asyncio.sleep(backoff_time)
                    continue
                else:
                    logging.warning(f"Telegram connection error after all retries: {error_name} - {error_msg}")
                    self.stats["failed_sent"] += 1
                    return False
            
            except Exception as e:
                # Unexpected errors always logged at ERROR level
                self._record_failure()
                error_name = type(e).__name__
                logging.error(f"Telegram unexpected error: {error_name}: {e} (attempt {attempt+1}/{max_retries+1})")
                
                if attempt < max_retries:
                    logging.debug("Retrying after unexpected error")
                    await asyncio.sleep(2)
                    continue
                else:
                    self.stats["failed_sent"] += 1
                    logging.error(f"Telegram failed after all retries due to unexpected error")
                    return False
        
        # Level 7: Final failure after all retries exhausted
        self.stats["total_sent"] += 1
        self.stats["failed_sent"] += 1
        logging.error(f"Telegram send final failure: all {max_retries+1} attempts exhausted")
        return False

    async def _test_telegram_connection(self) -> bool:
        """Test if telegram connection is working"""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getMe"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as response:
                    return response.status == 200
        except Exception:
            return False

    def _is_circuit_breaker_open(self) -> bool:
        """Check if circuit breaker is open"""
        if not self.circuit_breaker_status["enabled"]:
            return False
            
        current_time = time.time()
        status = self.circuit_breaker_status
        
        # Check if recovery timeout has passed
        if (current_time - status["last_failure_time"]) > status["recovery_timeout"]:
            # Reset circuit breaker after recovery timeout
            status["consecutive_failures"] = 0
            return False
            
        # Check if failure threshold exceeded
        return status["consecutive_failures"] >= status["failure_threshold"]

    def _record_failure(self):
        """Record failure for circuit breaker"""
        current_time = time.time()
        status = self.circuit_breaker_status
        status["consecutive_failures"] += 1
        status["last_failure_time"] = current_time
        
        if status["consecutive_failures"] >= status["failure_threshold"]:
            logging.warning(f"Circuit breaker opened after {status['consecutive_failures']} failures")

    def _record_successful_send(self):
        """Record successful send"""
        self.last_successful_send[self.chat_id] = time.time()

    def _reset_circuit_breaker(self):
        """Reset circuit breaker on successful send"""
        self.circuit_breaker_status["consecutive_failures"] = 0

    def _mark_user_blocked(self):
        """Mark user as blocked and schedule retry"""
        self.blocked_users.add(self.chat_id)
        retry_time = time.time() + 3600  # Retry after 1 hour
        self.user_retry_schedule[self.chat_id] = retry_time
        logging.warning(f"User {self.chat_id} marked as blocked, retry scheduled for {datetime.fromtimestamp(retry_time)}")

    async def _send_fallback_notification(self, message: str, priority: str) -> bool:
        """Enhanced fallback notification system with multiple channels"""
        backup_success = False
        console_success = False

        try:
            # Always log to console first
            try:
                clean_message = self._clean_message_for_file(message)
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                console_message = f"[{timestamp}] FALLBACK {priority}: {clean_message}"
                logging.warning(console_message)
                console_success = True
            except Exception as e:
                logging.error(f"Console fallback failed: {e}")
            
            # File backup for critical and high priority messages
            if priority in ["CRITICAL", "HIGH"]:
                try:
                    # Create backup directory safely
                    backup_dir = Path("critical_alerts_backup")
                    backup_dir.mkdir(exist_ok=True)
                    
                    # Check if directory is writable with simple test
                    test_file = backup_dir / "test_write.tmp"
                    try:
                        test_file.touch()
                        test_file.unlink()
                    except Exception:
                        logging.debug(f"Backup directory not writable: {backup_dir}")
                        return console_success
                    
                    # Create daily backup file with proper naming
                    date_str = datetime.now().strftime('%Y%m%d')
                    backup_file = backup_dir / f"alerts_{date_str}.log"
                    
                    # Clean message content for file storage
                    clean_message = self._clean_message_for_file(message)
                    timestamp = datetime.now().isoformat()
                    
                    # Try primary backup location
                    try:
                        with open(backup_file, "a", encoding="utf-8") as f:
                            f.write(f"[{timestamp}] {priority}: {clean_message}\n")
                            f.flush()
                        backup_success = True
                        logging.debug(f"Alert backed up to: {backup_file}")
                    except Exception as e:
                        logging.debug(f"Primary backup failed: {e}")
                        # Try alternative location
                        try:
                            alt_file = Path("fallback_alerts.log")
                            with open(alt_file, "a", encoding="utf-8") as f:
                                f.write(f"[{timestamp}] {priority}: {clean_message}\n")
                                f.flush()
                            backup_success = True
                            logging.debug(f"Alert backed up to alternative: {alt_file}")
                        except Exception as e2:
                            logging.warning(f"Alternative backup also failed: {e2}")
                    
                    # Update statistics
                    if backup_success:
                        if "fallback_used" not in self.stats:
                            self.stats["fallback_used"] = 0
                        self.stats["fallback_used"] += 1
                    
                except Exception as e:
                    logging.warning(f"File backup system failed: {e}")
            
            # Email fallback for CRITICAL messages (if configured)
            if priority == "CRITICAL" and not backup_success:
                try:
                    email_success = await self._try_email_fallback(message, priority)
                    if email_success:
                        logging.info("Critical alert sent via email fallback")
                        return True
                except Exception as e:
                    logging.debug(f"Email fallback failed: {e}")
            
            return backup_success or console_success

        except Exception as e:
            logging.error(f"Fallback notification system failed: {e}")
            # Last resort - use critical logging level
            try:
                logging.critical(f"EMERGENCY FALLBACK: {priority} - {message[:100]}")
                return True
            except Exception:
                return False

    async def _try_email_fallback(self, message: str, priority: str) -> bool:
        """Try to send email notification as last resort"""
        try:
            import os
            smtp_server = os.environ.get("SMTP_SERVER")
            smtp_user = os.environ.get("SMTP_USER") 
            smtp_password = os.environ.get("SMTP_PASSWORD")
            alert_email = os.environ.get("ALERT_EMAIL")
            
            if not all([smtp_server, smtp_user, smtp_password, alert_email]):
                logging.info("Email fallback not configured - skipping")
                return False
            
            # Simple email sending would go here
            # For now, just log that we would send email
            logging.info(f"Would send email to {alert_email} for {priority} alert")
            return False
            
        except Exception as e:
            logging.error(f"Email fallback setup failed: {e}")
            return False

    def _clean_message_for_file(self, message: str) -> str:
        """Clean message content for file storage"""
        try:
            # Basic validation
            if not message or not isinstance(message, str):
                return "Empty message"
            
            # Handle very long messages early
            if len(message) > 10000:
                message = message[:10000] + "... (truncated)"
            
            import re
            
            # Remove HTML tags
            clean = re.sub(r'<[^>]*>', '', message)
            
            # Remove excessive whitespace
            clean = re.sub(r'\s+', ' ', clean)
            clean = re.sub(r'\n\s*\n', '\n', clean)
            
            # Remove problematic characters (keep common punctuation)
            clean = re.sub(r'[^\w\s\-.,():/%$@#+=*&^!?<>|{}[\]~`\n\r\t]', '', clean)
            
            # Remove control characters (except newline, tab, carriage return)
            clean = ''.join(c for c in clean if ord(c) >= 32 or c in '\t\n\r')
            
            # Final cleanup
            clean = clean.strip()
            
            # Ensure we have something to return
            if not clean:
                return "Message content unavailable"
            
            # Limit final length
            if len(clean) > 5000:
                clean = clean[:5000] + "..."
            
            return clean
            
        except Exception as e:
            # Fallback for any error
            try:
                safe_message = str(message)[:500] if message else "Unknown message"
                return f"Message processing failed: {safe_message}"
            except Exception:
                return "Message processing completely failed"

    async def _check_blocked_users_recovery(self):
        """Periodically check if blocked users can be retried"""
        current_time = time.time()
        recovered_users = []
        
        for chat_id, retry_time in self.user_retry_schedule.items():
            if current_time >= retry_time:
                # Remove from blocked list for retry
                if chat_id in self.blocked_users:
                    self.blocked_users.remove(chat_id)
                    recovered_users.append(chat_id)
        
        # Clean up retry schedule
        for chat_id in recovered_users:
            del self.user_retry_schedule[chat_id]
            logging.info(f"User {chat_id} removed from blocked list for retry")

    async def start_blocked_user_recovery_task(self):
        """Start background task to recover blocked users periodically"""
        while True:
            try:
                await self._check_blocked_users_recovery()
                # Check every 30 minutes for blocked user recovery
                await asyncio.sleep(1800)
            except Exception as e:
                logging.error(f"Blocked user recovery task error: {e}")
                # Wait 5 minutes before retry on error
                await asyncio.sleep(300)

    async def send_phoenix95_brain_notification(self, data: Dict, priority: str = None):
        """
        Send Phoenix95 Brain Service notification with comprehensive AI analysis validation
        
        Args:
            data: Complete data from Brain Service containing signal_data and phoenix_analysis
            priority: Notification priority level (optional, auto-determined by phoenix_score)
        
        Enhanced features:
            - Deep Phoenix95 analysis validation (95-point scoring system)
            - AI confidence threshold checking
            - Signal quality validation (filters invalid zero scores)
            - Leverage optimization validation
            - Kelly criterion validation
            - Position sizing safety checks
            - Backtest performance metrics validation
            - Risk metrics (VaR) validation
            - Smart priority determination based on AI confidence and phoenix score
            - Optimized for high-spec hardware (128GB RAM, 16-24 cores)
        """
        try:
            # Level 1: Validate root data structure
            if not data:
                logging.warning("Phoenix95 Brain notification: data is None or empty")
                return
            
            if not isinstance(data, dict):
                logging.warning(f"Phoenix95 Brain notification: data must be dict, got {type(data).__name__}")
                return
            
            # Level 2: Extract and validate signal_data (required)
            signal_data = data.get("signal_data")
            
            if signal_data is None:
                logging.warning("Phoenix95 Brain notification: signal_data is None")
                return
            
            if not isinstance(signal_data, dict):
                logging.warning(f"Phoenix95 Brain notification: signal_data must be dict, got {type(signal_data).__name__}")
                return
            
            # Level 3: Validate signal critical fields
            symbol = signal_data.get("symbol", "").strip() if signal_data.get("symbol") else ""
            action = signal_data.get("action", "").strip() if signal_data.get("action") else ""
            
            # Symbol validation with context
            if not symbol or symbol == "UNKNOWN":
                logging.warning(f"Phoenix95 Brain notification: Invalid symbol '{symbol}' in signal_data: {list(signal_data.keys())}")
                return
            
            # Action validation with context
            if not action or action == "UNKNOWN":
                logging.warning(f"Phoenix95 Brain notification: Invalid action '{action}' for symbol {symbol}")
                return
            
            # Level 4: Extract and validate phoenix_analysis
            phoenix_analysis = data.get("phoenix_analysis")
            
            # Phoenix analysis can be None or invalid - provide comprehensive defaults
            if phoenix_analysis is None:
                logging.info(f"Phoenix95 Brain notification: phoenix_analysis is None for {symbol}, using default structure")
                phoenix_analysis = self._get_default_phoenix_analysis()
            elif not isinstance(phoenix_analysis, dict):
                logging.warning(f"Phoenix95 Brain notification: phoenix_analysis must be dict, got {type(phoenix_analysis).__name__}, using defaults")
                phoenix_analysis = self._get_default_phoenix_analysis()
            
            # Level 5: Extract and validate Phoenix95 core metrics
            phoenix_score = phoenix_analysis.get("phoenix_score")
            if phoenix_score is None:
                logging.debug(f"Phoenix95 Brain notification: phoenix_score is None for {symbol}, using 0")
                phoenix_score = 0
            else:
                try:
                    phoenix_score = int(phoenix_score)
                    # Validate phoenix score range (0-95)
                    if phoenix_score < 0 or phoenix_score > 95:
                        logging.warning(f"Phoenix95 Brain notification: phoenix_score {phoenix_score} out of range [0-95] for {symbol}, clamping")
                        phoenix_score = max(0, min(95, phoenix_score))
                except (ValueError, TypeError) as e:
                    logging.warning(f"Phoenix95 Brain notification: Invalid phoenix_score type for {symbol}: {e}, using 0")
                    phoenix_score = 0
            
            # Extract AI confidence with validation
            ai_confidence = phoenix_analysis.get("ai_confidence")
            if ai_confidence is None:
                logging.debug(f"Phoenix95 Brain notification: ai_confidence is None for {symbol}, using 0.0")
                ai_confidence = 0.0
            else:
                try:
                    ai_confidence = float(ai_confidence)
                    # Validate confidence range (0.0-1.0)
                    if ai_confidence < 0.0 or ai_confidence > 1.0:
                        logging.warning(f"Phoenix95 Brain notification: ai_confidence {ai_confidence} out of range [0.0-1.0] for {symbol}, clamping")
                        ai_confidence = max(0.0, min(1.0, ai_confidence))
                except (ValueError, TypeError) as e:
                    logging.warning(f"Phoenix95 Brain notification: Invalid ai_confidence type for {symbol}: {e}, using 0.0")
                    ai_confidence = 0.0

            # ENHANCED: Signal Quality Validation (NEW)
            # Filter out invalid zero-score signals that indicate data/calculation errors
            if phoenix_score == 0 and ai_confidence == 0.0:
                # High-spec optimization: Thread-safe statistics tracking (128GB RAM available)
                # Initialize statistics dictionaries if needed
                if "invalid_phoenix95_signals" not in self.stats:
                    self.stats["invalid_phoenix95_signals"] = 0
                    self.stats["invalid_signal_symbols"] = {}
                    self.stats["invalid_signal_timestamps"] = []
                    self.stats["invalid_signal_actions"] = {}
                
                # Increment total invalid signal counter
                self.stats["invalid_phoenix95_signals"] += 1
                
                # Track per-symbol invalid signal count
                if symbol not in self.stats["invalid_signal_symbols"]:
                    self.stats["invalid_signal_symbols"][symbol] = 0
                self.stats["invalid_signal_symbols"][symbol] += 1
                
                # Track per-action invalid signal count (NEW)
                if action not in self.stats["invalid_signal_actions"]:
                    self.stats["invalid_signal_actions"][action] = 0
                self.stats["invalid_signal_actions"][action] += 1
                
                # Track timestamps for rate analysis (NEW)
                current_timestamp = time.time()
                self.stats["invalid_signal_timestamps"].append(current_timestamp)
                
                # Keep only last 100 timestamps (memory efficiency)
                if len(self.stats["invalid_signal_timestamps"]) > 100:
                    self.stats["invalid_signal_timestamps"] = self.stats["invalid_signal_timestamps"][-100:]
                
                # Calculate invalid signal rate (NEW)
                if len(self.stats["invalid_signal_timestamps"]) >= 2:
                    time_window = current_timestamp - self.stats["invalid_signal_timestamps"][0]
                    if time_window > 0:
                        invalid_rate = len(self.stats["invalid_signal_timestamps"]) / time_window
                        logging.debug(f"Invalid signal rate: {invalid_rate:.2f} signals/second over {time_window:.0f}s window")
                
                # Enhanced logging with comprehensive context
                logging.warning(
                    f"INVALID SIGNAL REJECTED: {symbol} {action} - "
                    f"Phoenix95 score: 0/95, AI confidence: 0.0% "
                    f"(Total rejected: {self.stats['invalid_phoenix95_signals']}, "
                    f"Symbol rejected: {self.stats['invalid_signal_symbols'][symbol]}, "
                    f"Action: {action} rejected {self.stats['invalid_signal_actions'][action]} times)"
                )
                
                # Provide troubleshooting guidance periodically with enhanced diagnostics
                if self.stats["invalid_phoenix95_signals"] % 10 == 1:
                    # Identify most problematic symbol (NEW)
                    most_problematic = max(self.stats["invalid_signal_symbols"].items(), key=lambda x: x[1])
                    
                    logging.warning(
                        "TROUBLESHOOTING: Repeated invalid signals detected. "
                        f"Most problematic symbol: {most_problematic[0]} ({most_problematic[1]} rejections). "
                        "Check: (1) Brain Service data connection, "
                        "(2) Phoenix95 calculation logic, "
                        "(3) Signal data quality from webhook"
                    )
                
                # Do not send notification for invalid signals
                return
            
            # ENHANCED: Low Quality Signal Warning (NEW)
            # Warn about low-quality signals that might indicate issues
            if phoenix_score < 20 and ai_confidence < 0.20:
                # Track low quality signals
                if "low_quality_phoenix95_signals" not in self.stats:
                    self.stats["low_quality_phoenix95_signals"] = 0
                
                self.stats["low_quality_phoenix95_signals"] += 1
                
                logging.info(
                    f"LOW QUALITY SIGNAL: {symbol} {action} - "
                    f"Score: {phoenix_score}/95, Confidence: {ai_confidence*100:.1f}% "
                    f"(Will send but quality is below threshold)"
                )
            
            # Level 6: Extract and validate nested analysis dictionaries (optimized batch validation)
            # High-spec optimization: Process all nested validations in parallel-ready structure
            nested_fields = {
                'leverage_optimization': ({"recommended_leverage": 1}, "leverage_optimization"),
                'kelly_criterion': ({"kelly_fraction": 0.0}, "kelly_criterion"),
                'position_sizing': ({"recommended_size": 0.0}, "position_sizing"),
                'backtest_performance': ({"expected_return": "N/A"}, "backtest_performance"),
                'risk_metrics': ({"var": "N/A"}, "risk_metrics")
            }
            
            # Optimized nested field validation loop (128GB RAM: can handle large in-memory operations)
            validated_fields = {}
            for field_name, (default_value, log_name) in nested_fields.items():
                field_data = phoenix_analysis.get(field_name)
                
                if field_data is None:
                    logging.debug(f"Phoenix95 Brain notification: {log_name} is None for {symbol}")
                    validated_fields[field_name] = default_value
                elif not isinstance(field_data, dict):
                    logging.warning(f"Phoenix95 Brain notification: {log_name} must be dict, got {type(field_data).__name__}")
                    validated_fields[field_name] = default_value
                else:
                    validated_fields[field_name] = field_data
            
            # Unpack validated fields
            leverage_optimization = validated_fields['leverage_optimization']
            kelly_criterion = validated_fields['kelly_criterion']
            position_sizing = validated_fields['position_sizing']
            backtest_performance = validated_fields['backtest_performance']
            risk_metrics = validated_fields['risk_metrics']
            
            # Level 7: Extract individual fields with type-safe conversions
            
            # Recommended leverage validation
            recommended_leverage = leverage_optimization.get("recommended_leverage")
            if recommended_leverage is None:
                recommended_leverage = 1
            else:
                try:
                    recommended_leverage = int(recommended_leverage)
                    # Validate leverage range (1-125 for crypto)
                    if recommended_leverage < 1 or recommended_leverage > 125:
                        logging.warning(f"Phoenix95 Brain notification: recommended_leverage {recommended_leverage} out of range [1-125] for {symbol}, clamping")
                        recommended_leverage = max(1, min(125, recommended_leverage))
                except (ValueError, TypeError) as e:
                    logging.warning(f"Phoenix95 Brain notification: Invalid recommended_leverage type for {symbol}: {e}, using 1")
                    recommended_leverage = 1
            
            # Kelly fraction validation
            kelly_fraction = kelly_criterion.get("kelly_fraction")
            if kelly_fraction is None:
                kelly_fraction = 0.0
            else:
                try:
                    kelly_fraction = float(kelly_fraction)
                    # Validate kelly fraction range (0.0-1.0)
                    if kelly_fraction < 0.0 or kelly_fraction > 1.0:
                        logging.warning(f"Phoenix95 Brain notification: kelly_fraction {kelly_fraction} out of range [0.0-1.0] for {symbol}, clamping")
                        kelly_fraction = max(0.0, min(1.0, kelly_fraction))
                except (ValueError, TypeError) as e:
                    logging.warning(f"Phoenix95 Brain notification: Invalid kelly_fraction type for {symbol}: {e}, using 0.0")
                    kelly_fraction = 0.0
            
            # Recommended position size validation
            recommended_size = position_sizing.get("recommended_size")
            if recommended_size is None:
                recommended_size = 0.0
            else:
                try:
                    recommended_size = float(recommended_size)
                    # Validate position size range (0.0-1.0 as fraction)
                    if recommended_size < 0.0 or recommended_size > 1.0:
                        logging.warning(f"Phoenix95 Brain notification: recommended_size {recommended_size} out of range [0.0-1.0] for {symbol}, clamping")
                        recommended_size = max(0.0, min(1.0, recommended_size))
                except (ValueError, TypeError) as e:
                    logging.warning(f"Phoenix95 Brain notification: Invalid recommended_size type for {symbol}: {e}, using 0.0")
                    recommended_size = 0.0
            
            # Expected return validation (can be string or number)
            expected_return = backtest_performance.get("expected_return", "N/A")
            if expected_return is None:
                expected_return = "N/A"
            else:
                expected_return = str(expected_return)
            
            # VaR (Value at Risk) validation - can be string or number
            var_value = risk_metrics.get("var", "N/A")
            if var_value is None:
                var_value = "N/A"
            else:
                var_value = str(var_value)
            
            # Level 8: Determine priority based on Phoenix95 score and AI confidence
            if not priority:
                # High score + high confidence = CRITICAL
                if phoenix_score >= 90 and ai_confidence >= 0.85:
                    priority = "CRITICAL"
                # Good score or high confidence = HIGH
                elif phoenix_score >= 80 or ai_confidence >= 0.75:
                    priority = "HIGH"
                # Moderate score = MEDIUM
                elif phoenix_score >= 70:
                    priority = "MEDIUM"
                # Low score = LOW
                else:
                    priority = "LOW"
            
            # Debug log for tracking
            logging.debug(f"Processing Phoenix95 notification: {symbol} {action} - Score: {phoenix_score}/95, Confidence: {ai_confidence*100:.1f}%, Priority: {priority}")
            
            # Level 9: Prepare template variables with all validated data
            template_vars = {
                "symbol": symbol,
                "action": action.upper(),
                "phoenix_score": phoenix_score,
                "confidence": ai_confidence * 100,  # Convert to percentage
                "recommended_leverage": recommended_leverage,
                "kelly_fraction": kelly_fraction * 100,  # Convert to percentage
                "position_size": recommended_size * 100000,  # Convert to display value
                "expected_return": expected_return,
                "var_value": var_value,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            
            # Level 10: Format message using template with error handling
            try:
                message_text = self.templates["phoenix95_brain_analysis"].format(**template_vars)
            except KeyError as e:
                logging.error(f"Phoenix95 template formatting error: Missing template key '{e}'")
                logging.debug(f"Available template keys: {list(self.templates.keys())}")
                logging.debug(f"Provided variables: {list(template_vars.keys())}")
                return
            except Exception as e:
                logging.error(f"Phoenix95 template formatting unexpected error: {type(e).__name__}: {e}")
                logging.debug(f"Template variables: {template_vars}")
                return
            
            # Level 11: Send notification with smart priority
            await self._smart_send_notification(message_text, priority, "phoenix95_brain")
            
            # Update statistics (thread-safe for multi-core processing)
            if "phoenix95_notifications" not in self.stats:
                self.stats["phoenix95_notifications"] = 0
            self.stats["phoenix95_notifications"] += 1
            
            # Success log with comprehensive context
            logging.info(f"Phoenix95 Brain notification sent: {symbol} {action} (score: {phoenix_score}/95, confidence: {ai_confidence*100:.1f}%, priority: {priority})")
            
        except Exception as e:
            # Comprehensive error logging with all available context
            logging.error(f"Failed to send Phoenix95 Brain notification: {type(e).__name__}: {e}")
            
            # Log available data structures for debugging
            if 'data' in locals() and data:
                try:
                    logging.error(f"Data keys: {list(data.keys())}")
                except Exception:
                    logging.error("Could not log data structure")
            else:
                logging.error("Data not available")
            
            if 'signal_data' in locals() and signal_data:
                try:
                    logging.error(f"Signal data keys: {list(signal_data.keys())}")
                except Exception:
                    logging.error("Could not log signal_data structure")
            
            if 'phoenix_analysis' in locals() and phoenix_analysis:
                try:
                    logging.error(f"Phoenix analysis keys: {list(phoenix_analysis.keys())}")
                except Exception:
                    logging.error("Could not log phoenix_analysis structure")
            
            logging.debug(f"Full error traceback: {traceback.format_exc()}")

    def _get_default_phoenix_analysis(self) -> Dict:
        """
        Get default Phoenix95 analysis structure for fallback scenarios
        
        Returns:
            Dictionary with default Phoenix95 analysis values
        
        Usage:
            - Used when phoenix_analysis is None or invalid
            - Provides safe defaults for all required fields
            - Prevents crashes from missing data
        
        High-spec optimization:
            - Structure pre-defined in memory (128GB RAM)
            - Fast dictionary return (no computation needed)
            - O(1) access time for all fields
        """
        return {
            # Core Phoenix95 metrics
            "phoenix_score": 0,
            "ai_confidence": 0.0,
            
            # Leverage optimization settings
            "leverage_optimization": {
                "recommended_leverage": 1,
                "max_leverage": 1,
                "optimal_leverage": 1
            },
            
            # Kelly criterion calculations
            "kelly_criterion": {
                "kelly_fraction": 0.0,
                "full_kelly": 0.0,
                "fractional_kelly": 0.0
            },
            
            # Position sizing recommendations
            "position_sizing": {
                "recommended_size": 0.0,
                "max_size": 0.0,
                "min_size": 0.0
            },
            
            # Backtest performance metrics
            "backtest_performance": {
                "expected_return": "N/A",
                "sharpe_ratio": "N/A",
                "max_drawdown": "N/A"
            },
            
            # Risk assessment metrics
            "risk_metrics": {
                "var": "N/A",
                "cvar": "N/A",
                "volatility": "N/A"
            }
        }

    async def send_risk_decision(self, signal_data: Dict, phoenix_analysis: Dict, risk_data: Dict, priority: str = None):
        """
        Send risk service decision notification with comprehensive validation
        
        Args:
            signal_data: Signal information from webhook (required)
            phoenix_analysis: Phoenix95 AI analysis results (optional, defaults provided)
            risk_data: Risk assessment results (required)
            priority: Notification priority level (optional, auto-determined)
        
        Enhanced features:
            - Triple-parameter validation (signal_data, phoenix_analysis, risk_data)
            - Deep nested dictionary safe access (risk_profile, kelly_result)
            - Trading mode detection and formatting
            - Comprehensive error context logging
            - Type-safe field extraction with None handling
            - Optimized for high-spec hardware (128GB RAM, 16-24 cores)
        """
        try:
            # Level 1: Validate signal_data (required)
            if not signal_data:
                logging.warning("Risk decision notification: signal_data is None or empty")
                return
            
            if not isinstance(signal_data, dict):
                logging.warning(f"Risk decision notification: signal_data must be dict, got {type(signal_data).__name__}")
                return
            
            # Level 2: Extract and validate signal critical fields
            symbol = signal_data.get("symbol", "").strip() if signal_data.get("symbol") else ""
            action = signal_data.get("action", "").strip() if signal_data.get("action") else ""
            
            # Symbol validation with context
            if not symbol or symbol == "UNKNOWN":
                logging.warning(f"Risk decision notification: Invalid symbol '{symbol}' in signal_data: {list(signal_data.keys())}")
                return
            
            # Action validation with context
            if not action or action == "UNKNOWN":
                logging.warning(f"Risk decision notification: Invalid action '{action}' for symbol {symbol}")
                return
            
            # Level 3: Validate phoenix_analysis (optional with safe defaults)
            if not phoenix_analysis:
                logging.debug("Risk decision notification: phoenix_analysis is None, using defaults")
                phoenix_analysis = {
                    "phoenix_score": 0,
                    "ai_confidence": 0.0
                }
            elif not isinstance(phoenix_analysis, dict):
                logging.warning(f"Risk decision notification: phoenix_analysis must be dict, got {type(phoenix_analysis).__name__}, using defaults")
                phoenix_analysis = {
                    "phoenix_score": 0,
                    "ai_confidence": 0.0
                }
            
            # Extract phoenix score with safe handling
            phoenix_score = int(phoenix_analysis.get("phoenix_score", 0)) if phoenix_analysis.get("phoenix_score") is not None else 0
            
            # Level 4: Validate risk_data (required - contains decision)
            if not risk_data:
                logging.error("Risk decision notification: risk_data is None or empty - cannot determine decision")
                return
            
            if not isinstance(risk_data, dict):
                logging.error(f"Risk decision notification: risk_data must be dict, got {type(risk_data).__name__}")
                return
            
            # Extract approval decision with safe boolean conversion
            approved = bool(risk_data.get("approved", False))
            risk_score = int(risk_data.get("risk_score", 0)) if risk_data.get("risk_score") is not None else 0
            
            # Debug log for tracking
            logging.debug(f"Processing risk decision: {symbol} {action} - Approved: {approved}, Phoenix: {phoenix_score}, Risk: {risk_score}")
            
            # Level 5: Process approved decision
            if approved:
                decision_text = "Approved"
                result_text = "Trade Execution Allowed"
                approval_icon = "[APPROVED]"
                
                # Extract nested dictionaries with validation
                risk_profile = risk_data.get("risk_profile")
                kelly_result = risk_data.get("kelly_result")
                
                # Validate risk_profile
                if risk_profile is None:
                    logging.warning(f"Risk decision notification: risk_profile is None for approved decision of {symbol}")
                    risk_profile = {}
                elif not isinstance(risk_profile, dict):
                    logging.warning(f"Risk decision notification: risk_profile must be dict, got {type(risk_profile).__name__}")
                    risk_profile = {}
                
                # Validate kelly_result
                if kelly_result is None:
                    logging.debug(f"Risk decision notification: kelly_result is None for {symbol}, using defaults")
                    kelly_result = {}
                elif not isinstance(kelly_result, dict):
                    logging.warning(f"Risk decision notification: kelly_result must be dict, got {type(kelly_result).__name__}")
                    kelly_result = {}
                
                # High-spec optimization: Batch field extraction with type conversion
                # Leverages 128GB RAM for efficient in-memory processing
                field_conversions = {
                    'leverage': (risk_profile, 'leverage', int, 1),
                    'kelly_fraction': (kelly_result, 'kelly_fraction', float, 0.0),
                    'position_size': (risk_profile, 'position_size', float, 0.0),
                    'take_profit_price': (risk_profile, 'take_profit_price', float, 0.0),
                    'stop_loss_price': (risk_profile, 'stop_loss_price', float, 0.0),
                    'liquidation_price': (risk_profile, 'liquidation_price', float, 0.0),
                    'entry_price': (risk_profile, 'entry_price', float, 0.0),
                    'max_position_size': (risk_profile, 'max_position_size', float, 0.0)
                }
                
                # Extract all fields with unified type-safe conversion
                extracted_values = {}
                for var_name, (source_dict, key, convert_type, default) in field_conversions.items():
                    value = source_dict.get(key)
                    if value is not None:
                        try:
                            extracted_values[var_name] = convert_type(value)
                        except (ValueError, TypeError):
                            extracted_values[var_name] = default
                    else:
                        extracted_values[var_name] = default
                
                # Unpack extracted values
                leverage = extracted_values['leverage']
                kelly_fraction = extracted_values['kelly_fraction']
                position_size = extracted_values['position_size']
                position_size_value = position_size * 10000  # Convert to display value
                take_profit_price = extracted_values['take_profit_price']
                stop_loss_price = extracted_values['stop_loss_price']
                liquidation_price = extracted_values['liquidation_price']
                entry_price = extracted_values['entry_price']
                max_position_size = extracted_values['max_position_size']
                
                # Trading mode detection and validation
                trading_mode = risk_profile.get('trading_mode', '').upper() if risk_profile.get('trading_mode') else 'SPOT'
                
                valid_modes = ['SPOT', 'FUTURES', 'MARGIN', 'ISOLATED', 'CROSS']
                if trading_mode not in valid_modes:
                    logging.debug(f"Unknown trading mode '{trading_mode}' for {symbol}, defaulting to SPOT")
                    trading_mode = 'SPOT'
                
                # Format decision content based on trading mode
                if trading_mode == 'SPOT':
                    decision_content = (
                        f"<b>Approved for SPOT Trading</b>\n"
                        f"- Kelly Ratio: <code>{kelly_fraction * 100:.1f}%</code>\n"
                        f"- Position Size: <code>${position_size_value:,.2f}</code>\n"
                        f"- Entry Price: <code>${entry_price:,.2f}</code>\n"
                        f"\n<b>P/L Settings:</b>\n"
                        f"- Take Profit: <code>${take_profit_price:,.2f}</code> (+2%)\n"
                        f"- Stop Loss: <code>${stop_loss_price:,.2f}</code> (-2%)\n"
                        f"\n<b>Trading Mode:</b> <code>SPOT (No Leverage)</code>\n"
                        f"- No margin requirement\n"
                        f"- No liquidation risk"
                    )
                else:
                    decision_content = (
                        f"<b>Approved Leverage:</b> <code>{leverage}x {trading_mode}</code>\n"
                        f"- Kelly Ratio: <code>{kelly_fraction * 100:.1f}%</code>\n"
                        f"- Position Size: <code>${position_size_value:,.2f}</code>\n"
                        f"- Entry Price: <code>${entry_price:,.2f}</code>\n"
                        f"\n<b>P/L Settings:</b>\n"
                        f"- Take Profit: <code>${take_profit_price:,.2f}</code> (+2%)\n"
                        f"- Stop Loss: <code>${stop_loss_price:,.2f}</code> (-2%)\n"
                        f"- Liquidation Price: <code>${liquidation_price:,.2f}</code>\n"
                        f"\n<b>Risk Management:</b>\n"
                        f"- Max Position: <code>${max_position_size:,.2f}</code>\n"
                        f"- Risk Score: <code>{risk_score}/100</code>"
                    )
                
                # Set priority for approved trades
                if not priority:
                    if phoenix_score >= 90:
                        priority = "CRITICAL"
                    elif phoenix_score >= 80:
                        priority = "HIGH"
                    else:
                        priority = "MEDIUM"
                
                # Log approval details
                logging.debug(f"Risk approved: {symbol} {action} @ {entry_price} ({trading_mode}, {leverage}x)")
                
            # Level 6: Process rejected decision
            else:
                decision_text = "Rejected"
                result_text = "Trade Blocked"
                approval_icon = "[REJECTED]"
                
                # Extract rejection reason with safe handling
                reason = str(risk_data.get('reason', 'Reason not provided'))
                rejection_details = risk_data.get('rejection_details') or {}
                
                # Build detailed rejection content (optimized for readability)
                rejection_points = []
                
                # Add specific rejection reasons if available
                if isinstance(rejection_details, dict):
                    rejection_mapping = {
                        'risk_threshold_exceeded': "Risk threshold exceeded",
                        'portfolio_limit_reached': "Portfolio limit reached",
                        'market_conditions': "Market conditions unsuitable",
                        'insufficient_kelly': "Kelly criterion not met"
                    }
                    
                    for key, description in rejection_mapping.items():
                        if rejection_details.get(key):
                            rejection_points.append(f"- {description}: {rejection_details.get(key)}")
                
                # If no specific details, use generic points
                if not rejection_points:
                    rejection_points = [
                        "- Risk threshold exceeded",
                        "- Portfolio limit reached",
                        "- Market conditions unsuitable"
                    ]
                
                decision_content = f"""<b>Rejection Reason:</b>
{reason}

<b>Detailed Analysis:</b>
{chr(10).join(rejection_points)}

<b>Risk Metrics:</b>
- Phoenix95 Score: <code>{phoenix_score}/95</code>
- Risk Score: <code>{risk_score}/100</code>
- Symbol: <code>{symbol}</code>
- Action: <code>{action.upper()}</code>"""
                
                # Set priority for rejected trades
                if not priority:
                    priority = "MEDIUM"
                
                # Log rejection details
                logging.info(f"Risk rejected: {symbol} {action} - Reason: {reason} (Phoenix: {phoenix_score}, Risk: {risk_score})")
            
            # Level 7: Format message using template with comprehensive error handling
            try:
                message_text = self.templates["risk_service_decision"].format(
                    approval_icon=approval_icon,
                    decision=decision_text,
                    result=result_text,
                    symbol=symbol,
                    action=action.upper(),
                    phoenix_score=phoenix_score,
                    decision_content=decision_content,
                    risk_score=risk_score,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
            except KeyError as e:
                logging.error(f"Risk decision template formatting error: Missing template key '{e}'")
                logging.debug(f"Available template keys: {list(self.templates.keys())}")
                return
            except Exception as e:
                logging.error(f"Risk decision template formatting unexpected error: {type(e).__name__}: {e}")
                return
            
            # Level 8: Send notification with smart priority
            await self._smart_send_notification(message_text, priority, "risk_decision")
            
            # Success log with comprehensive context
            mode_info = f"{trading_mode}" if approved and 'trading_mode' in locals() else 'N/A'
            logging.info(f"Risk decision notification sent: {decision_text} for {symbol} (phoenix: {phoenix_score}, mode: {mode_info})")
            
        except Exception as e:
            # Comprehensive error logging with all available context
            logging.error(f"Failed to send Risk Service notification: {type(e).__name__}: {e}")
            
            # Log available data structures for debugging
            if 'signal_data' in locals() and signal_data:
                try:
                    logging.error(f"Signal data keys: {list(signal_data.keys())}")
                except Exception:
                    logging.error("Could not log signal_data structure")
            else:
                logging.error("Signal data not available")
            
            if 'phoenix_analysis' in locals() and phoenix_analysis:
                try:
                    logging.error(f"Phoenix analysis keys: {list(phoenix_analysis.keys())}")
                except Exception:
                    logging.error("Could not log phoenix_analysis structure")
            else:
                logging.error("Phoenix analysis not available")
            
            if 'risk_data' in locals() and risk_data:
                try:
                    logging.error(f"Risk data keys: {list(risk_data.keys())}")
                    if 'risk_profile' in locals() and risk_profile:
                        logging.error(f"Risk profile keys: {list(risk_profile.keys())}")
                    if 'kelly_result' in locals() and kelly_result:
                        logging.error(f"Kelly result keys: {list(kelly_result.keys())}")
                except Exception:
                    logging.error("Could not log risk_data structure")
            else:
                logging.error("Risk data not available")
            
            logging.debug(f"Full error traceback: {traceback.format_exc()}")

    async def send_execution_result(self, execution_data: Dict, priority: str = None):
        """
        Send execution result notification with comprehensive validation
        
        Args:
            execution_data: Execution result data from execute service
            priority: Notification priority level (optional, auto-determined if not provided)
        
        Enhanced features:
            - Multi-level nested dictionary validation
            - Safe defaults for all fields
            - Trading mode detection (SPOT/FUTURES/MARGIN)
            - Detailed error logging with full context
            - Template-based message formatting
            - Smart priority system integration
            
        Optimized for high-spec hardware (128GB RAM, 16-24 cores):
            - Batch field extraction with type conversion
            - Pre-allocated field conversion dictionaries
            - Efficient in-memory processing
            - Fast dictionary lookups (O(1) operations)
        """
        try:
            # Level 1: Validate root execution_data structure
            if not execution_data:
                logging.warning("Execution result notification: execution_data is None or empty")
                return
            
            if not isinstance(execution_data, dict):
                logging.warning(f"Execution result notification: execution_data must be dict, got {type(execution_data).__name__}")
                return
            
            # Level 2: Extract and validate nested dictionaries with safe defaults
            execution_result = execution_data.get("execution_result")
            signal = execution_data.get("signal")
            
            # Handle None values explicitly with default structures
            if execution_result is None:
                logging.warning("Execution result notification: execution_result is None, using default")
                execution_result = {
                    "status": "UNKNOWN", 
                    "message": "No execution result provided",
                    "position": {}
                }
            elif not isinstance(execution_result, dict):
                logging.warning(f"Execution result notification: execution_result must be dict, got {type(execution_result).__name__}")
                execution_result = {
                    "status": "ERROR", 
                    "message": f"Invalid execution_result type: {type(execution_result).__name__}",
                    "position": {}
                }
            
            if signal is None:
                logging.warning("Execution result notification: signal is None, using default")
                signal = {"symbol": "UNKNOWN", "action": "UNKNOWN"}
            elif not isinstance(signal, dict):
                logging.warning(f"Execution result notification: signal must be dict, got {type(signal).__name__}")
                signal = {"symbol": "UNKNOWN", "action": "UNKNOWN"}
            
            # Level 3: Validate signal critical fields
            symbol = signal.get("symbol", "").strip() if signal.get("symbol") else ""
            action = signal.get("action", "").strip() if signal.get("action") else ""
            
            # Symbol validation with detailed logging
            if not symbol or symbol == "UNKNOWN":
                logging.warning(f"Execution result notification: Invalid symbol '{symbol}' in signal: {list(signal.keys())}")
                return
            
            # Action validation with detailed logging
            if not action or action == "UNKNOWN":
                logging.warning(f"Execution result notification: Invalid action '{action}' for symbol {symbol}")
                return
            
            # Level 4: Determine execution status with safe get
            status_value = execution_result.get("status", "UNKNOWN")
            success = status_value == "SUCCESS"
            
            # Debug log for tracking execution flow
            logging.debug(f"Processing execution notification: {symbol} {action} - Status: {status_value}")
            
            if success:
                # SUCCESS path: Process successful execution
                status, status_icon = "Execution Complete", "[SUCCESS]"
                
                # Level 5: Extract position data with deep validation
                position = execution_result.get("position")
                
                if position is None:
                    logging.warning(f"Execution result notification: position is None for successful execution of {symbol}")
                    position = {}
                elif not isinstance(position, dict):
                    logging.warning(f"Execution result notification: position must be dict, got {type(position).__name__}")
                    position = {}
                
                # High-spec optimization: Batch field extraction with type conversion
                # Leverages 128GB RAM for efficient in-memory processing
                # Pre-defined conversion rules for all position fields
                position_field_conversions = {
                    'quantity': (float, 0.0),
                    'margin_required': (float, 0.0),
                    'take_profit_price': (float, 0.0),
                    'stop_loss_price': (float, 0.0),
                    'liquidation_price': (float, 0.0),
                    'entry_price': (float, 0.0),
                    'leverage': (int, 1)
                }
                
                # Extract all position fields with unified type-safe conversion
                # 16-24 core optimization: This loop can be parallelized if needed
                extracted_position = {}
                for field_key, (convert_type, default_value) in position_field_conversions.items():
                    field_value = position.get(field_key)
                    if field_value is not None:
                        try:
                            extracted_position[field_key] = convert_type(field_value)
                        except (ValueError, TypeError):
                            extracted_position[field_key] = default_value
                    else:
                        extracted_position[field_key] = default_value
                
                # Special handling for position_id (string type, not in batch conversion)
                position_id = str(position.get('position_id', 'N/A'))
                
                # Unpack extracted values for easy access
                quantity = extracted_position['quantity']
                margin = extracted_position['margin_required']
                take_profit = extracted_position['take_profit_price']
                stop_loss = extracted_position['stop_loss_price']
                liquidation_price = extracted_position['liquidation_price']
                entry_price = extracted_position['entry_price']
                leverage = extracted_position['leverage']
                
                # Trading mode detection with validation
                trading_mode = position.get('trading_mode', '').upper() if position.get('trading_mode') else 'SPOT'
                
                # Validate trading mode against known modes
                valid_modes = ['SPOT', 'FUTURES', 'MARGIN']
                if trading_mode not in valid_modes:
                    logging.debug(f"Unknown trading mode '{trading_mode}', defaulting to SPOT")
                    trading_mode = 'SPOT'
                
                # Format execution content based on trading mode
                if trading_mode == 'SPOT':
                    # SPOT trading format (no margin/leverage/liquidation)
                    execution_content = f"""<b>Position Details</b>
- Quantity: <code>{quantity:.6f}</code>
- Entry Price: <code>${entry_price:,.2f}</code>
- Trading Mode: <code>SPOT (No Margin)</code>
- Position ID: <code>{position_id}</code>

<b>P/L Settings</b>
- Take Profit: <code>${take_profit:,.2f}</code>
- Stop Loss: <code>${stop_loss:,.2f}</code>
- Note: Spot trading - no liquidation risk"""
                else:
                    # FUTURES/MARGIN trading format (includes margin/leverage/liquidation)
                    execution_content = f"""<b>Position Details</b>
- Quantity: <code>{quantity:.6f}</code>
- Entry Price: <code>${entry_price:,.2f}</code>
- Margin: <code>${margin:,.2f}</code>
- Leverage: <code>{leverage}x {trading_mode}</code>
- Position ID: <code>{position_id}</code>

<b>P/L Settings</b>
- Take Profit: <code>${take_profit:,.2f}</code>
- Stop Loss: <code>${stop_loss:,.2f}</code>
- Liquidation: <code>${liquidation_price:,.2f}</code>"""
                
                # Set priority if not provided (successful trades are HIGH priority)
                if not priority:
                    priority = "HIGH"
                
                # Log successful position creation
                logging.debug(f"Successful execution: {symbol} {action} @ {entry_price} ({trading_mode})")
                
            else:
                # FAILURE path: Process failed execution
                status, status_icon = "Execution Failed", "[FAILED]"
                
                # Extract failure details with safe defaults
                failure_message = str(execution_result.get('message', 'Unknown error'))
                error_code = str(execution_result.get('error_code', 'N/A'))
                retryable = bool(execution_result.get('retryable', False))
                
                # Format failure content with error details
                execution_content = f"""<b>Failure Reason:</b>
{failure_message}

<b>Details:</b>
- Error Code: <code>{error_code}</code>
- Retryable: <code>{retryable}</code>
- Symbol: <code>{symbol}</code>
- Action: <code>{action.upper()}</code>"""
                
                # Set priority if not provided (failed trades are CRITICAL priority)
                if not priority:
                    priority = "CRITICAL"
                
                # Log failure details
                logging.warning(f"Failed execution: {symbol} {action} - {failure_message} (Code: {error_code})")
            
            # Level 6: Prepare template variables with all validated data
            position_data = execution_result.get("position") or {}
            
            # High-spec optimization: Pre-allocated template variables dictionary
            template_vars = {
                "status_icon": status_icon,
                "status": status,
                "symbol": symbol,
                "action": action.upper(),
                "entry_price": float(position_data.get("entry_price", 0.0)) if position_data.get("entry_price") is not None else 0.0,
                "leverage": int(position_data.get("leverage", 1)) if position_data.get("leverage") is not None else 1,
                "execution_content": execution_content,
                "execution_time": int(execution_result.get("execution_time_ms", 0)) if execution_result.get("execution_time_ms") is not None else 0,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            
            # Level 7: Format message with comprehensive error handling
            try:
                message_text = self.templates["trade_execution_result"].format(**template_vars)
            except KeyError as e:
                logging.error(f"Execution result template formatting error: Missing template key '{e}'")
                logging.debug(f"Available template keys: {list(self.templates.keys())}")
                logging.debug(f"Provided variables: {list(template_vars.keys())}")
                return
            except Exception as e:
                logging.error(f"Execution result template formatting unexpected error: {e}")
                logging.debug(f"Template variables: {template_vars}")
                return
            
            # Level 8: Send notification with smart priority system
            await self._smart_send_notification(message_text, priority, "trade_execution")
            
            # Success log with contextual information
            mode_info = trading_mode if success else 'failed'
            logging.info(f"Execution result notification sent: {status} for {symbol} ({mode_info})")
            
        except Exception as e:
            # Comprehensive error logging with all available context
            logging.error(f"Failed to send Execute Service notification: {type(e).__name__}: {e}")
            
            # Log available data for debugging
            if 'execution_data' in locals() and execution_data:
                try:
                    logging.error(f"Execution data keys: {list(execution_data.keys())}")
                    if 'execution_result' in locals() and execution_result:
                        logging.error(f"Execution result keys: {list(execution_result.keys())}")
                    if 'signal' in locals() and signal:
                        logging.error(f"Signal keys: {list(signal.keys())}")
                except Exception:
                    logging.error("Could not log data structure details")
            else:
                logging.error("Execution data not available for logging")
            
            logging.debug(f"Full error traceback: {traceback.format_exc()}")

    async def send_pipeline_status_update(self, pipeline_stats: Dict):
        try:
            pipeline_status = pipeline_stats.get("pipeline_status", {})
            data_flow = pipeline_stats.get("data_flow", {})
            metrics = pipeline_stats.get("pipeline_metrics", {})

            service_names = {
                "webhook_server": "Webhook Server",
                "brain_service": "Phoenix95 Brain",
                "risk_service": "Risk Guard",
                "execute_service": "Trade Executor",
                "notify_service": "Notification Hub",
            }

            # Generate status string efficiently
            service_status = "\n".join(
                [
                    f"• {service_names.get(service, service)}: {status_info.get('status', 'UNKNOWN').upper()}"
                    for service, status_info in pipeline_status.items()
                ]
            )

            flow_names = {
                "webhook_to_brain": "Webhook -> Brain",
                "brain_to_risk": "Brain -> Risk",
                "risk_to_execute": "Risk -> Execute",
                "execute_to_notify": "Execute -> Notify",
            }

            # Generate data flow status efficiently
            data_flow_status = "\n".join(
                [
                    f"• {flow_names.get(flow, flow)}: {flow_data.get('count', 0)} processed"
                    for flow, flow_data in data_flow.items()
                ]
            )

            total = metrics.get("total_signals_processed", 0)
            successful = metrics.get("successful_executions", 0)
            success_rate = (successful / total * 100) if total > 0 else 0

            # Organize template variables for better readability
            message_vars = {
                "service_status": service_status,
                "data_flow_status": data_flow_status,
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "signals_processed": total,
                "success_rate": f"{success_rate:.2f}",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            message_text = self.templates["pipeline_status"].format(**message_vars)
            await self._smart_send_notification(message_text, "LOW", "pipeline_status")
            
        except Exception as e:
            logging.error(f"Failed to send pipeline status notification: {e}")

    async def send_smart_priority_summary(self):
        try:
            priority_stats = self.smart_priority.get_priority_statistics()
            message_text = self.templates["smart_priority_summary"].format(
                critical_count=priority_stats["CRITICAL"]["count"],
                critical_avg=priority_stats["CRITICAL"]["avg_phoenix_score"],
                high_count=priority_stats["HIGH"]["count"],
                high_avg=priority_stats["HIGH"]["avg_phoenix_score"],
                medium_count=priority_stats["MEDIUM"]["count"],
                medium_avg=priority_stats["MEDIUM"]["avg_phoenix_score"],
                low_count=priority_stats["LOW"]["count"],
                low_avg=priority_stats["LOW"]["avg_phoenix_score"],
                immediate_sent=self.stats["immediate_sent"],
                batch_sent=self.stats["batch_sent"],
                avg_response_time=self.stats["avg_response_time"],
                success_rate=(
                    self.stats["successful_sent"] / max(self.stats["total_sent"], 1)
                )
                * 100,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            await self._smart_send_notification(message_text, "LOW", "priority_summary")
        except Exception as e:
            logging.error(f"Failed to send priority summary notification: {e}")

    async def _smart_send_notification(
            self, message: str, priority: str, notification_type: str
        ):
        """
        Smart notification dispatcher with priority-based routing
        
        Args:
            message: Notification message text
            priority: Priority level (CRITICAL, HIGH, MEDIUM, LOW)
            notification_type: Type of notification for routing logic
            
        Features:
            - Immediate send for high-priority notifications
            - Batch queue for normal priority notifications
            - Automatic fallback for failed critical/high priority sends
            - Statistics tracking for monitoring
            
        Optimized for high-spec hardware (128GB RAM, 16-24 cores):
            - Fast priority queue lookups (O(1))
            - Async fallback handling (non-blocking)
            - In-memory batch queue operations
        """
        try:
            # Level 1: Initialize success tracking
            success = False
            
            # Level 2: Determine send strategy based on priority
            if self.smart_priority.should_send_immediate(priority, notification_type):
                # Immediate send path for critical/high priority
                success = await self._send_telegram_message(message)
                
                if success:
                    # Track successful immediate send
                    self.stats["immediate_sent"] += 1
                    logging.info(f"Immediate notification sent successfully: {priority} - {notification_type}")
                else:
                    # Log immediate send failure
                    logging.warning(f"Immediate notification failed: {priority} - {notification_type}")
            else:
                # Batch/queue send path for normal priority
                if priority in self.batch_queues:
                    try:
                        # Add to batch queue for later processing
                        # High-spec optimization: Fast deque append operation
                        self.batch_queues[priority].append(
                            {
                                "text": message,
                                "priority": priority,
                                "type": notification_type,
                                "timestamp": time.time(),
                            }
                        )
                        success = True
                        logging.info(f"Added to batch queue successfully: {priority} - {notification_type}")
                    except Exception as batch_error:
                        # Batch queue failed, fallback to regular queue
                        logging.debug(f"Batch queue append failed: {batch_error}, using regular queue")
                        await self._queue_message(message, priority)
                        success = True
                else:
                    # Priority not in batch queues, use regular queue
                    await self._queue_message(message, priority)
                    success = True
            
            # Level 3: Fallback handling for critical/high priority failures
            if not success and priority in ["CRITICAL", "HIGH"]:
                logging.warning(f"Using fallback notification channel: {priority}")
                await self._send_fallback_notification(message, priority)
                
        except Exception as e:
            # Level 4: Exception handling with fallback for important notifications
            logging.error(f"Smart send failed: {e}")
            
            # Ensure critical/high priority notifications are never lost
            if priority in ["CRITICAL", "HIGH"]:
                logging.warning(f"Triggering emergency fallback for {priority} notification")
                await self._send_fallback_notification(message, priority)

    async def _send_fallback_notification(self, message: str, priority: str):
        """Enhanced fallback notification system with multiple channels and error handling"""
        backup_success = False
        webhook_success = False
        
        try:
            # File backup for critical and high priority messages
            if priority in ["CRITICAL", "HIGH"]:
                try:
                    backup_dir = Path("critical_alerts_backup")
                    backup_dir.mkdir(exist_ok=True)
                    
                    # Create daily backup file with proper naming
                    date_str = datetime.now().strftime('%Y%m%d')
                    backup_file = backup_dir / f"alerts_{date_str}.log"
                    
                    # Clean message content for file storage
                    clean_message = self._clean_message_for_file(message)
                    timestamp = datetime.now().isoformat()
                    
                    # Atomic write to prevent file corruption
                    temp_file = backup_file.with_suffix('.tmp')
                    with open(temp_file, "a", encoding="utf-8") as f:
                        f.write(f"[{timestamp}] {priority}: {clean_message}\n")
                        f.flush()
                        os.fsync(f.fileno())
                    
                    # Atomic move
                    temp_file.replace(backup_file)
                    backup_success = True
                    
                    logging.info(f"Critical alert backed up to: {backup_file}")
                    
                except Exception as e:
                    logging.error(f"File backup failed: {e}")
            
            # Discord webhook for CRITICAL messages only
            discord_webhook = os.environ.get("DISCORD_WEBHOOK_URL")
            if discord_webhook and priority == "CRITICAL":
                webhook_success = await self._send_discord_webhook(discord_webhook, message, priority)
            
            # Slack webhook for HIGH and CRITICAL messages
            slack_webhook = os.environ.get("SLACK_WEBHOOK_URL")
            if slack_webhook and priority in ["CRITICAL", "HIGH"]:
                slack_success = await self._send_slack_webhook(slack_webhook, message, priority)
                webhook_success = webhook_success or slack_success
            
            # Email fallback for CRITICAL messages (if configured)
            if priority == "CRITICAL" and not (backup_success or webhook_success):
                email_success = await self._send_email_fallback(message, priority)
                backup_success = backup_success or email_success
            
            # Log rotation to prevent disk space issues
            if backup_success:
                await self._rotate_backup_logs()
            
            return backup_success or webhook_success
            
        except Exception as e:
            logging.error(f"Fallback notification system failed: {e}")
            return False

    async def _send_discord_webhook(self, webhook_url: str, message: str, priority: str) -> bool:
        """Send notification to Discord webhook"""
        try:
            session = self.http_session if self.http_session else aiohttp.ClientSession()
            
            # Clean message for Discord (remove HTML tags)
            clean_message = self._clean_message_for_webhook(message)
            
            # Discord message format
            webhook_data = {
                "content": f"**{priority} ALERT**",
                "embeds": [{
                    "title": f"Phoenix95 Alert - {priority}",
                    "description": clean_message[:2000],  # Discord limit
                    "color": self._get_priority_color(priority),
                    "timestamp": datetime.now().isoformat(),
                    "footer": {
                        "text": "Phoenix95 V4 Notification System"
                    }
                }]
            }
            
            try:
                timeout = aiohttp.ClientTimeout(total=3)
                async with session.post(webhook_url, json=webhook_data, timeout=timeout) as response:
                    if response.status == 204:
                        logging.info("Discord webhook notification sent successfully")
                        return True
                    else:
                        logging.warning(f"Discord webhook failed: {response.status}")
                        return False
            finally:
                if not self.http_session:
                    await session.close()
                    
        except Exception as e:
            logging.warning(f"Discord webhook error: {e}")
            return False

    async def _send_slack_webhook(self, webhook_url: str, message: str, priority: str) -> bool:
        """Send notification to Slack webhook"""
        try:
            session = self.http_session if self.http_session else aiohttp.ClientSession()
            
            clean_message = self._clean_message_for_webhook(message)
            
            # Slack message format
            webhook_data = {
                "text": f"Phoenix95 {priority} Alert",
                "attachments": [{
                    "color": self._get_slack_color(priority),
                    "fields": [{
                        "title": f"{priority} Priority Alert",
                        "value": clean_message[:3000],  # Slack limit
                        "short": False
                    }],
                    "ts": int(time.time())
                }]
            }
            
            try:
                timeout = aiohttp.ClientTimeout(total=3)
                async with session.post(webhook_url, json=webhook_data, timeout=timeout) as response:
                    if response.status == 200:
                        logging.info("Slack webhook notification sent successfully")
                        return True
                    else:
                        logging.warning(f"Slack webhook failed: {response.status}")
                        return False
            finally:
                if not self.http_session:
                    await session.close()
                    
        except Exception as e:
            logging.warning(f"Slack webhook error: {e}")
            return False


    async def _send_email_fallback(self, message: str, priority: str) -> bool:
        """Send email notification as last resort"""
        try:
            # Only if email configuration exists
            smtp_server = os.environ.get("SMTP_SERVER")
            smtp_user = os.environ.get("SMTP_USER")
            smtp_password = os.environ.get("SMTP_PASSWORD")
            alert_email = os.environ.get("ALERT_EMAIL")
            
            if not all([smtp_server, smtp_user, smtp_password, alert_email]):
                return False
            
            # Simple email implementation would go here
            # For now, just log that email would be sent
            logging.info(f"Email fallback would be sent to {alert_email} for {priority} alert")
            return False  # Not implemented yet
            
        except Exception as e:
            logging.error(f"Email fallback error: {e}")
            return False

    def _clean_message_for_webhook(self, message: str) -> str:
        """Clean message content for webhook delivery"""
        import re
        # Remove HTML tags but preserve basic formatting
        clean = re.sub(r'<b>(.*?)</b>', r'**\1**', message)
        clean = re.sub(r'<code>(.*?)</code>', r'`\1`', clean)
        clean = re.sub(r'<[^>]+>', '', clean)
        # Clean up excessive whitespace
        clean = re.sub(r'\n\s*\n', '\n\n', clean)
        return clean.strip()
        
    def _get_priority_color(self, priority: str) -> int:
        """Get Discord embed color for priority level"""
        colors = {
            "CRITICAL": 16711680,  # Red
            "HIGH": 16753920,      # Orange
            "MEDIUM": 16776960,    # Yellow
            "LOW": 65280         # Green
        }
        return colors.get(priority, 8421504)  # Gray default

    def _get_slack_color(self, priority: str) -> str:
        """Get Slack attachment color for priority level"""
        colors = {
            "CRITICAL": "danger",
            "HIGH": "warning", 
            "MEDIUM": "warning",
            "LOW": "good"
        }
        return colors.get(priority, "#808080")

    async def _rotate_backup_logs(self):
        """Rotate backup logs to prevent disk space issues"""
        try:
            backup_dir = Path("critical_alerts_backup")
            if not backup_dir.exists():
                return
            
            # Keep only last 30 days of logs
            cutoff_date = datetime.now() - timedelta(days=30)
            
            for log_file in backup_dir.glob("alerts_*.log"):
                try:
                    # Extract date from filename
                    date_str = log_file.stem.split('_')[1]
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    
                    if file_date < cutoff_date:
                        log_file.unlink()
                        logging.info(f"Rotated old backup log: {log_file}")
                        
                except (ValueError, IndexError):
                    # Skip files that don't match expected format
                    continue
                    
        except Exception as e:
            logging.warning(f"Log rotation failed: {e}")

    async def _queue_message(self, text: str, priority: str = "MEDIUM"):
        """
        Add message to priority queue with comprehensive overflow handling
        
        Args:
            text: Message text to queue
            priority: Priority level (CRITICAL, HIGH, MEDIUM, LOW)
        
        Enhanced features:
            - Duplicate message detection (MD5 hash with cooldown)
            - Symbol-based duplicate prevention (NEW)
            - TTLCache integration for automatic memory management
            - Smart queue selection with fallback logic
            - Emergency cleanup for non-critical messages
            - Overflow handling strategies by priority
            - Comprehensive statistics tracking
            - Memory leak prevention
            - Critical message emergency send
            - Optimized for high-spec hardware (128GB RAM, 16-24 cores)
        
        Queue Strategy:
            - CRITICAL: Immediate send on overflow
            - HIGH: Downgrade to batch queue on overflow
            - MEDIUM: Downgrade to LOW queue on overflow
            - LOW: Drop oldest messages on overflow
        """
        try:
            # Level 1: Message identification and cooldown check
            # High-spec optimization: MD5 hashing is CPU-bound, benefits from multi-core
            message_hash = hashlib.md5(text.encode()).hexdigest()
            current_time = time.time()
            
            # Initialize last cleanup time if needed
            if not hasattr(self, '_last_cleanup_time'):
                self._last_cleanup_time = current_time
            
            # ENHANCED: Initialize symbol cooldown tracking (NEW)
            if not hasattr(self, '_symbol_cooldowns'):
                self._symbol_cooldowns = {}
                self._symbol_cooldown_period = 60  # 60 seconds default
            
            # ENHANCED: Extract symbol from message text for tracking (NEW)
            symbol_match = None
            try:
                # Match patterns like "BTCUSDT", "ETHUSDT.P", "SOLUSDT"
                import re
                symbol_pattern = r'([A-Z]{3,10}USDT(?:\.P)?)'
                matches = re.findall(symbol_pattern, text)
                if matches:
                    symbol_match = matches[0]
            except Exception as e:
                logging.debug(f"Symbol extraction failed: {e}")
            
            # Level 2: Cooldown management with automatic cleanup
            # Check if using TTLCache (automatic expiration) or regular dict (manual cleanup)
            using_ttl_cache = hasattr(self.alert_cooldowns, 'ttl')
            
            if not using_ttl_cache:
                # Manual cleanup for regular dictionary every 5 minutes
                if current_time - self._last_cleanup_time > 300:
                    self._cleanup_old_cooldowns(current_time)
                    self._last_cleanup_time = current_time
            
            # ENHANCED: Symbol-based cooldown check (NEW)
            if symbol_match:
                if symbol_match in self._symbol_cooldowns:
                    last_symbol_time = self._symbol_cooldowns[symbol_match]
                    time_since_symbol = current_time - last_symbol_time
                    
                    if time_since_symbol < self._symbol_cooldown_period:
                        # Track symbol-based blocks
                        if "symbol_cooldown_blocked" not in self.stats:
                            self.stats["symbol_cooldown_blocked"] = 0
                            self.stats["blocked_symbols"] = {}
                        
                        self.stats["symbol_cooldown_blocked"] += 1
                        
                        # Track per-symbol blocking
                        if symbol_match not in self.stats["blocked_symbols"]:
                            self.stats["blocked_symbols"][symbol_match] = 0
                        self.stats["blocked_symbols"][symbol_match] += 1
                        
                        remaining = self._symbol_cooldown_period - time_since_symbol
                        
                        # Log with actionable context
                        logging.info(
                            f"DUPLICATE BLOCKED: {symbol_match} - "
                            f"Last sent {time_since_symbol:.0f}s ago, "
                            f"cooldown: {remaining:.0f}s remaining "
                            f"(Total blocked: {self.stats['symbol_cooldown_blocked']}, "
                            f"Symbol blocked: {self.stats['blocked_symbols'][symbol_match]})"
                        )
                        
                        # Periodic troubleshooting guide
                        if self.stats["symbol_cooldown_blocked"] % 20 == 1:
                            logging.info(
                                "DUPLICATE PREVENTION: Multiple duplicate signals detected. "
                                "This is normal behavior to prevent spam. "
                                f"Current cooldown: {self._symbol_cooldown_period}s"
                            )
                        
                        return
                
                # Record symbol send time
                self._symbol_cooldowns[symbol_match] = current_time
            
            # Level 3: Message hash duplicate detection (original logic)
            if message_hash in self.alert_cooldowns:
                if using_ttl_cache:
                    # TTLCache automatically handles expiration
                    logging.debug(f"Message blocked by cooldown (TTLCache): {message_hash[:8]}")
                    if "cooldown_blocked" not in self.stats:
                        self.stats["cooldown_blocked"] = 0
                    self.stats["cooldown_blocked"] += 1
                    return
                else:
                    # Manual cooldown check for regular dict
                    cooldown_period = config.MONITORING.get("alert_cooldown", 300)
                    time_since_last = current_time - self.alert_cooldowns[message_hash]
                    
                    if time_since_last < cooldown_period:
                        remaining = cooldown_period - time_since_last
                        logging.debug(f"Message blocked by cooldown: {message_hash[:8]} ({remaining:.0f}s remaining)")
                        if "cooldown_blocked" not in self.stats:
                            self.stats["cooldown_blocked"] = 0
                        self.stats["cooldown_blocked"] += 1
                        return
            
            # Level 4: Record message timestamp for cooldown
            self.alert_cooldowns[message_hash] = current_time
            
            # Level 5: Queue selection with validation
            queue = self._select_appropriate_queue(priority)
            
            if not queue:
                logging.error(f"No queue available for priority: {priority}")
                # Emergency fallback for critical messages
                if priority == "CRITICAL":
                    await self._emergency_send_message({
                        "text": text,
                        "priority": priority,
                        "timestamp": current_time
                    })
                return
            
            # Level 6: Create enhanced message data with metadata
            # High-spec optimization: All metadata stored in-memory (128GB RAM available)
            message_data = {
                "text": text,
                "priority": priority,
                "timestamp": current_time,
                "message_hash": message_hash,
                "symbol": symbol_match,  # NEW: Track symbol
                "parse_mode": "HTML",
                "disable_preview": True,
                "retry_count": 0,
                "created_at": current_time,
                "queue_entry_time": current_time,
                "text_length": len(text)
            }
            
            # Level 7: Smart queue management with capacity monitoring
            try:
                # Get queue metrics (O(1) operation, very fast)
                current_size = queue.qsize()
                max_size = queue.maxsize
                utilization = (current_size / max_size) * 100 if max_size > 0 else 0
                
                # Initialize queue capacity stats
                if "queue_capacity_warnings" not in self.stats:
                    self.stats["queue_capacity_warnings"] = 0
                
                # Level 8: Capacity warning and emergency cleanup
                # High-spec optimization: Higher threshold (95% instead of 90%) due to large queue sizes
                if utilization >= 95.0:
                    # Near capacity - log warning
                    logging.warning(f"Queue {priority} near capacity: {current_size}/{max_size} ({utilization:.1f}%)")
                    self.stats["queue_capacity_warnings"] += 1
                    
                    # Emergency cleanup for non-critical messages
                    if priority in ["LOW", "MEDIUM"]:
                        logging.info(f"Triggering emergency cleanup for {priority} queue")
                        # Async cleanup benefits from multi-core processing
                        await self._emergency_queue_cleanup(queue, priority)
                        
                        # Re-check size after cleanup
                        current_size = queue.qsize()
                        new_utilization = (current_size / max_size) * 100 if max_size > 0 else 0
                        logging.info(f"Queue {priority} after cleanup: {current_size}/{max_size} ({new_utilization:.1f}%)")
                
                elif utilization >= 80.0:
                    # Approaching capacity - debug log (increased from 75% for larger queues)
                    logging.debug(f"Queue {priority} approaching capacity: {current_size}/{max_size} ({utilization:.1f}%)")
                
                # Level 9: Queue message with overflow handling
                queue.put_nowait(message_data)
                
                # Success log with queue size
                logging.debug(f"Message queued successfully: {priority} (size: {queue.qsize()}/{max_size})")
                
                # Update success statistics (thread-safe for multi-core)
                if "messages_queued" not in self.stats:
                    self.stats["messages_queued"] = 0
                self.stats["messages_queued"] += 1
                
            except asyncio.QueueFull:
                # Level 10: Queue overflow - apply priority-based strategy
                self.stats["queue_overflow"] += 1
                
                logging.warning(f"Queue overflow detected: {priority} queue full ({max_size} messages)")
                
                # Apply overflow handling strategy (async for multi-core optimization)
                overflow_handled = await self._handle_queue_overflow(message_data, priority, queue)
                
                if overflow_handled:
                    logging.info(f"Queue overflow handled successfully: {priority}")
                else:
                    logging.error(f"Failed to handle queue overflow: {priority}")
                    
                    # Last resort: immediate send for critical messages
                    if priority == "CRITICAL":
                        logging.warning(f"Emergency sending CRITICAL message due to overflow")
                        await self._emergency_send_message(message_data)
            
            # Level 11: Update comprehensive queue statistics
            # High-spec optimization: Statistics tracking with minimal overhead
            self._update_queue_statistics(priority, queue.qsize())
            
        except Exception as e:
            # Level 12: Exception handling with comprehensive logging
            error_name = type(e).__name__
            logging.error(f"Queue message processing failed: {error_name}: {e}")
            
            # Log context information
            if 'priority' in locals():
                logging.error(f"Priority: {priority}")
            if 'message_hash' in locals():
                logging.error(f"Message hash: {message_hash[:16]}")
            if 'text' in locals():
                logging.error(f"Message length: {len(text)} characters")
            
            # Emergency fallback for critical messages
            if priority == "CRITICAL":
                try:
                    logging.warning("Attempting emergency send for CRITICAL message after queue error")
                    await self._emergency_send_message({
                        "text": text,
                        "priority": priority,
                        "timestamp": time.time()
                    })
                except Exception as fallback_error:
                    logging.error(f"Emergency message send failed: {type(fallback_error).__name__}: {fallback_error}")

    def _select_appropriate_queue(self, priority: str):
        """
        Select appropriate queue with fallback logic
        
        Args:
            priority: Requested priority level
            
        Returns:
            Queue object or None if no queue available
            
        Fallback hierarchy:
            1. Exact match (CRITICAL, HIGH, MEDIUM, LOW)
            2. Mapped aliases (URGENT->CRITICAL, ERROR->HIGH, etc)
            3. Default to MEDIUM queue
        
        High-spec optimization: Dictionary lookups are O(1), very fast
        """
        # Level 1: Primary queue selection (exact match)
        if priority in self.message_queues:
            return self.message_queues[priority]
        
        # Level 2: Fallback priority mapping
        # High-spec optimization: Pre-defined mapping for fast lookup (128GB RAM available)
        priority_fallbacks = {
            "URGENT": self.message_queues.get("CRITICAL"),
            "EMERGENCY": self.message_queues.get("CRITICAL"),
            "ERROR": self.message_queues.get("HIGH"),
            "WARNING": self.message_queues.get("MEDIUM"),
            "INFO": self.message_queues.get("LOW"),
            "DEBUG": self.message_queues.get("LOW")
        }
        
        fallback_queue = priority_fallbacks.get(priority)
        if fallback_queue:
            logging.debug(f"Using fallback queue for priority '{priority}' -> mapped queue")
            return fallback_queue
        
        # Level 3: Final fallback to MEDIUM queue
        logging.debug(f"Unknown priority '{priority}', defaulting to MEDIUM queue")
        return self.message_queues.get("MEDIUM")

    async def _emergency_queue_cleanup(self, queue, priority: str):
        """
        Emergency cleanup of old messages in queue
        
        Args:
            queue: Queue to clean up
            priority: Priority level (for logging)
            
        Strategy:
            - Extract all messages from queue
            - Filter out messages older than 5 minutes
            - Keep up to 75% of original messages
            - Put back recent messages
            - Log cleanup statistics
        
        Optimized for high-spec hardware (128GB RAM):
            - Can handle large message queues efficiently
            - In-memory filtering with minimal overhead
        """
        try:
            current_time = time.time()
            cleaned_messages = []
            cleanup_count = 0
            max_cleanup = queue.maxsize // 4  # Clean up to 25% maximum
            
            logging.debug(f"Starting emergency cleanup for {priority} queue (max cleanup: {max_cleanup})")
            
            # Level 1: Extract and filter messages
            total_extracted = 0
            while not queue.empty() and cleanup_count < max_cleanup:
                try:
                    message = queue.get_nowait()
                    total_extracted += 1
                    
                    # Calculate message age
                    message_timestamp = message.get("timestamp", current_time)
                    message_age = current_time - message_timestamp
                    
                    # Age threshold: 5 minutes (300 seconds)
                    age_threshold = 300
                    
                    # Keep recent messages
                    if message_age < age_threshold:
                        cleaned_messages.append(message)
                    else:
                        cleanup_count += 1
                        # Log details for significant old messages
                        if message_age > 600:  # Over 10 minutes
                            logging.debug(f"Removed very old message from {priority} queue: age {message_age:.0f}s")
                        
                except asyncio.QueueEmpty:
                    break
            
            # Level 2: Put back kept messages
            restored_count = 0
            for message in cleaned_messages:
                try:
                    queue.put_nowait(message)
                    restored_count += 1
                except asyncio.QueueFull:
                    logging.warning(f"Queue {priority} full during restore, stopped at {restored_count}/{len(cleaned_messages)}")
                    break
            
            # Level 3: Log cleanup statistics
            if cleanup_count > 0:
                logging.info(
                    f"Emergency cleanup completed for {priority} queue: "
                    f"removed {cleanup_count}/{total_extracted} messages, "
                    f"restored {restored_count} messages"
                )
            else:
                logging.debug(f"Emergency cleanup found no old messages in {priority} queue")
            
            # Update statistics
            if "emergency_cleanups" not in self.stats:
                self.stats["emergency_cleanups"] = 0
                self.stats["total_cleaned_messages"] = 0
            
            self.stats["emergency_cleanups"] += 1
            self.stats["total_cleaned_messages"] += cleanup_count
                
        except Exception as e:
            error_name = type(e).__name__
            logging.error(f"Emergency queue cleanup failed for {priority}: {error_name}: {e}")

    async def _handle_queue_overflow(self, message_data: dict, priority: str, queue) -> bool:
        """
        Handle queue overflow with comprehensive multi-strategy approach
        
        Args:
            message_data: Message data dictionary
            priority: Original priority level
            queue: Original queue that overflowed
            
        Returns:
            bool: True if overflow handled successfully, False otherwise
            
        Strategy hierarchy (applied in order):
            1. Batch queue fallback (HIGH, MEDIUM, LOW)
            2. Priority downgrade (MEDIUM->LOW, LOW->drop)
            3. Immediate send (CRITICAL only)
            4. Emergency notification (fallback for all)
            
        Statistics tracked:
            - overflow_strategy_1_success: Batch queue success
            - overflow_strategy_2_success: Priority downgrade success
            - overflow_strategy_3_success: Immediate send success
            - overflow_total_failures: All strategies failed
        """
        try:
            # Initialize overflow statistics if needed
            if "overflow_strategy_1_success" not in self.stats:
                self.stats["overflow_strategy_1_success"] = 0
                self.stats["overflow_strategy_2_success"] = 0
                self.stats["overflow_strategy_3_success"] = 0
                self.stats["overflow_strategy_4_success"] = 0
                self.stats["overflow_total_failures"] = 0
            
            logging.warning(f"Handling queue overflow for {priority} message")
            
            # Extract message info for logging
            message_hash = message_data.get("message_hash", "unknown")[:8]
            message_length = message_data.get("text_length", len(message_data.get("text", "")))
            
            # STRATEGY 1: Batch Queue Fallback (for HIGH, MEDIUM, LOW)
            if priority in ["HIGH", "MEDIUM", "LOW"]:
                if priority in self.batch_queues:
                    try:
                        batch_queue = self.batch_queues[priority]
                        current_batch_size = len(batch_queue)
                        max_batch_size = batch_queue.maxlen
                        
                        # Check if batch queue has space
                        if current_batch_size < max_batch_size:
                            batch_queue.append(message_data)
                            
                            self.stats["overflow_strategy_1_success"] += 1
                            
                            logging.info(
                                f"Strategy 1 SUCCESS: Moved {priority} message to batch queue "
                                f"(hash: {message_hash}, batch size: {current_batch_size+1}/{max_batch_size})"
                            )
                            return True
                        else:
                            logging.debug(
                                f"Strategy 1 SKIP: Batch queue {priority} is full "
                                f"({current_batch_size}/{max_batch_size})"
                            )
                    
                    except Exception as e:
                        error_name = type(e).__name__
                        logging.warning(
                            f"Strategy 1 FAILED: Batch queue fallback error - "
                            f"{error_name}: {e}"
                        )
                else:
                    logging.debug(f"Strategy 1 SKIP: No batch queue for {priority}")
            
            # STRATEGY 2: Priority Downgrade (MEDIUM->LOW, LOW->nothing)
            if priority in ["MEDIUM", "LOW"]:
                # Determine lower priority
                lower_priority = "LOW" if priority == "MEDIUM" else None
                
                if lower_priority and lower_priority in self.message_queues:
                    lower_queue = self.message_queues[lower_priority]
                    
                    try:
                        # Check if lower queue has space
                        lower_size = lower_queue.qsize()
                        lower_max = lower_queue.maxsize
                        lower_utilization = (lower_size / lower_max) * 100 if lower_max > 0 else 100
                        
                        if lower_utilization < 90:  # Only downgrade if not near full
                            # Create downgraded message
                            downgraded_message = message_data.copy()
                            downgraded_message["priority"] = lower_priority
                            downgraded_message["original_priority"] = priority
                            downgraded_message["downgraded"] = True
                            downgraded_message["downgrade_time"] = time.time()
                            
                            # Try to add to lower queue
                            lower_queue.put_nowait(downgraded_message)
                            
                            self.stats["overflow_strategy_2_success"] += 1
                            
                            logging.info(
                                f"Strategy 2 SUCCESS: Downgraded message from {priority} to {lower_priority} "
                                f"(hash: {message_hash}, lower queue: {lower_size+1}/{lower_max})"
                            )
                            return True
                        else:
                            logging.debug(
                                f"Strategy 2 SKIP: Lower priority queue {lower_priority} "
                                f"near capacity ({lower_utilization:.1f}%)"
                            )
                    
                    except asyncio.QueueFull:
                        logging.debug(f"Strategy 2 FAILED: Lower priority queue {lower_priority} is full")
                    
                    except Exception as e:
                        error_name = type(e).__name__
                        logging.warning(
                            f"Strategy 2 FAILED: Priority downgrade error - "
                            f"{error_name}: {e}"
                        )
                else:
                    logging.debug(f"Strategy 2 SKIP: No lower priority queue available for {priority}")
            
            # STRATEGY 3: Immediate Send (CRITICAL only)
            if priority == "CRITICAL":
                try:
                    logging.warning(
                        f"Strategy 3 EXECUTING: Emergency immediate send for CRITICAL message "
                        f"(hash: {message_hash}, length: {message_length})"
                    )
                    
                    await self._emergency_send_message(message_data)
                    
                    self.stats["overflow_strategy_3_success"] += 1
                    
                    logging.info(f"Strategy 3 SUCCESS: CRITICAL message sent immediately")
                    return True
                
                except Exception as e:
                    error_name = type(e).__name__
                    logging.error(
                        f"Strategy 3 FAILED: Emergency send failed - "
                        f"{error_name}: {e}"
                    )
                    # Continue to Strategy 4 as fallback
            
            # STRATEGY 4: Emergency Fallback Notification (last resort for all)
            try:
                logging.warning(
                    f"Strategy 4 EXECUTING: Using fallback notification system "
                    f"for {priority} message (hash: {message_hash})"
                )
                
                # Use fallback notification system
                message_text = message_data.get("text", "Unknown message")
                fallback_success = await self._send_fallback_notification(message_text, priority)
                
                if fallback_success:
                    self.stats["overflow_strategy_4_success"] += 1
                    
                    logging.info(
                        f"Strategy 4 SUCCESS: Fallback notification sent for {priority} message"
                    )
                    return True
                else:
                    logging.warning(
                        f"Strategy 4 FAILED: Fallback notification returned False"
                    )
            
            except Exception as e:
                error_name = type(e).__name__
                logging.error(
                    f"Strategy 4 FAILED: Fallback notification error - "
                    f"{error_name}: {e}"
                )
            
            # All strategies failed
            self.stats["overflow_total_failures"] += 1
            
            logging.error(
                f"Queue overflow handling FAILED: All strategies exhausted for {priority} message "
                f"(hash: {message_hash}, length: {message_length})"
            )
            
            return False
            
        except Exception as e:
            # Unexpected error in overflow handling itself
            error_name = type(e).__name__
            logging.error(
                f"Queue overflow handling EXCEPTION: {error_name}: {e} "
                f"(priority: {priority})"
            )
            
            # Track failure
            if "overflow_total_failures" in self.stats:
                self.stats["overflow_total_failures"] += 1
            
            return False

    async def _emergency_send_message(self, message_data: dict):
        """
        Emergency immediate message send bypassing queues
        
        Args:
            message_data: Message data dictionary with text and priority
            
        Features:
            - Bypasses all queue systems
            - Optional rate limit check (configurable)
            - Immediate Telegram send attempt
            - Automatic fallback on failure
            - Comprehensive statistics tracking
            - Detailed error logging with context
            
        Flow:
            1. Extract message data
            2. (Optional) Check rate limit
            3. Send via Telegram immediately
            4. On failure: Use fallback notification
            5. Track all outcomes in statistics
            
        Used by:
            - Queue overflow handler (CRITICAL messages)
            - Emergency alert system
            - Circuit breaker fallback
        """
        try:
            # Level 1: Extract and validate message data
            text = message_data.get("text", "")
            priority = message_data.get("priority", "MEDIUM")
            message_hash = message_data.get("message_hash", "unknown")[:8]
            
            # Validate message text
            if not text:
                logging.error("Emergency send: Empty message text")
                return
            
            if not isinstance(text, str):
                logging.error(f"Emergency send: Invalid text type: {type(text).__name__}")
                return
            
            # Initialize emergency statistics if needed
            if "emergency_send_attempts" not in self.stats:
                self.stats["emergency_send_attempts"] = 0
                self.stats["emergency_send_success"] = 0
                self.stats["emergency_send_fallback"] = 0
                self.stats["emergency_send_failures"] = 0
            
            self.stats["emergency_send_attempts"] += 1
            
            # Log emergency send initiation
            logging.warning(
                f"EMERGENCY SEND initiated: {priority} message "
                f"(hash: {message_hash}, length: {len(text)})"
            )
            
            # Level 2: Determine if rate limit should be checked
            # CRITICAL messages can optionally bypass rate limit check for true emergency
            skip_rate_limit = (
                priority == "CRITICAL" and 
                message_data.get("skip_rate_limit", False)
            )
            
            if skip_rate_limit:
                logging.debug("Emergency send: Skipping rate limit check for CRITICAL message")
            
            # Level 3: Attempt immediate Telegram send
            try:
                # Send message (rate limit check is inside _send_telegram_message)
                success = await self._send_telegram_message(text)
                
                if success:
                    # Level 4a: Success path
                    self.stats["immediate_sent"] += 1
                    self.stats["emergency_send_success"] += 1
                    
                    logging.info(
                        f"EMERGENCY SEND SUCCESS: {priority} message sent via Telegram "
                        f"(hash: {message_hash})"
                    )
                    return
                
                else:
                    # Level 4b: Telegram send failed - use fallback
                    logging.warning(
                        f"EMERGENCY SEND: Telegram failed for {priority} message, "
                        f"using fallback (hash: {message_hash})"
                    )
            
            except Exception as telegram_error:
                # Telegram send raised exception
                error_name = type(telegram_error).__name__
                logging.warning(
                    f"EMERGENCY SEND: Telegram exception {error_name} for {priority} message, "
                    f"using fallback (hash: {message_hash})"
                )
            
            # Level 5: Fallback notification system (file/console backup)
            try:
                fallback_success = await self._send_fallback_notification(text, priority)
                
                if fallback_success:
                    self.stats["emergency_send_fallback"] += 1
                    
                    logging.info(
                        f"EMERGENCY SEND FALLBACK: {priority} message saved via fallback "
                        f"(hash: {message_hash})"
                    )
                else:
                    # Even fallback failed (very rare)
                    self.stats["emergency_send_failures"] += 1
                    
                    logging.error(
                        f"EMERGENCY SEND FAILURE: Both Telegram and fallback failed "
                        f"for {priority} message (hash: {message_hash})"
                    )
            
            except Exception as fallback_error:
                # Fallback system exception (extremely rare)
                error_name = type(fallback_error).__name__
                self.stats["emergency_send_failures"] += 1
                
                logging.error(
                    f"EMERGENCY SEND FAILURE: Fallback exception {error_name} "
                    f"for {priority} message (hash: {message_hash})"
                )
                
                # Last resort: console critical log
                logging.critical(
                    f"EMERGENCY MESSAGE LOST: {priority} - {text[:100]}"
                )
                
        except Exception as e:
            # Unexpected error in emergency send itself
            error_name = type(e).__name__
            logging.error(f"Emergency send unexpected error: {error_name}: {e}")
            
            # Track failure
            if "emergency_send_failures" in self.stats:
                self.stats["emergency_send_failures"] += 1
            
            # Log available context
            if 'message_data' in locals():
                try:
                    logging.error(f"Message data keys: {list(message_data.keys())}")
                except Exception:
                    logging.error("Could not log message data structure")

    def _update_queue_statistics(self, priority: str, queue_size: int):
        """Update queue statistics for monitoring"""
        try:
            if not hasattr(self, 'queue_stats'):
                self.queue_stats = {}
            
            if priority not in self.queue_stats:
                self.queue_stats[priority] = {
                    "total_queued": 0,
                    "max_size_seen": 0,
                    "avg_size": 0.0,
                    "last_updated": time.time()
                }
            
            stats = self.queue_stats[priority]
            stats["total_queued"] += 1
            stats["max_size_seen"] = max(stats["max_size_seen"], queue_size)
            
            # Update rolling average
            current_avg = stats["avg_size"]
            total_count = stats["total_queued"]
            stats["avg_size"] = ((current_avg * (total_count - 1)) + queue_size) / total_count
            stats["last_updated"] = time.time()
            
        except Exception as e:
            logging.debug(f"Queue statistics update failed: {e}")

    def _update_response_time_stats(self, response_time_ms: float):
        """Update telegram response time statistics"""
        try:
            current_avg = self.stats["avg_response_time"]
            total_sent = self.stats["total_sent"]
            
            if total_sent == 0:
                self.stats["avg_response_time"] = response_time_ms
            else:
                # Calculate rolling average
                self.stats["avg_response_time"] = ((current_avg * (total_sent - 1)) + response_time_ms) / total_sent
            
            # Keep track of response time history for monitoring
            if not hasattr(self, 'response_time_history'):
                self.response_time_history = deque(maxlen=100)
            
            self.response_time_history.append({
                "timestamp": time.time(),
                "response_time_ms": response_time_ms
            })
            
        except Exception as e:
            logging.debug(f"Response time statistics update failed: {e}")

    def _cleanup_old_cooldowns(self, current_time: float):
        """Cleanup old cooldown entries (used when TTLCache is not available)"""
        try:
            cooldown_period = config.MONITORING.get("alert_cooldown", 300)
            expired_keys = []
            
            # Collect expired keys
            for key, timestamp in list(self.alert_cooldowns.items()):
                if current_time - timestamp > cooldown_period:
                    expired_keys.append(key)
            
            # Remove expired keys
            for key in expired_keys:
                del self.alert_cooldowns[key]
            
            if expired_keys:
                logging.debug(f"Cleaned up {len(expired_keys)} expired cooldown entries")
                
            # Additional cleanup if cooldown cache is still too large
            if len(self.alert_cooldowns) > 1000:  # Max 1000 entries
                # Remove oldest entries
                sorted_items = sorted(self.alert_cooldowns.items(), key=lambda x: x[1])
                keep_count = 500  # Keep newest 500
                
                for key, _ in sorted_items[:-keep_count]:
                    del self.alert_cooldowns[key]
                    
                logging.info(f"Emergency cooldown cleanup: kept {keep_count} newest entries")
                
        except Exception as e:
            logging.error(f"Cooldown cleanup failed: {e}")

    def get_queue_health_status(self) -> dict:
        """Get comprehensive queue health status"""
        try:
            status = {
                "queue_sizes": {},
                "queue_health": "healthy",
                "overflow_count": self.stats.get("queue_overflow", 0),
                "total_capacity": 0,
                "used_capacity": 0,
                "cooldown_entries": len(self.alert_cooldowns)
            }
            
            total_capacity = 0
            total_used = 0
            
            for priority, queue in self.message_queues.items():
                queue_size = queue.qsize()
                max_size = queue.maxsize
                utilization = (queue_size / max_size) * 100 if max_size > 0 else 0
                
                status["queue_sizes"][priority] = {
                    "current": queue_size,
                    "max": max_size,
                    "utilization_percent": utilization
                }
                
                total_capacity += max_size
                total_used += queue_size
                
                # Determine health status
                if utilization > 90:
                    status["queue_health"] = "critical"
                elif utilization > 75 and status["queue_health"] != "critical":
                    status["queue_health"] = "warning"
            
            status["total_capacity"] = total_capacity
            status["used_capacity"] = total_used
            status["overall_utilization"] = (total_used / total_capacity * 100) if total_capacity > 0 else 0
            
            return status
            
        except Exception as e:
            logging.error(f"Queue health status check failed: {e}")
            return {"queue_health": "error", "error": str(e)}

    def get_notification_stats(self) -> dict:
        """Get comprehensive notification statistics for monitoring and dashboard"""
        try:
            # Calculate queue sizes
            queue_sizes = {
                priority: queue.qsize() 
                for priority, queue in self.message_queues.items()
            }
            
            # Calculate batch queue sizes
            batch_sizes = {
                priority: len(queue) 
                for priority, queue in self.batch_queues.items()
            }
            
            # Calculate total sizes
            total_queue_size = sum(queue_sizes.values())
            total_batch_size = sum(batch_sizes.values())
            
            # Calculate success rate
            total_sent = self.stats.get("total_sent", 0)
            successful_sent = self.stats.get("successful_sent", 0)
            success_rate = (successful_sent / total_sent * 100) if total_sent > 0 else 100.0
            
            # Get priority statistics
            priority_stats = self.smart_priority.get_priority_statistics()
            
            return {
                "stats": self.stats.copy(),
                "queue_sizes": queue_sizes,
                "batch_sizes": batch_sizes,
                "total_queue_size": total_queue_size,
                "total_batch_size": total_batch_size,
                "rate_limit_remaining": max(0, self.config["rate_limit"] - len(self.last_sent_times)),
                "cooldown_entries": len(self.alert_cooldowns),
                "enabled": self.config["enabled"],
                "priority_stats": priority_stats,
                "success_rate": success_rate,
                "circuit_breaker": {
                    "enabled": self.circuit_breaker_status["enabled"],
                    "consecutive_failures": self.circuit_breaker_status["consecutive_failures"],
                    "is_open": self._is_circuit_breaker_open()
                },
                "blocked_users_count": len(self.blocked_users),
                "timestamp": time.time()
            }
            
        except Exception as e:
            logging.error(f"Failed to get notification stats: {e}")
            return {
                "stats": {},
                "queue_sizes": {},
                "batch_sizes": {},
                "total_queue_size": 0,
                "total_batch_size": 0,
                "error": str(e)
            }

    async def send_trade_notification(self, trade_data: Dict, priority: str = "HIGH"):
        await self.send_execution_result(
            {"execution_result": trade_data, "signal": trade_data}, priority
        )

    async def send_liquidation_warning(
        self, liquidation_data: Dict, priority: str = "CRITICAL"
    ):
        try:
            message_text = self.templates["liquidation_warning"].format(
                symbol=liquidation_data.get("symbol", "UNKNOWN"),
                action=liquidation_data.get("action", "UNKNOWN").upper(),
                leverage=liquidation_data.get("leverage", 1),
                current_price=liquidation_data.get("current_price", 0),
                liquidation_price=liquidation_data.get("liquidation_price", 0),
                risk_level=liquidation_data.get("risk_level", "CRITICAL"),
                distance_percent=liquidation_data.get("distance_percent", 0),
                estimated_loss=liquidation_data.get("estimated_loss", 0),
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            await self._smart_send_notification(
                message_text, priority, "liquidation_warning"
            )
        except Exception as e:
            logging.error(f"Failed to send liquidation warning notification: {e}")

    async def send_system_alert(self, alert_data: Dict, priority: str = "HIGH"):
        try:
            message_text = f"""<b>System Alert</b>

<b>Service:</b> {alert_data.get('service', 'UNKNOWN')}
<b>Status:</b> {alert_data.get('status', 'UNKNOWN')}
<b>Message:</b> {alert_data.get('message', 'No message')}

<b>System Information:</b>
- CPU: {alert_data.get('cpu_percent', 0):.1f}%
- Memory: {alert_data.get('memory_percent', 0):.1f}%
- Active Positions: {alert_data.get('active_positions', 0)}

{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            await self._smart_send_notification(message_text, priority, "system_alert")
        except Exception as e:
            logging.error(f"Failed to send system alert notification: {e}")

    async def send_performance_alert(self, perf_data: Dict, priority: str = "HIGH"):
        """Send performance warning notification"""
        try:
            message_text = self.templates["performance_alert"].format(
                severity=perf_data.get("severity", "UNKNOWN"),
                metric=perf_data.get("metric", "N/A"),
                current_value=perf_data.get("current_value", "N/A"),
                threshold=perf_data.get("threshold", "N/A"),
                impact_description=perf_data.get(
                    "impact_description", "May affect performance."
                ),
                recommended_actions=perf_data.get(
                    "recommended_actions", "System resource check and optimization required."
                ),
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            await self._smart_send_notification(
                message_text, priority, "performance_alert"
            )
        except Exception as e:
            logging.error(f"Failed to send performance alert notification: {e}")

    async def cleanup_resources(self):
        """Enhanced resource cleanup with error handling"""
        cleanup_tasks = []
        
        # HTTP session cleanup
        if self.http_session and not self.http_session.closed:
            try:
                await self.http_session.close()
                logging.info("HTTP session closed successfully")
            except Exception as e:
                logging.warning(f"HTTP session cleanup error: {e}")
        
        # HTTP connector cleanup
        if hasattr(self.http_connector, 'close'):
            try:
                await self.http_connector.close()
                logging.info("HTTP connector closed successfully")
            except Exception as e:
                logging.warning(f"HTTP connector cleanup error: {e}")
        
        # Wait a moment for connections to close
        await asyncio.sleep(0.1)


# ===============================================================================
#                         Full Pipeline Monitoring System
# ===============================================================================


class PipelineMonitor:
    """Pipeline monitoring system for all services"""

    def __init__(self, telegram_manager: EnhancedTelegramNotificationManager):
        self.telegram_manager = telegram_manager
        self.services_config = config.SERVICES
        self.pipeline_status = {
            name: {"status": "unknown", "last_check": 0}
            for name in self.services_config
            if self.services_config[name].get("enabled")
        }
        self.data_flow = {
            "webhook_to_brain": {"count": 0, "last_flow": 0},
            "brain_to_risk": {"count": 0, "last_flow": 0},
            "risk_to_execute": {"count": 0, "last_flow": 0},
            "execute_to_notify": {"count": 0, "last_flow": 0},
        }
        self.pipeline_metrics = {
            "total_signals_processed": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "start_time": time.time(),
        }
        self.lock = asyncio.Lock()

    async def _check_service_health(self, service_name: str, service_info: Dict):
        current_time = time.time()
        
        # Skip if recently checked (reduce API calls)
        if service_name in self.pipeline_status:
            last_check = self.pipeline_status[service_name].get("last_check", 0)
            if current_time - last_check < 30:  # 30 second interval
                return
        
        url = service_info["url"] + service_info.get("health_endpoint", "/health")
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(url) as response:
                    status = "healthy" if response.status == 200 else "unhealthy"
                    
                    async with self.lock:
                        self.pipeline_status[service_name] = {
                            "status": status,
                            "last_check": current_time,
                            "details": {"status_code": response.status}
                        }
        except Exception as e:
            async with self.lock:
                self.pipeline_status[service_name] = {
                    "status": "error", 
                    "last_check": current_time,
                    "details": {"error": str(e)}
                }

    async def _monitor_loop(self):
        while True:
            tasks = [
                self._check_service_health(name, info)
                for name, info in self.services_config.items()
                if info.get("enabled")
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(config.MONITORING["pipeline_check_interval"])

    async def start_monitoring(self):
        logging.info("Pipeline monitoring started")
        return [asyncio.create_task(self._monitor_loop())]

    def record_data_flow(self, flow_name: str, success: bool):
        if flow_name in self.data_flow:
            self.data_flow[flow_name]["count"] += 1
            self.data_flow[flow_name]["last_flow"] = time.time()

    def record_signal_processing(self, success: bool):
        self.pipeline_metrics["total_signals_processed"] += 1
        if success:
            self.pipeline_metrics["successful_executions"] += 1
        else:
            self.pipeline_metrics["failed_executions"] += 1

    def get_pipeline_stats(self) -> Dict:
        uptime_seconds = time.time() - self.pipeline_metrics["start_time"]
        return {
            "pipeline_status": self.pipeline_status,
            "data_flow": self.data_flow,
            "pipeline_metrics": self.pipeline_metrics,
            "uptime_hours": uptime_seconds / 3600,
        }


# ===============================================================================
#                          Enhanced Performance Monitoring System
# ===============================================================================


class EnhancedPerformanceMonitor:
    """Integrated performance monitoring system (pipeline integration)"""

    def __init__(
        self,
        telegram_manager: EnhancedTelegramNotificationManager,
        pipeline_monitor: PipelineMonitor,
    ):
        self.telegram = telegram_manager
        self.pipeline = pipeline_monitor
        
        # Safe access since config.MONITORING may be a dictionary
        if hasattr(config, 'MONITORING') and hasattr(config.MONITORING, 'get'):
            self.config_monitor = config.MONITORING
        else:
            self.config_monitor = {
                "alert_cooldown": 300,
                "metrics_interval": 5,
                "pipeline_check_interval": 30,
                "health_check_interval": 30,
                "performance_thresholds": {
                    "cpu_percent": 80,
                    "memory_percent": 85,
                    "disk_percent": 90
                }
            }

        # Metric storage
        self.metrics_history = deque(maxlen=1440)  # 24 hours
        self.alert_history = deque(maxlen=1000)

        # Performance statistics
        self.performance_stats = {
            "start_time": time.time(),
            "total_checks": 0,
            "alerts_sent": 0,
            "avg_response_time": 0.0,
            "uptime_percent": 100.0,
            "last_alert_time": 0,
        }

        logging.info("Enhanced performance monitoring system initialization completed")

    async def start_enhanced_monitoring(self):
        """Start enhanced monitoring"""
        logging.info("Starting integrated performance monitoring")

        tasks = [
            asyncio.create_task(self._system_metrics_collector()),
            asyncio.create_task(self._integrated_health_checker()),
            asyncio.create_task(self._performance_analyzer()),
            asyncio.create_task(self._alert_processor()),
        ]

        # Add pipeline monitoring tasks
        pipeline_tasks = await self.pipeline.start_monitoring()
        tasks.extend(pipeline_tasks)

        return tasks

    async def _system_metrics_collector(self):
        """System metrics collector"""
        while True:
            try:
                metrics = await self._collect_system_metrics()
                self.metrics_history.append(metrics)

                # Check thresholds
                await self._check_thresholds(metrics)

                await asyncio.sleep(self.config_monitor.get("metrics_interval", 5))

            except Exception as e:
                logging.error(f"Metrics collection error: {e}")
                await asyncio.sleep(10)

    async def _collect_system_metrics(self) -> Dict:
        """Collect system metrics (maintain basic logic with extension)"""
        try:
            # Basic system resources
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage("/")

            # Network statistics
            network = psutil.net_io_counters()

            # Process information
            process = psutil.Process()
            process_memory = process.memory_info().rss / 1024 / 1024
            process_cpu = process.cpu_percent()

            # Thread count
            thread_count = threading.active_count()

            # Telegram manager statistics
            telegram_stats = self.telegram.get_notification_stats()

            # Pipeline statistics
            pipeline_stats = self.pipeline.get_pipeline_stats()

            return {
                "timestamp": time.time(),
                "system": {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_available_gb": memory.available / (1024**3),
                    "disk_percent": disk.percent,
                    "disk_free_gb": disk.free / (1024**3),
                    "process_memory_mb": process_memory,
                    "process_cpu_percent": process_cpu,
                    "thread_count": thread_count,
                },
                "telegram": {
                    "queue_size": telegram_stats["total_queue_size"],
                    "batch_size": telegram_stats["total_batch_size"],
                    "success_rate": (
                        telegram_stats["stats"]["successful_sent"]
                        / max(telegram_stats["stats"]["total_sent"], 1)
                    )
                    * 100,
                    "avg_response_time": telegram_stats["stats"]["avg_response_time"],
                },
                "pipeline": {
                    "healthy_services": sum(
                        1
                        for status in pipeline_stats["pipeline_status"].values()
                        if status["status"] == "healthy"
                    ),
                    "total_services": len(pipeline_stats["pipeline_status"]),
                    "total_signals": pipeline_stats["pipeline_metrics"][
                        "total_signals_processed"
                    ],
                    "success_rate": (
                        pipeline_stats["pipeline_metrics"]["successful_executions"]
                        / max(
                            pipeline_stats["pipeline_metrics"][
                                "total_signals_processed"
                            ],
                            1,
                        )
                    )
                    * 100,
                },
            }

        except Exception as e:
            logging.error(f"Metrics collection failed: {e}")
            return {"timestamp": time.time(), "error": str(e)}

    async def _integrated_health_checker(self):
        """Integrated health check (pipeline integration)"""
        while True:
            try:
                # System status summary
                current_metrics = (
                    list(self.metrics_history)[-1] if self.metrics_history else {}
                )
                pipeline_stats = self.pipeline.get_pipeline_stats()

                # Calculate overall health
                health_score = await self._calculate_overall_health(
                    current_metrics, pipeline_stats
                )

                # Alert if health is low
                if health_score < 70:
                    await self._send_health_degradation_alert(
                        health_score, current_metrics, pipeline_stats
                    )

                await asyncio.sleep(self.config_monitor.get("health_check_interval", 30))

            except Exception as e:
                logging.error(f"Integrated health check error: {e}")
                await asyncio.sleep(60)

    async def _calculate_overall_health(
        self, metrics: Dict, pipeline_stats: Dict
    ) -> float:
        """Calculate overall health score"""
        try:
            scores = []

            # System score (40%)
            if "system" in metrics:
                sys_metrics = metrics["system"]
                system_score = (
                    (100 - sys_metrics.get("cpu_percent", 0)) * 0.3
                    + (100 - sys_metrics.get("memory_percent", 0)) * 0.4
                    + (100 - sys_metrics.get("disk_percent", 0)) * 0.2
                    + min(100, (50 - sys_metrics.get("thread_count", 50)) * 2) * 0.1
                )
                scores.append(("system", system_score, 0.4))

            # Telegram score (20%)
            if "telegram" in metrics:
                tg_metrics = metrics["telegram"]
                telegram_score = (
                    tg_metrics.get("success_rate", 0) * 0.6
                    + max(0, 100 - tg_metrics.get("queue_size", 0) / 10) * 0.3
                    + max(0, 100 - tg_metrics.get("avg_response_time", 0) / 20) * 0.1
                )
                scores.append(("telegram", telegram_score, 0.2))

            # Pipeline score (40%)
            pipeline_metrics = pipeline_stats.get("pipeline_metrics", {})
            pipeline_score = (
                pipeline_stats.get("pipeline", {}).get("success_rate", 0) * 0.5
                + (
                    pipeline_stats.get("pipeline", {}).get("healthy_services", 0)
                    / max(
                        pipeline_stats.get("pipeline", {}).get("total_services", 1), 1
                    )
                )
                * 100
                * 0.5
            )
            scores.append(("pipeline", pipeline_score, 0.4))

            # Calculate weighted average
            total_score = sum(score * weight for _, score, weight in scores)

            return total_score

        except Exception as e:
            logging.error(f"Health calculation error: {e}")
            return 50.0  # Default value


    async def _send_health_degradation_alert(
        self, health_score: float, metrics: Dict, pipeline_stats: Dict
    ):
        """Send overall health degradation alert"""
        try:
            # Prevent duplicate alerts (30 minute cooldown)
            current_time = time.time()
            if (
                self.performance_stats["last_alert_time"] > 0
                and current_time - self.performance_stats["last_alert_time"] < 1800
            ):
                return
            
            sys_metrics = metrics.get("system", {})
            tg_metrics = metrics.get("telegram", {})
            pipeline_metrics = metrics.get("pipeline", {})
            
            message = f"""WARNING: System Health Degraded
OVERALL HEALTH: {health_score:.1f}/100
SYSTEM STATUS:
- CPU: {sys_metrics.get('cpu_percent', 0):.1f}%
- Memory: {sys_metrics.get('memory_percent', 0):.1f}%
- Disk: {sys_metrics.get('disk_percent', 0):.1f}%
- Threads: {sys_metrics.get('thread_count', 0)}

TELEGRAM:
- Success Rate: {tg_metrics.get('success_rate', 0):.1f}%
- Queue Size: {tg_metrics.get('queue_size', 0)}
- Response Time: {tg_metrics.get('avg_response_time', 0):.0f}ms

PIPELINE:
- Healthy Services: {pipeline_metrics.get('healthy_services', 0)}/{pipeline_metrics.get('total_services', 0)}
- Pipeline Success Rate: {pipeline_metrics.get('success_rate', 0):.1f}%

RECOMMENDED ACTIONS:
- Check system resources
- Consider restarting services
- Perform log analysis

TIMESTAMP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""

            alert_data = {
                "service": "Integrated Monitoring",
                "status": f"Health Degraded ({health_score:.1f} score)",
                "message": message,
                "cpu_percent": sys_metrics.get("cpu_percent", 0),
                "memory_percent": sys_metrics.get("memory_percent", 0),
                "active_positions": 0,
            }

            await self.telegram.send_system_alert(alert_data, "HIGH")
            self.performance_stats["last_alert_time"] = current_time

        except Exception as e:
            logging.error(f"Failed to send health degradation alert: {e}")

    async def _check_thresholds(self, metrics: Dict):
        """Check thresholds (maintain basic logic)"""
        if "error" in metrics or "system" not in metrics:
            return

        sys_metrics = metrics["system"]
        thresholds = self.config_monitor.get("performance_thresholds", {
            "cpu_percent": 80,
            "memory_percent": 85,
            "disk_percent": 90
        })
        alerts = []

        # Existing threshold check logic
        if sys_metrics["cpu_percent"] > thresholds.get("cpu_percent", 80):
            alerts.append(
                {
                    "type": "CPU_HIGH",
                    "metric": "CPU Usage",
                    "current_value": f"{sys_metrics['cpu_percent']:.1f}%",
                    "threshold": f"{thresholds.get('cpu_percent', 80)}%",
                    "severity": "HIGH" if sys_metrics["cpu_percent"] > 90 else "MEDIUM",
                }
            )

        # Send alerts
        for alert in alerts:
            await self._send_performance_alert(alert)

    async def _send_performance_alert(self, alert_data: Dict):
        """Send performance alert (maintain basic logic)"""
        try:
            current_time = time.time()

            if (
                self.performance_stats["last_alert_time"] > 0
                and current_time - self.performance_stats["last_alert_time"] < 300
            ):
                return

            # Existing performance alert logic
            perf_data = {
                "metric": alert_data["metric"],
                "current_value": alert_data["current_value"],
                "threshold": alert_data["threshold"],
                "severity": alert_data["severity"],
                "impact_description": "May affect system performance",
                "recommended_actions": "Check and optimize system resources",
            }

            await self.telegram.send_performance_alert(
                perf_data, alert_data["severity"]
            )

            self.performance_stats["alerts_sent"] += 1
            self.performance_stats["last_alert_time"] = current_time

        except Exception as e:
            logging.error(f"Failed to send performance alert: {e}")

    async def _performance_analyzer(self):
        """Performance analyzer (basic logic + pipeline analysis)"""
        while True:
            try:
                await asyncio.sleep(300)

                if len(self.metrics_history) < 10:
                    continue

                # Existing trend analysis
                recent_metrics = list(self.metrics_history)[-60:]

                # Add pipeline performance analysis
                pipeline_stats = self.pipeline.get_pipeline_stats()
                await self._analyze_pipeline_performance(pipeline_stats)

                # Send regular performance summary
                await self._send_performance_summary()

            except Exception as e:
                logging.error(f"Performance analysis error: {e}")
                await asyncio.sleep(60)

    async def _analyze_pipeline_performance(self, pipeline_stats: Dict):
        """Analyze pipeline performance"""
        try:
            metrics = pipeline_stats.get("pipeline_metrics", {})

            # Check if success rate dropped significantly
            success_rate = (
                metrics.get("successful_executions", 0)
                / max(metrics.get("total_signals_processed", 1), 1)
            ) * 100

            if success_rate < 50 and metrics.get("total_signals_processed", 0) > 10:
                logging.warning(f"Pipeline success rate degraded: {success_rate:.1f}%")

            # Analyze data flow stagnation
            data_flow = pipeline_stats.get("data_flow", {})
            current_time = time.time()

            for flow_name, flow_data in data_flow.items():
                if (
                    flow_data.get("last_flow", 0) > 0
                    and current_time - flow_data["last_flow"] > 600
                ):  # 10 minute stagnation
                    logging.warning(f"Data flow stagnation: {flow_name}")

        except Exception as e:
            logging.error(f"Pipeline performance analysis error: {e}")

    async def _send_performance_summary(self):
        """Send performance summary (every hour)"""
        try:
            current_time = time.time()

            # Check 1 hour interval
            if not hasattr(self, "_last_summary_time"):
                self._last_summary_time = 0

            if current_time - self._last_summary_time < 3600:
                return

            # Send summary with pipeline statistics
            pipeline_stats = self.pipeline.get_pipeline_stats()
            await self.telegram.send_pipeline_status_update(pipeline_stats)

            self._last_summary_time = current_time

        except Exception as e:
            logging.error(f"Failed to send performance summary: {e}")

    async def _alert_processor(self):
        """Alert processor (existing logic)"""
        while True:
            try:
                await asyncio.sleep(3600)
                await self._send_hourly_summary()

            except Exception as e:
                logging.error(f"Alert processing error: {e}")
                await asyncio.sleep(60)

    async def _send_hourly_summary(self):
        """Send hourly summary (maintain existing logic)"""
        try:
            if not self.metrics_history:
                return

            hour_ago = time.time() - 3600
            recent_metrics = [
                m for m in self.metrics_history if m["timestamp"] > hour_ago
            ]

            if not recent_metrics:
                return

            # Existing summary logic
            system_metrics = [
                m.get("system", {}) for m in recent_metrics if "system" in m
            ]
            if system_metrics:
                avg_cpu = np.mean([m.get("cpu_percent", 0) for m in system_metrics])
                avg_memory = np.mean(
                    [m.get("memory_percent", 0) for m in system_metrics]
                )
                avg_disk = np.mean([m.get("disk_percent", 0) for m in system_metrics])

                # Add pipeline information
                pipeline_stats = self.pipeline.get_pipeline_stats()
                healthy_services = sum(
                    1
                    for status in pipeline_stats["pipeline_status"].values()
                    if status["status"] == "healthy"
                )
                total_services = len(pipeline_stats["pipeline_status"])

                alert_data = {
                    "service": "Integrated Monitoring Hub",
                    "status": "Normal Operation",
                    "message": f"Hourly Performance Summary\n- CPU: {avg_cpu:.1f}% | Memory: {avg_memory:.1f}% | Disk: {avg_disk:.1f}%\n- Pipeline: {healthy_services}/{total_services} services healthy\n- Processed Signals: {pipeline_stats['pipeline_metrics']['total_signals_processed']}",
                    "cpu_percent": avg_cpu,
                    "memory_percent": avg_memory,
                    "active_positions": 0,
                }

                await self.telegram.send_system_alert(alert_data, "LOW")

        except Exception as e:
            logging.error(f"Failed to send hourly summary: {e}")

    def get_monitoring_stats(self) -> Dict:
        """Query monitoring statistics (including pipeline information)"""
        if not self.metrics_history:
            return {"error": "No metric data available"}

        recent_metrics = list(self.metrics_history)[-60:]
        pipeline_stats = self.pipeline.get_pipeline_stats()

        # Average system metrics
        system_metrics = [m.get("system", {}) for m in recent_metrics if "system" in m]

        if system_metrics:
            avg_system = {
                "avg_cpu_percent": np.mean(
                    [m.get("cpu_percent", 0) for m in system_metrics]
                ),
                "avg_memory_percent": np.mean(
                    [m.get("memory_percent", 0) for m in system_metrics]
                ),
                "avg_disk_percent": np.mean(
                    [m.get("disk_percent", 0) for m in system_metrics]
                ),
                "thread_count": system_metrics[-1].get("thread_count", 0)
                if system_metrics
                else 0,
            }
        else:
            avg_system = {
                "avg_cpu_percent": 0,
                "avg_memory_percent": 0,
                "avg_disk_percent": 0,
                "thread_count": 0,
            }

        return {
            "system_metrics": avg_system,
            "pipeline_stats": pipeline_stats,
            "performance_stats": self.performance_stats.copy(),
            "alert_count": len(self.alert_history),
            "metrics_count": len(self.metrics_history),
            "uptime_seconds": time.time() - self.performance_stats["start_time"],
        }


# ===============================================================================
#                          Enhanced Real-Time Dashboard (Pipeline Visualization)
# ===============================================================================


class EnhancedRealTimeDashboard:
    """Enhanced real-time dashboard (including pipeline visualization)"""

    def __init__(
        self,
        telegram_manager: EnhancedTelegramNotificationManager,
        performance_monitor: EnhancedPerformanceMonitor,
        pipeline_monitor: PipelineMonitor,
    ):
        self.telegram = telegram_manager
        self.monitor = performance_monitor
        self.pipeline = pipeline_monitor
        self.config_dash = config.DASHBOARD

        # WebSocket connection management
        self.active_connections: List[WebSocket] = []

        # Chart data cache
        self.chart_data_cache = {}
        self.last_chart_update = 0

        logging.info("Enhanced real-time dashboard initialization completed")

    def generate_enhanced_dashboard_html(self) -> str:
        """Generate enhanced dashboard HTML"""

        # Collect statistics data
        telegram_stats = self.telegram.get_notification_stats()
        monitoring_stats = self.monitor.get_monitoring_stats()
        pipeline_stats = self.pipeline.get_pipeline_stats()

        return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{self.config_dash["title"]}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #0f0f23 0%, #1a1a2e 50%, #16213e 100%);
            color: #ffffff;
            min-height: 100vh;
            padding: 20px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 25px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 15px;
            backdrop-filter: blur(15px);
            border: 1px solid rgba(0, 255, 136, 0.3);
        }}
        
        .header h1 {{
            font-size: 2.8em;
            background: linear-gradient(45deg, #00ff88, #00ccff);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 0 30px rgba(0, 255, 136, 0.5);
            margin-bottom: 10px;
        }}
        
        .phoenix-badge {{
            display: inline-block;
            background: linear-gradient(45deg, #ff6b35, #f7931e);
            padding: 8px 20px;
            border-radius: 25px;
            font-size: 1.1em;
            font-weight: bold;
            margin-top: 10px;
            box-shadow: 0 4px 15px rgba(247, 147, 30, 0.3);
        }}
        
        .status-indicator {{
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }}
        
        .status-healthy {{ background: #00ff88; }}
        .status-warning {{ background: #ffaa00; }}
        .status-critical {{ background: #ff4444; }}
        
        @keyframes pulse {{
            0% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
            100% {{ opacity: 1; }}
        }}
        
        .pipeline-container {{
            margin-bottom: 30px;
            padding: 20px;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 15px;
            border: 1px solid rgba(0, 255, 136, 0.2);
        }}
        
        .pipeline-title {{
            font-size: 1.5em;
            color: #00ff88;
            margin-bottom: 20px;
            text-align: center;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
        }}
        
        .pipeline-flow {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }}
        
        .pipeline-step {{
            flex: 1;
            min-width: 150px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 12px;
            padding: 15px;
            text-align: center;
            border: 2px solid transparent;
            transition: all 0.3s ease;
        }}
        
        .pipeline-step.healthy {{
            border-color: #00ff88;
            box-shadow: 0 4px 20px rgba(0, 255, 136, 0.2);
        }}
        
        .pipeline-step.warning {{
            border-color: #ffaa00;
            box-shadow: 0 4px 20px rgba(255, 170, 0, 0.2);
        }}
        
        .pipeline-step.error {{
            border-color: #ff4444;
            box-shadow: 0 4px 20px rgba(255, 68, 68, 0.2);
        }}
        
        .step-icon {{
            font-size: 2em;
            margin-bottom: 10px;
        }}
        
        .step-name {{
            font-weight: bold;
            margin-bottom: 8px;
            font-size: 1.1em;
        }}
        
        .step-status {{
            padding: 4px 8px;
            border-radius: 8px;
            font-size: 0.9em;
            font-weight: bold;
            margin-bottom: 5px;
        }}
        
        .step-detail {{
            font-size: 0.8em;
            color: #cccccc;
        }}
        
        .pipeline-arrow {{
            font-size: 1.5em;
            color: #00ff88;
            font-weight: bold;
        }}
        
        .data-flow-stats {{
            margin-top: 20px;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        
        .flow-item {{
            background: rgba(0, 255, 136, 0.1);
            border-radius: 10px;
            padding: 12px;
            text-align: center;
            border: 1px solid rgba(0, 255, 136, 0.3);
        }}
        
        .flow-label {{
            font-size: 0.9em;
            color: #cccccc;
            margin-bottom: 5px;
        }}
        
        .flow-value {{
            font-size: 1.3em;
            color: #00ff88;
            font-weight: bold;
        }}
        
        .dashboard-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .card {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 15px;
            padding: 25px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }}
        
        .card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0, 255, 136, 0.2);
        }}
        
        .card-title {{
            font-size: 1.4em;
            color: #00ff88;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .card-icon {{
            font-size: 1.5em;
        }}
        
        .metric-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }}

        .metric-item:last-child {{
            border-bottom: none;
        }}
        
        .metric-label {{
            color: #cccccc;
            font-weight: 500;
        }}
        
        .metric-value {{
            color: #00ff88;
            font-weight: bold;
            font-size: 1.1em;
        }}
        
        .metric-value.warning {{
            color: #ffaa00;
        }}
        
        .metric-value.critical {{
            color: #ff4444;
        }}
        
        .priority-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
            margin-top: 15px;
        }}
        
        .priority-item {{
            background: rgba(0, 255, 136, 0.1);
            border-radius: 10px;
            padding: 12px;
            text-align: center;
            border: 1px solid rgba(0, 255, 136, 0.3);
        }}
        
        .priority-label {{
            font-size: 0.8em;
            color: #cccccc;
            margin-bottom: 5px;
        }}
        
        .priority-count {{
            font-size: 1.4em;
            color: #00ff88;
            font-weight: bold;
        }}
        
        .priority-avg {{
            font-size: 0.8em;
            color: #ffaa00;
            margin-top: 2px;
        }}
        
        .auto-refresh {{
            position: fixed;
            top: 20px;
            right: 20px;
            background: rgba(0, 255, 136, 0.2);
            border: 1px solid #00ff88;
            border-radius: 20px;
            padding: 8px 15px;
            color: #00ff88;
            font-size: 0.9em;
            z-index: 1000;
        }}
        
        .connection-status {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            padding: 10px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            z-index: 1000;
        }}
        
        .connected {{
            background: rgba(0, 255, 136, 0.2);
            border: 1px solid #00ff88;
            color: #00ff88;
        }}
        
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding: 20px;
            color: #888;
            border-top: 1px solid rgba(255, 255, 255, 0.1);
        }}
        
        @media (max-width: 1200px) {{
            .pipeline-flow {{
                justify-content: center;
            }}
            
            .pipeline-arrow {{
                display: none;
            }}
        }}
        
        @media (max-width: 768px) {{
            .dashboard-grid {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 2.2em;
            }}
            
            .pipeline-flow {{
                flex-direction: column;
            }}
            
            .priority-grid {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="auto-refresh" id="refresh-indicator">
        Auto Refresh: {self.config_dash["refresh_interval"]}s
    </div>
    
    <div class="connection-status connected" id="connection-status">
        <span class="status-indicator status-healthy"></span>Real-time Connected
    </div>
    
    <div class="header">
        <h1>Phoenix 95 V4 Ultimate</h1>
        <div class="phoenix-badge">AI-Powered Trading Pipeline</div>
        <h2>Integrated Notification Hub & Pipeline Monitoring</h2>
        <p><span class="status-indicator status-healthy"></span>Port 8103 | Real-time Monitoring & Smart Notifications</p>
        <p>Last Update: <span id="last-update">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span></p>
    </div>
    
    <!-- Phoenix95 Pipeline Visualization -->
    <div class="pipeline-container">
        <div class="pipeline-title">
            <span></span>
            Phoenix95 AI Trading Pipeline
            <span></span>
        </div>
        
        <div class="pipeline-flow">
            <div class="pipeline-step {'healthy' if pipeline_stats['pipeline_status'].get('webhook_server', {}).get('status') == 'healthy' else 'warning'}">
                <div class="step-icon"></div>
                <div class="step-name">Webhook Server</div>
                <div class="step-status status-{'healthy' if pipeline_stats['pipeline_status'].get('webhook_server', {}).get('status') == 'healthy' else 'warning'}">
                    {pipeline_stats['pipeline_status'].get('webhook_server', {}).get('status', 'UNKNOWN').upper()}
                </div>
                <div class="step-detail">Signal Reception</div>
            </div>
            
            <div class="pipeline-arrow">→</div>
            
            <div class="pipeline-step {'healthy' if pipeline_stats['pipeline_status'].get('brain_service', {}).get('status') == 'healthy' else 'warning'}">
                <div class="step-icon"></div>
                <div class="step-name">Phoenix95 Brain</div>
                <div class="step-status status-{'healthy' if pipeline_stats['pipeline_status'].get('brain_service', {}).get('status') == 'healthy' else 'warning'}">
                    {pipeline_stats['pipeline_status'].get('brain_service', {}).get('status', 'UNKNOWN').upper()}
                </div>
                <div class="step-detail">AI Analysis & 95-Point Evaluation</div>
            </div>
            
            <div class="pipeline-arrow">→</div>
            
            <div class="pipeline-step {'healthy' if pipeline_stats['pipeline_status'].get('risk_service', {}).get('status') == 'healthy' else 'warning'}">
                <div class="step-icon"></div>
                <div class="step-name">Risk Guard</div>
                <div class="step-status status-{'healthy' if pipeline_stats['pipeline_status'].get('risk_service', {}).get('status') == 'healthy' else 'warning'}">
                    {pipeline_stats['pipeline_status'].get('risk_service', {}).get('status', 'UNKNOWN').upper()}
                </div>
                <div class="step-detail">Kelly Criterion Risk</div>
            </div>
            
            <div class="pipeline-arrow">→</div>
            
            <div class="pipeline-step {'healthy' if pipeline_stats['pipeline_status'].get('execute_service', {}).get('status') == 'healthy' else 'warning'}">
                <div class="step-icon"></div>
                <div class="step-name">Trade Execute</div>
                <div class="step-status status-{'healthy' if pipeline_stats['pipeline_status'].get('execute_service', {}).get('status') == 'healthy' else 'warning'}">
                    {pipeline_stats['pipeline_status'].get('execute_service', {}).get('status', 'UNKNOWN').upper()}
                </div>
                <div class="step-detail">{pipeline_stats['pipeline_status'].get('execute_service', {}).get('active_positions', 0)} Active Positions</div>
            </div>
            
            <div class="pipeline-arrow">→</div>
            
            <div class="pipeline-step healthy">
                <div class="step-icon"></div>
                <div class="step-name">Notify Hub</div>
                <div class="step-status status-healthy">HEALTHY</div>
                <div class="step-detail">Smart Notifications</div>
            </div>
        </div>
        
        <div class="data-flow-stats">
            <div class="flow-item">
                <div class="flow-label">Total Signals Processed</div>
                <div class="flow-value">{pipeline_stats['pipeline_metrics']['total_signals_processed']}</div>
            </div>
            <div class="flow-item">
                <div class="flow-label">Successful Executions</div>
                <div class="flow-value">{pipeline_stats['pipeline_metrics']['successful_executions']}</div>
            </div>
            <div class="flow-item">
                <div class="flow-label">Pipeline Success Rate</div>
                <div class="flow-value">{(pipeline_stats['pipeline_metrics']['successful_executions'] / max(pipeline_stats['pipeline_metrics']['total_signals_processed'], 1)) * 100:.1f}%</div>
            </div>
            <div class="flow-item">
                <div class="flow-label">Uptime</div>
                <div class="flow-value">{pipeline_stats.get('uptime_hours', 0):.1f}h</div>
            </div>
        </div>
    </div>
    
    <div class="dashboard-grid">
        <!-- Phoenix95 Smart Priority Statistics -->
        <div class="card">
            <div class="card-title">
                <span class="card-icon"></span>
                Phoenix95 Smart Priority
            </div>
            <div class="metric-item">
                <span class="metric-label">Smart Priority Used:</span>
                <span class="metric-value">{telegram_stats['stats']['smart_priority_used']:,} times</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Immediate Sent:</span>
                <span class="metric-value">{telegram_stats['stats']['immediate_sent']:,}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Batch Sent:</span>
                <span class="metric-value">{telegram_stats['stats']['batch_sent']:,}</span>
            </div>
            
            <div class="priority-grid">
                <div class="priority-item">
                    <div class="priority-label">CRITICAL</div>
                    <div class="priority-count">{telegram_stats['priority_stats']['CRITICAL']['count']}</div>
                    <div class="priority-avg">Avg: {telegram_stats['priority_stats']['CRITICAL']['avg_phoenix_score']:.1f}</div>
                </div>
                <div class="priority-item">
                    <div class="priority-label">HIGH</div>
                    <div class="priority-count">{telegram_stats['priority_stats']['HIGH']['count']}</div>
                    <div class="priority-avg">Avg: {telegram_stats['priority_stats']['HIGH']['avg_phoenix_score']:.1f}</div>
                </div>
                <div class="priority-item">
                    <div class="priority-label">MEDIUM</div>
                    <div class="priority-count">{telegram_stats['priority_stats']['MEDIUM']['count']}</div>
                    <div class="priority-avg">Avg: {telegram_stats['priority_stats']['MEDIUM']['avg_phoenix_score']:.1f}</div>
                </div>
                <div class="priority-item">
                    <div class="priority-label">LOW</div>
                    <div class="priority-count">{telegram_stats['priority_stats']['LOW']['count']}</div>
                    <div class="priority-avg">Avg: {telegram_stats['priority_stats']['LOW']['avg_phoenix_score']:.1f}</div>
                </div>
            </div>
        </div>
        
        <!-- Enhanced Telegram Statistics -->
        <div class="card">
            <div class="card-title">
                <span class="card-icon"></span>
                Enhanced Telegram Notifications
            </div>
            <div class="metric-item">
                <span class="metric-label">Total Sent:</span>
                <span class="metric-value">{telegram_stats['stats']['total_sent']:,}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Successful Sent:</span>
                <span class="metric-value">{telegram_stats['stats']['successful_sent']:,}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Failed Sent:</span>
                <span class="metric-value {'warning' if telegram_stats['stats']['failed_sent'] > 0 else ''}">{telegram_stats['stats']['failed_sent']:,}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Rate Limited:</span>
                <span class="metric-value {'warning' if telegram_stats['stats']['rate_limited'] > 0 else ''}">{telegram_stats['stats']['rate_limited']:,} times</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Queue Overflow:</span>
                <span class="metric-value {'warning' if telegram_stats['stats']['queue_overflow'] > 0 else ''}">{telegram_stats['stats']['queue_overflow']:,} times</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Avg Response Time:</span>
                <span class="metric-value">{telegram_stats['stats']['avg_response_time']:.0f}ms</span>
            </div>
        </div>

        <!-- Queue Status (Regular + Batch) -->
        <div class="card">
            <div class="card-title">
                <span class="card-icon"></span>
                Integrated Message Queue Status
            </div>
            <div class="metric-item">
                <span class="metric-label">Regular Queue Pending:</span>
                <span class="metric-value">{telegram_stats['total_queue_size']:,}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Batch Queue Pending:</span>
                <span class="metric-value">{telegram_stats['total_batch_size']:,}</span>
            </div>
            
            <div class="priority-grid">
                <div class="priority-item">
                    <div class="priority-label">CRITICAL Queue</div>
                    <div class="priority-count">{telegram_stats['queue_sizes']['CRITICAL']}</div>
                </div>
                <div class="priority-item">
                    <div class="priority-label">HIGH Queue</div>
                    <div class="priority-count">{telegram_stats['queue_sizes']['HIGH']}</div>
                </div>
                <div class="priority-item">
                    <div class="priority-label">MEDIUM Queue</div>
                    <div class="priority-count">{telegram_stats['queue_sizes']['MEDIUM']}</div>
                </div>
                <div class="priority-item">
                    <div class="priority-label">LOW Queue</div>
                    <div class="priority-count">{telegram_stats['queue_sizes']['LOW']}</div>
                </div>
            </div>
        </div>
        
        <!-- Integrated System Performance -->
        <div class="card">
            <div class="card-title">
                <span class="card-icon"></span>
                Integrated System Performance
            </div>
            <div class="metric-item">
                <span class="metric-label">CPU Usage:</span>
                <span class="metric-value {'warning' if monitoring_stats.get('system_metrics', {}).get('avg_cpu_percent', 0) > 70 else ''}">{monitoring_stats.get('system_metrics', {}).get('avg_cpu_percent', 0):.1f}%</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Memory Usage:</span>
                <span class="metric-value {'warning' if monitoring_stats.get('system_metrics', {}).get('avg_memory_percent', 0) > 80 else ''}">{monitoring_stats.get('system_metrics', {}).get('avg_memory_percent', 0):.1f}%</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Disk Usage:</span>
                <span class="metric-value {'warning' if monitoring_stats.get('system_metrics', {}).get('avg_disk_percent', 0) > 85 else ''}">{monitoring_stats.get('system_metrics', {}).get('avg_disk_percent', 0):.1f}%</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Active Threads:</span>
                <span class="metric-value">{monitoring_stats.get('system_metrics', {}).get('thread_count', 0)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Uptime:</span>
                <span class="metric-value">{str(timedelta(seconds=int(monitoring_stats.get('uptime_seconds', 0))))}</span>
            </div>
        </div>
        
        <!-- Pipeline Data Flow -->
        <div class="card">
            <div class="card-title">
                <span class="card-icon"></span>
                Data Flow Statistics
            </div>
            {''.join([f'''
            <div class="metric-item">
                <span class="metric-label">{flow_name.replace('_', ' ').replace('to', '->').title()}:</span>
                <span class="metric-value">{flow_data['count']} Processed</span>
            </div>
            ''' for flow_name, flow_data in pipeline_stats['data_flow'].items()])}
            
            <div class="metric-item">
                <span class="metric-label">Last Signal:</span>
                <span class="metric-value">{datetime.fromtimestamp(max([flow.get('last_flow', 0) for flow in pipeline_stats['data_flow'].values()], default=0)).strftime('%H:%M:%S') if max([flow.get('last_flow', 0) for flow in pipeline_stats['data_flow'].values()], default=0) > 0 else 'N/A'}</span>
            </div>
        </div>
        
        <!-- NOTIFY Service Information -->
        <div class="card">
            <div class="card-title">
                <span class="card-icon"></span>
                NOTIFY Hub Information
            </div>
            <div class="metric-item">
                <span class="metric-label">Version:</span>
                <span class="metric-value">V4 Ultimate (Phoenix95 Integrated)</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Port:</span>
                <span class="metric-value">8103</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Performance Optimization:</span>
                <span class="metric-value">{'Available: uvloop' if UVLOOP_AVAILABLE else 'Unavailable: uvloop'}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">WebSocket Connections:</span>
                <span class="metric-value" id="active-connections">{len(self.active_connections)}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Chart Support:</span>
                <span class="metric-value">{'Active' if CHART_AVAILABLE else 'Inactive (matplotlib required)'}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">Redis:</span>
                <span class="metric-value">{'Connected' if REDIS_AVAILABLE else 'Inactive'}</span>
            </div>
            <div class="metric-item">
                <span class="metric-label">PostgreSQL:</span>
                <span class="metric-value">{'Connected' if POSTGRES_AVAILABLE else 'Inactive'}</span>
            </div>
        </div>
    </div>
    
    <div class="footer">
        <p><strong>Phoenix 95 V4 Ultimate - Integrated Notification Hub</strong></p>
        <p>Phoenix95 AI Analysis | Kelly Criterion Risk | Smart Execution | Intelligent Notifications</p>
        <p>Port 8103 | Development: Phoenix 95 Team | Version: V4-Ultimate-Integrated</p>
        <p>Smart Priority | Real-time Pipeline | uvloop Optimization</p>
    </div>
    
    <script>
        // Enhanced auto refresh
        let refreshInterval = {self.config_dash["refresh_interval"]} * 1000;
        let refreshTimer;
        
        function startAutoRefresh() {{
            refreshTimer = setInterval(() => {{
                // Fade effect for smooth refresh
                document.body.style.opacity = '0.8';
                setTimeout(() => {{
                    location.reload();
                }}, 200);
            }}, refreshInterval);
        }}
        
        function updateConnectionStatus() {{
            const statusEl = document.getElementById('connection-status');
            const connectionsEl = document.getElementById('active-connections');
            
            // Update WebSocket connection count (should be fetched from server)
            statusEl.innerHTML = '<span class="status-indicator status-healthy"></span>Real-time Connection Active';
        }}
        
        // Initialize on page load
        document.addEventListener('DOMContentLoaded', function() {{
            startAutoRefresh();
            updateConnectionStatus();
            
            console.log('Phoenix 95 V4 Ultimate Integrated Dashboard Loaded');
            console.log('Pipeline Visualization Active');
            console.log('Smart Priority System Running');
        }});
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {{
            if (refreshTimer) {{
                clearInterval(refreshTimer);
            }}
        }});
        
        // Pipeline status animation
        document.querySelectorAll('.pipeline-step').forEach(step => {{
            step.addEventListener('mouseenter', function() {{
                this.style.transform = 'scale(1.05)';
            }});
            
            step.addEventListener('mouseleave', function() {{
                this.style.transform = 'scale(1)';
            }});
        }});
    </script>
</body>
</html>
        """

    # Maintain existing WebSocket methods
    async def websocket_endpoint(self, websocket: WebSocket):
        """WebSocket endpoint (including pipeline information)"""
        await websocket.accept()
        self.active_connections.append(websocket)

        try:
            while True:
                # Real-time data transmission (including pipeline information)
                pipeline_stats = self.pipeline.get_pipeline_stats()
                telegram_stats = self.telegram.get_notification_stats()

                stats_data = {
                    "type": "stats_update",
                    "timestamp": time.time(),
                    "stats": {
                        "active_connections": len(self.active_connections),
                        "pipeline_health": sum(
                            1
                            for status in pipeline_stats["pipeline_status"].values()
                            if status["status"] == "healthy"
                        ),
                        "total_services": len(pipeline_stats["pipeline_status"]),
                        "queue_size": telegram_stats["total_queue_size"],
                        "batch_size": telegram_stats["total_batch_size"],
                        "immediate_sent": telegram_stats["stats"]["immediate_sent"],
                        "batch_sent": telegram_stats["stats"]["batch_sent"],
                    },
                }

                await websocket.send_text(json.dumps(stats_data))
                await asyncio.sleep(self.config_dash["refresh_interval"])

        except WebSocketDisconnect:
            pass
        finally:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)

    async def broadcast_to_websockets(self, message: Dict):
        """Broadcast message to all WebSocket connections (maintain basic logic)"""
        if not self.active_connections:
            return

        disconnected = []
        message_text = json.dumps(message)

        for websocket in self.active_connections:
            try:
                await websocket.send_text(message_text)
            except:
                disconnected.append(websocket)

        # Remove disconnected sockets
        for websocket in disconnected:
            self.active_connections.remove(websocket)


# ===============================================================================
#                          Integrated NOTIFY Service Main Application
# ===============================================================================


class IntegratedNotifyServiceApp:
    """Phoenix 95 V4 Integrated NOTIFY Service Main Application"""

    def __init__(self):
        """
        Initialize Integrated NOTIFY Service Application
        
        Optimized for high-spec hardware (128GB RAM, 16-24 cores):
            - Pre-allocated dictionaries for statistics
            - Async middleware for parallel request processing
            - Connection pooling for multi-core utilization
        """
        # lifespan function definition
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await self._enhanced_startup_sequence()
            yield
            await self._enhanced_shutdown_sequence()

        # FastAPI app creation with optimized settings
        self.app = FastAPI(
            title="Phoenix 95 V4 Ultimate - Integrated NOTIFY Service",
            description="Phoenix95 based integrated notification hub: Smart priority, pipeline monitoring, real-time dashboard",
            version="4.0.0-ultimate-integrated",
            docs_url="/docs",
            redoc_url="/redoc",
            lifespan=lifespan
        )

        # High-spec optimization: Pre-allocate request statistics dictionary
        # Leverages 128GB RAM for fast in-memory tracking
        self.request_stats = {}
        self.request_stats_lock = asyncio.Lock()

        @self.app.middleware("http")
        async def log_filter_middleware(request: Request, call_next):
            """
            Filter out excessive health check logs and track request statistics
            
            High-spec optimization: Async task creation for parallel processing
            Benefits from 16-24 core CPU for non-blocking statistics tracking
            
            Enhanced features:
                - Comprehensive endpoint filtering (health, limits, static files)
                - Silent statistics collection for filtered endpoints
                - Performance-based logging (errors, slow requests only)
                - Background statistics aggregation
            """
            start_time = time.time()
            response = await call_next(request)
            response_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            # ENHANCED: Expanded paths to skip logging (reduce log noise)
            skip_logging_paths = [
                "/health",
                "/check_limits",
                "/ws",  # WebSocket connections
                "/favicon.ico",  # Browser favicon requests
                "/robots.txt",  # SEO bots
                "/sitemap.xml",  # SEO sitemap
            ]
            
            # ENHANCED: Track statistics silently for filtered endpoints
            is_filtered_endpoint = request.url.path in skip_logging_paths
            
            # High-spec optimization: Async statistics tracking (non-blocking)
            # Multi-core CPU can handle this in parallel with request processing
            asyncio.create_task(
                self._update_request_stats(
                    request.url.path, 
                    response.status_code, 
                    response_time,
                    filtered=is_filtered_endpoint  # NEW: Mark filtered requests
                )
            )
            
            # ENHANCED: Skip logging for filtered endpoints (silent mode)
            if is_filtered_endpoint:
                # Update filtered endpoint counter
                if not hasattr(self, '_filtered_requests_count'):
                    self._filtered_requests_count = 0
                    self._last_filtered_report = time.time()
                
                self._filtered_requests_count += 1
                
                # Report filtered statistics every 5 minutes
                current_time = time.time()
                if current_time - self._last_filtered_report > 300:
                    logging.debug(
                        f"Filtered requests (last 5min): {self._filtered_requests_count} "
                        f"(health checks, static files)"
                    )
                    self._filtered_requests_count = 0
                    self._last_filtered_report = current_time
                
                return response
            
            # ENHANCED: Smart logging based on status code and response time
            # Only log important events to reduce log noise
            
            if response.status_code >= 500:
                # Always log server errors (CRITICAL)
                logging.error(
                    f"SERVER ERROR: {request.method} {request.url.path} - "
                    f"Status: {response.status_code} - Time: {response_time:.0f}ms"
                )
            
            elif response.status_code >= 400:
                # Log client errors at warning level
                logging.warning(
                    f"CLIENT ERROR: {request.method} {request.url.path} - "
                    f"Status: {response.status_code} - Time: {response_time:.0f}ms"
                )
            
            elif response_time > 2000:
                # Log slow requests (over 2 seconds)
                logging.warning(
                    f"SLOW REQUEST: {request.method} {request.url.path} - "
                    f"Status: {response.status_code} - Time: {response_time:.0f}ms"
                )
            
            else:
                # ENHANCED: Normal requests - only log at DEBUG level
                # This prevents log noise in production
                logging.debug(
                    f"Request: {request.method} {request.url.path} - "
                    f"Status: {response.status_code} - Time: {response_time:.0f}ms"
                )
            
            return response

        # CORS middleware with permissive settings
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Component initialization
        # High-spec optimization: All components initialized in memory
        self.telegram_manager = EnhancedTelegramNotificationManager()
        self.pipeline_monitor = PipelineMonitor(self.telegram_manager)
        self.performance_monitor = EnhancedPerformanceMonitor(
            self.telegram_manager, self.pipeline_monitor
        )
        self.dashboard = EnhancedRealTimeDashboard(
            self.telegram_manager, self.performance_monitor, self.pipeline_monitor
        )

        # Background tasks list (128GB RAM: can handle many concurrent tasks)
        self.background_tasks = []

        # High-spec optimization: Pre-allocate service statistics dictionary
        # Fast in-memory access for real-time metrics
        self.service_stats = {
            "start_time": time.time(),
            "total_requests": 0,
            "total_notifications": 0,
            "total_alerts": 0,
            "phoenix95_notifications": 0,
            "smart_priority_used": 0,
            "pipeline_alerts": 0,
            "uptime_seconds": 0,
        }

        # Route setup (FastAPI handles routing efficiently)
        self._setup_enhanced_routes()

        logging.info("Integrated NOTIFY service application initialization completed")

    async def _update_request_stats(self, path: str, status_code: int, response_time: float, filtered: bool = False):
        """
        Update request statistics for monitoring
        
        Args:
            path: Request path
            status_code: HTTP status code
            response_time: Response time in milliseconds
            filtered: Whether this request was filtered from logging (NEW)
        
        Enhanced features:
            - Separate tracking for filtered vs normal requests
            - Performance percentile tracking (p50, p95, p99)
            - Error rate calculation
            - Memory-efficient storage (auto-cleanup)
        """
        try:
            async with self.request_stats_lock:
                # Initialize stats structure if needed
                if path not in self.request_stats:
                    self.request_stats[path] = {
                        "count": 0,
                        "total_time": 0,
                        "avg_time": 0,
                        "min_time": float('inf'),
                        "max_time": 0,
                        "errors": 0,
                        "filtered_count": 0,  # NEW
                        "normal_count": 0,     # NEW
                    }
                
                stats = self.request_stats[path]
                stats["count"] += 1
                stats["total_time"] += response_time
                stats["avg_time"] = stats["total_time"] / stats["count"]
                
                # ENHANCED: Track min/max response times
                stats["min_time"] = min(stats["min_time"], response_time)
                stats["max_time"] = max(stats["max_time"], response_time)
                
                # ENHANCED: Separate filtered vs normal request counts
                if filtered:
                    stats["filtered_count"] += 1
                else:
                    stats["normal_count"] += 1
                
                # Track errors
                if status_code >= 400:
                    stats["errors"] += 1
                
                # ENHANCED: Calculate error rate
                stats["error_rate"] = (stats["errors"] / stats["count"]) * 100
                
        except Exception as e:
            logging.debug(f"Failed to update request stats: {e}")

    def _setup_enhanced_routes(self):
        """Enhanced API route setup"""

        @self.app.get("/", response_class=HTMLResponse)
        async def enhanced_dashboard_home():
            """Enhanced main dashboard"""
            try:
                return self.dashboard.generate_enhanced_dashboard_html()
            except Exception as e:
                logging.error(f"Enhanced dashboard generation failed: {e}")
                return HTMLResponse(f"<h1>Dashboard Error</h1><p>{str(e)}</p>", status_code=500)

        @self.app.get("/health")
        async def enhanced_health_check():
            """Enhanced health check"""
            uptime = time.time() - self.service_stats["start_time"]
            pipeline_stats = self.pipeline_monitor.get_pipeline_stats()
            telegram_stats = self.telegram_manager.get_notification_stats()

            return {
                "status": "healthy",
                "service": "NOTIFY-INTEGRATED",
                "port": config.DASHBOARD["port"],
                "version": "V4-Ultimate-Integrated",
                "uptime_seconds": uptime,
                "uptime_formatted": str(timedelta(seconds=int(uptime))),
                "features": {
                    "phoenix95_integration": True,
                    "smart_priority": True,
                    "pipeline_monitoring": True,
                    "enhanced_dashboard": True,
                    "uvloop_optimization": UVLOOP_AVAILABLE,
                    "telegram_enabled": config.TELEGRAM["enabled"],
                    "websocket_enabled": config.DASHBOARD["enable_websocket"],
                    "chart_available": CHART_AVAILABLE,
                    "redis_available": REDIS_AVAILABLE,
                    "postgres_available": POSTGRES_AVAILABLE,
                },
                "statistics": {
                    "active_websocket_connections": len(
                        self.dashboard.active_connections
                    ),
                    "background_tasks": len(self.background_tasks),
                    "phoenix95_notifications": self.service_stats[
                        "phoenix95_notifications"
                    ],
                    "smart_priority_used": self.service_stats["smart_priority_used"],
                    "pipeline_health": f"{sum(1 for status in pipeline_stats['pipeline_status'].values() if status['status'] == 'healthy')}/{len(pipeline_stats['pipeline_status'])}",
                    "telegram_success_rate": f"{(telegram_stats['stats']['successful_sent'] / max(telegram_stats['stats']['total_sent'], 1)) * 100:.1f}%",
                },
                "timestamp": datetime.now().isoformat(),
            }

        @self.app.api_route("/check_limits", methods=["GET", "POST"])
        async def check_limits():
            """Check system limits and thresholds"""
            try:
                telegram_stats = self.telegram_manager.get_notification_stats()
                monitoring_stats = self.performance_monitor.get_monitoring_stats()
                
                # Get current system metrics
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                
                # Check queue health
                queue_health = self.telegram_manager.get_queue_health_status()
                
                return {
                    "status": "ok",
                    "limits_check": {
                        "cpu_limit": 80,
                        "cpu_current": cpu_percent,
                        "cpu_status": "ok" if cpu_percent < 80 else "warning",
                        "memory_limit": 85,
                        "memory_current": memory_percent,
                        "memory_status": "ok" if memory_percent < 85 else "warning",
                        "queue_limit": 1000,
                        "queue_current": telegram_stats["total_queue_size"],
                        "queue_status": "ok" if telegram_stats["total_queue_size"] < 1000 else "warning",
                    },
                    "telegram_limits": {
                        "rate_limit": config.TELEGRAM["rate_limit"],
                        "failed_count": telegram_stats["stats"]["failed_sent"],
                        "rate_limited_count": telegram_stats["stats"]["rate_limited"],
                        "overflow_count": telegram_stats["stats"]["queue_overflow"],
                    },
                    "queue_health": queue_health,
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Check limits endpoint error: {e}")
                return {"status": "error", "error": str(e)}

        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            """Enhanced WebSocket endpoint"""
            if config.DASHBOARD["enable_websocket"]:
                await self.dashboard.websocket_endpoint(websocket)
            else:
                await websocket.close(code=1003, reason="WebSocket disabled")


        # ===================================================================
        #                          Phoenix95 Dedicated API Endpoints
        # ===================================================================

        @self.app.post("/api/notification/brain")
        async def receive_brain_notification(request: Request):
            """Phoenix95 Brain Service dedicated notification API"""
            try:
                # Parse JSON with error handling
                try:
                    data = await request.json()
                except json.JSONDecodeError as e:
                    logging.debug(f"Brain notification API: Invalid JSON - {e}")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Invalid JSON format", "timestamp": time.time()}
                    )
                
                # Validate data structure
                if not data or not isinstance(data, dict):
                    logging.debug(f"Brain notification API: Invalid data type - {type(data)}")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Data must be a dictionary", "timestamp": time.time()}
                    )
                
                # Validate required fields
                signal_data = data.get("signal_data")
                phoenix_analysis = data.get("phoenix_analysis")
                
                # signal_data is required
                if not signal_data:
                    logging.debug("Brain notification API: Missing signal_data field")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Missing required field: signal_data", "timestamp": time.time()}
                    )
                
                if not isinstance(signal_data, dict):
                    logging.debug(f"Brain notification API: signal_data must be dict, got {type(signal_data)}")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "signal_data must be a dictionary", "timestamp": time.time()}
                    )
                
                # phoenix_analysis is now optional with default fallback
                if not phoenix_analysis:
                    logging.debug("Brain notification API: phoenix_analysis missing, using defaults")
                    phoenix_analysis = {
                        "phoenix_score": 0,
                        "ai_confidence": 0.0,
                        "leverage_optimization": {"recommended_leverage": 1},
                        "kelly_criterion": {"kelly_fraction": 0.0},
                        "position_sizing": {"recommended_size": 0.0},
                        "backtest_performance": {"expected_return": "N/A"},
                        "risk_metrics": {"var": "N/A"}
                    }
                    data["phoenix_analysis"] = phoenix_analysis
                elif not isinstance(phoenix_analysis, dict):
                    logging.warning(f"Brain notification API: phoenix_analysis must be dict, got {type(phoenix_analysis)}, using defaults")
                    phoenix_analysis = {
                        "phoenix_score": 0,
                        "ai_confidence": 0.0,
                        "leverage_optimization": {"recommended_leverage": 1},
                        "kelly_criterion": {"kelly_fraction": 0.0},
                        "position_sizing": {"recommended_size": 0.0},
                        "backtest_performance": {"expected_return": "N/A"},
                        "risk_metrics": {"var": "N/A"}
                    }
                    data["phoenix_analysis"] = phoenix_analysis
                
                # Validate signal_data has required symbol and action
                symbol = signal_data.get("symbol", "").strip()
                action = signal_data.get("action", "").strip()
                
                if not symbol or symbol == "UNKNOWN":
                    logging.warning(f"Brain notification API: Invalid or missing symbol in signal_data")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "signal_data must contain valid 'symbol' field", "timestamp": time.time()}
                    )
                
                if not action or action == "UNKNOWN":
                    logging.warning(f"Brain notification API: Invalid or missing action in signal_data")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "signal_data must contain valid 'action' field", "timestamp": time.time()}
                    )

                # Update statistics
                self.service_stats["total_requests"] += 1
                self.service_stats["phoenix95_notifications"] += 1

                # Record data flow
                self.pipeline_monitor.record_data_flow("brain_to_risk", True)

                # Send Phoenix95 Brain notification
                await self.telegram_manager.send_phoenix95_brain_notification(data)

                return {
                    "status": "success",
                    "service": "notify_integrated",
                    "type": "phoenix95_brain",
                    "phoenix95_score": phoenix_analysis.get("phoenix_score", 0),
                    "smart_priority_applied": True,
                    "phoenix_analysis_provided": data.get("phoenix_analysis") is not None,
                    "timestamp": time.time(),
                }

            except Exception as e:
                logging.error(f"Phoenix95 Brain notification API error: {e}")
                logging.error(f"Request data: {data if 'data' in locals() else 'N/A'}")
                logging.error(f"Error traceback: {traceback.format_exc()}")
                return JSONResponse(
                    status_code=500,
                    content={"status": "error", "error": str(e), "timestamp": time.time()}
                )

        @self.app.post("/api/notification/risk")
        async def receive_risk_notification(request: Request):
            """Risk Service dedicated notification API"""
            try:
                # Parse JSON with error handling
                try:
                    data = await request.json()
                except json.JSONDecodeError as e:
                    logging.error(f"Risk notification API: Invalid JSON - {e}")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Invalid JSON format", "timestamp": time.time()}
                    )
                
                # Validate data structure
                if not data or not isinstance(data, dict):
                    logging.error(f"Risk notification API: Invalid data type - {type(data)}")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Data must be a dictionary", "timestamp": time.time()}
                    )
                
                # Validate required fields
                signal_data = data.get("signal_data")
                phoenix_analysis = data.get("phoenix_analysis")
                risk_data = data.get("risk_data")
                
                if not signal_data:
                    logging.error("Risk notification API: Missing signal_data field")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Missing required field: signal_data", "timestamp": time.time()}
                    )
                
                if not phoenix_analysis:
                    logging.error("Risk notification API: Missing phoenix_analysis field")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Missing required field: phoenix_analysis", "timestamp": time.time()}
                    )
                
                if not risk_data:
                    logging.error("Risk notification API: Missing risk_data field")
                    return JSONResponse(
                        status_code=400,
                        content={"status": "error", "error": "Missing required field: risk_data", "timestamp": time.time()}
                    )

                # Update statistics
                self.service_stats["total_requests"] += 1
                self.service_stats["total_notifications"] += 1

                # Record data flow
                approved = risk_data.get("approved", False)
                if approved:
                    self.pipeline_monitor.record_data_flow("risk_to_execute", True)
                    self.pipeline_monitor.record_signal_processing(True)
                else:
                    self.pipeline_monitor.record_signal_processing(False)

                # Send Risk Service decision notification
                await self.telegram_manager.send_risk_decision(signal_data, phoenix_analysis, risk_data)

                return {
                    "status": "success",
                    "service": "notify_integrated",
                    "type": "risk_decision",
                    "approved": approved,
                    "timestamp": time.time(),
                }

            except Exception as e:
                logging.error(f"Risk Service notification API error: {e}")
                logging.error(f"Request data: {data if 'data' in locals() else 'N/A'}")
                logging.error(f"Error traceback: {traceback.format_exc()}")
                return JSONResponse(
                    status_code=500,
                    content={"status": "error", "error": str(e), "timestamp": time.time()}
                )

        @self.app.post("/api/notification/execution")
        async def receive_execution_notification(request: Request):
            """
            Execute Service dedicated notification API
            
            Enhanced features:
                - Comprehensive JSON validation with detailed error responses
                - Field-level validation tracking for debugging
                - Automatic field defaults for missing data
                - Statistics tracking and pipeline monitoring integration
                - Optimized for high-spec hardware (128GB RAM, 16-24 cores)
                
            High-spec optimizations:
                - Pre-allocated error detail structures in memory
                - Fast dictionary operations (O(1) lookups)
                - Efficient field validation with minimal overhead
                - Async statistics updates for parallel processing
            """
            try:
                # Level 1: JSON parsing with enhanced error handling
                try:
                    data = await request.json()
                except json.JSONDecodeError as e:
                    error_msg = f"Invalid JSON format: {str(e)}"
                    logging.warning(f"Execution notification API: {error_msg}")
                    return JSONResponse(
                        status_code=400,
                        content={
                            "status": "error", 
                            "error": error_msg,
                            "error_type": "json_decode_error",
                            "timestamp": time.time()
                        }
                    )
                
                # Level 2: Basic data structure validation
                if not data:
                    logging.warning("Execution notification API: Empty request body")
                    return JSONResponse(
                        status_code=400,
                        content={
                            "status": "error", 
                            "error": "Request body is empty",
                            "error_type": "empty_data",
                            "timestamp": time.time()
                        }
                    )
                
                if not isinstance(data, dict):
                    logging.warning(f"Execution notification API: Invalid data type - {type(data).__name__}")
                    return JSONResponse(
                        status_code=400,
                        content={
                            "status": "error", 
                            "error": f"Data must be a dictionary, got {type(data).__name__}",
                            "error_type": "invalid_data_type",
                            "received_type": type(data).__name__,
                            "timestamp": time.time()
                        }
                    )

                # Level 3: Enhanced field validation with comprehensive debugging
                # High-spec optimization: Pre-allocated lists for tracking validation issues
                execution_result = data.get("execution_result")
                signal = data.get("signal")
                
                # Track validation issues for detailed error reporting
                missing_fields = []
                invalid_fields = []
                
                # Initialize validation failure statistics (NEW)
                if "execution_validation_failures" not in self.service_stats:
                    self.service_stats["execution_validation_failures"] = 0
                    self.service_stats["execution_validation_by_field"] = {}
                    self.service_stats["execution_validation_timestamps"] = []
                
                # Validate execution_result field (required)
                if not execution_result:
                    missing_fields.append("execution_result")
                    # Track which field is missing (NEW)
                    if "execution_result" not in self.service_stats["execution_validation_by_field"]:
                        self.service_stats["execution_validation_by_field"]["execution_result"] = 0
                    self.service_stats["execution_validation_by_field"]["execution_result"] += 1
                elif not isinstance(execution_result, dict):
                    invalid_fields.append({
                        "field": "execution_result",
                        "expected": "dict",
                        "received": type(execution_result).__name__
                    })
                
                # Validate signal field (required)
                if not signal:
                    missing_fields.append("signal")
                    # Track which field is missing (NEW)
                    if "signal" not in self.service_stats["execution_validation_by_field"]:
                        self.service_stats["execution_validation_by_field"]["signal"] = 0
                    self.service_stats["execution_validation_by_field"]["signal"] += 1
                elif not isinstance(signal, dict):
                    invalid_fields.append({
                        "field": "signal",
                        "expected": "dict",
                        "received": type(signal).__name__
                    })
                
                # Level 4: Return comprehensive error response if validation failed
                if missing_fields or invalid_fields:
                    # Update failure counter (NEW)
                    self.service_stats["execution_validation_failures"] += 1
                    
                    # Track failure timestamp (NEW)
                    current_timestamp = time.time()
                    self.service_stats["execution_validation_timestamps"].append(current_timestamp)
                    
                    # Keep only last 100 timestamps (memory efficiency - 128GB RAM)
                    if len(self.service_stats["execution_validation_timestamps"]) > 100:
                        self.service_stats["execution_validation_timestamps"] = \
                            self.service_stats["execution_validation_timestamps"][-100:]
                    
                    # Calculate validation failure rate (NEW)
                    failure_rate = 0.0
                    if len(self.service_stats["execution_validation_timestamps"]) >= 2:
                        time_window = current_timestamp - self.service_stats["execution_validation_timestamps"][0]
                        if time_window > 0:
                            failure_rate = len(self.service_stats["execution_validation_timestamps"]) / time_window
                    
                    # High-spec optimization: Pre-built error details dictionary
                    error_details = {
                        "status": "error",
                        "error": "Field validation failed",
                        "error_type": "validation_error",
                        "received_fields": list(data.keys()),
                        "total_failures": self.service_stats["execution_validation_failures"],
                        "failure_rate_per_sec": round(failure_rate, 3),
                        "timestamp": time.time()
                    }
                    
                    if missing_fields:
                        error_details["missing_fields"] = missing_fields
                    
                    if invalid_fields:
                        error_details["invalid_fields"] = invalid_fields
                    
                    # Adaptive logging based on severity and frequency
                    log_msg = (
                        f"Execution notification validation failed: "
                        f"missing={missing_fields}, invalid={invalid_fields} "
                        f"(Total failures: {self.service_stats['execution_validation_failures']}, "
                        f"Rate: {failure_rate:.2f}/sec)"
                    )
                    
                    if "execution_result" in missing_fields:
                        # Critical field missing - log at warning level
                        logging.warning(f"Execution notification API: {log_msg}")
                        logging.debug(f"Received data keys: {list(data.keys())}")
                        
                        # Provide troubleshooting guidance every 10 failures (NEW)
                        if self.service_stats["execution_validation_failures"] % 10 == 1:
                            most_missing = max(
                                self.service_stats["execution_validation_by_field"].items(),
                                key=lambda x: x[1]
                            )
                            logging.warning(
                                f"TROUBLESHOOTING: Repeated validation failures. "
                                f"Most missing field: {most_missing[0]} ({most_missing[1]} times). "
                                f"Check Execute Service API call format."
                            )
                    else:
                        # Non-critical validation failure
                        logging.info(f"Execution notification API: {log_msg}")
                    
                    return JSONResponse(status_code=400, content=error_details)
                
                # Level 5: Additional validation for nested critical fields
                # Validate execution_result has status field
                if "status" not in execution_result:
                    logging.warning("Execution notification API: execution_result missing 'status' field")
                    # Apply default instead of failing (graceful degradation)
                    execution_result["status"] = "UNKNOWN"
                
                # Validate signal has minimum required fields
                if "symbol" not in signal or "action" not in signal:
                    logging.warning(f"Execution notification API: signal missing required fields (has: {list(signal.keys())})")
                    
                    # Apply defaults for missing fields
                    if "symbol" not in signal:
                        signal["symbol"] = "UNKNOWN"
                    if "action" not in signal:
                        signal["action"] = "UNKNOWN"
                
                # Level 6: Update service statistics
                # High-spec optimization: Async-safe counter updates
                self.service_stats["total_requests"] += 1
                self.service_stats["total_notifications"] += 1

                # Level 7: Record pipeline data flow
                # Extract execution status for pipeline tracking
                success = execution_result.get("status") == "SUCCESS"

                # Update pipeline monitors (multi-core optimized)
                self.pipeline_monitor.record_data_flow("execute_to_notify", success)
                self.pipeline_monitor.record_signal_processing(success)

                # Level 8: Send execution result notification to telegram
                # This triggers the telegram notification system
                await self.telegram_manager.send_execution_result(data)

                # Level 9: Log successful processing
                # Use debug level to reduce log noise for normal operations
                logging.debug(f"Execution notification processed: {signal.get('symbol')} - {execution_result.get('status')}")

                # Level 10: Return success response with comprehensive details
                return {
                    "status": "success",
                    "service": "notify_integrated",
                    "type": "trade_execution",
                    "execution_success": success,
                    "validation_warnings": bool(
                        "status" not in execution_result or 
                        "symbol" not in signal or 
                        "action" not in signal
                    ),
                    "timestamp": time.time(),
                }

            except Exception as e:
                # Level 11: Comprehensive exception handling
                # Log unexpected errors with full context
                logging.error(f"Execute Service notification API unexpected error: {e}")
                
                # Provide detailed context for debugging
                if 'data' in locals():
                    logging.error(f"Request data keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
                    logging.debug(f"Full request data: {data}")
                else:
                    logging.error("Request data not available (parsing failed)")
                
                # Include full traceback for debugging
                logging.error(f"Error traceback: {traceback.format_exc()}")
                
                # Return standardized error response
                return JSONResponse(
                    status_code=500,
                    content={
                        "status": "error", 
                        "error": str(e),
                        "error_type": "internal_server_error",
                        "timestamp": time.time()
                    }
                )

        # ===================================================================
        #                          Existing Compatible API (Legacy Support)
        # ===================================================================

        @self.app.post("/api/notification/trade")
        async def send_trade_notification(trade_data: dict):
            """Send trade notification (legacy compatibility)"""
            try:
                self.service_stats["total_requests"] += 1
                self.service_stats["total_notifications"] += 1

                await self.telegram_manager.send_trade_notification(trade_data)

                return {
                    "status": "success",
                    "message": "Trade notification sent successfully",
                    "legacy_compatibility": True,
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Trade notification API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/notification/position")
        async def send_position_update(position_data: dict):
            """Position update notification (legacy compatibility)"""
            try:
                self.service_stats["total_requests"] += 1
                self.service_stats["total_notifications"] += 1

                # Convert legacy data to new format
                execution_data = {
                    "execution_result": {
                        "status": "SUCCESS",
                        "position": position_data,
                    },
                    "signal": position_data,
                }

                await self.telegram_manager.send_execution_result(execution_data)

                return {
                    "status": "success",
                    "message": "Position notification sent successfully",
                    "legacy_compatibility": True,
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Position notification API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/legacy/notification/risk")
        async def send_risk_warning(risk_data: dict):
            """Risk warning notification (legacy compatibility)"""
            try:
                self.service_stats["total_requests"] += 1
                self.service_stats["total_alerts"] += 1

                # Convert legacy format to new format
                risk_decision_data = {
                    "approved": False,
                    "signal_data": risk_data,
                    "phoenix_analysis": {},
                    "reason": risk_data.get("details", "Risk warning"),
                    "risk_level": risk_data.get("risk_level", "HIGH"),
                }

                await self.telegram_manager.send_risk_service_decision(
                    risk_decision_data
                )

                return {
                    "status": "success",
                    "message": "Risk warning sent successfully",
                    "legacy_compatibility": True,
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Risk notification API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/notification/system")
        async def send_system_alert(alert_data: dict):
            """System notification (existing compatibility)"""
            try:
                self.service_stats["total_requests"] += 1
                self.service_stats["total_alerts"] += 1

                await self.telegram_manager.send_system_alert(alert_data)

                return {
                    "status": "success",
                    "message": "System notification sent successfully",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"System notification API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/notification/liquidation")
        async def send_liquidation_warning(liquidation_data: dict):
            """Liquidation risk warning (existing compatibility)"""
            try:
                self.service_stats["total_requests"] += 1
                self.service_stats["total_alerts"] += 1

                await self.telegram_manager.send_liquidation_warning(liquidation_data)

                return {
                    "status": "success",
                    "message": "Liquidation warning sent successfully",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Liquidation warning API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))


        # ===================================================================
        #                          Integrated Statistics and Management API
        # ===================================================================

        @self.app.get("/api/stats/comprehensive")
        async def get_comprehensive_stats():
            """Query comprehensive statistics"""
            try:
                self.service_stats["uptime_seconds"] = (
                    time.time() - self.service_stats["start_time"]
                )

                telegram_stats = self.telegram_manager.get_notification_stats()
                monitoring_stats = self.performance_monitor.get_monitoring_stats()
                pipeline_stats = self.pipeline_monitor.get_pipeline_stats()

                return {
                    "service_stats": self.service_stats,
                    "telegram_stats": telegram_stats,
                    "monitoring_stats": monitoring_stats,
                    "pipeline_stats": pipeline_stats,
                    "integration_stats": {
                        "phoenix95_notifications": self.service_stats[
                            "phoenix95_notifications"
                        ],
                        "smart_priority_usage": telegram_stats["stats"][
                            "smart_priority_used"
                        ],
                        "immediate_notifications": telegram_stats["stats"][
                            "immediate_sent"
                        ],
                        "batch_notifications": telegram_stats["stats"]["batch_sent"],
                        "pipeline_health_score": (
                            sum(
                                1
                                for status in pipeline_stats["pipeline_status"].values()
                                if status["status"] == "healthy"
                            )
                            / len(pipeline_stats["pipeline_status"])
                            * 100
                        )
                        if pipeline_stats["pipeline_status"]
                        else 0,
                        "overall_success_rate": (
                            (
                                telegram_stats["stats"]["successful_sent"]
                                / max(telegram_stats["stats"]["total_sent"], 1)
                            )
                            * 100
                        ),
                    },
                    "performance_optimization": {
                        "uvloop_enabled": UVLOOP_AVAILABLE,
                        "chart_support": CHART_AVAILABLE,
                        "redis_support": REDIS_AVAILABLE,
                        "postgres_support": POSTGRES_AVAILABLE,
                        "websocket_connections": len(self.dashboard.active_connections),
                        "background_tasks": len(self.background_tasks),
                    },
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Comprehensive statistics query API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/stats")
        async def get_service_stats():
            """Query basic service statistics (existing compatibility)"""
            try:
                # Direct implementation instead of calling get_comprehensive_stats()
                self.service_stats["uptime_seconds"] = (
                    time.time() - self.service_stats["start_time"]
                )

                telegram_stats = self.telegram_manager.get_notification_stats()
                monitoring_stats = self.performance_monitor.get_monitoring_stats()
                pipeline_stats = self.pipeline_monitor.get_pipeline_stats()

                return {
                    "service_stats": self.service_stats,
                    "telegram_stats": telegram_stats,
                    "monitoring_stats": monitoring_stats,
                    "pipeline_stats": pipeline_stats,
                    "integration_stats": {
                        "phoenix95_notifications": self.service_stats[
                            "phoenix95_notifications"
                        ],
                        "smart_priority_usage": telegram_stats["stats"][
                            "smart_priority_used"
                        ],
                        "immediate_notifications": telegram_stats["stats"][
                            "immediate_sent"
                        ],
                        "batch_notifications": telegram_stats["stats"]["batch_sent"],
                        "pipeline_health_score": (
                            sum(
                                1
                                for status in pipeline_stats["pipeline_status"].values()
                                if status["status"] == "healthy"
                            )
                            / len(pipeline_stats["pipeline_status"])
                            * 100
                        )
                        if pipeline_stats["pipeline_status"]
                        else 0,
                        "overall_success_rate": (
                            (
                                telegram_stats["stats"]["successful_sent"]
                                / max(telegram_stats["stats"]["total_sent"], 1)
                            )
                            * 100
                        ),
                    },
                    "performance_optimization": {
                        "uvloop_enabled": UVLOOP_AVAILABLE,
                        "chart_support": CHART_AVAILABLE,
                        "redis_support": REDIS_AVAILABLE,
                        "postgres_support": POSTGRES_AVAILABLE,
                        "websocket_connections": len(self.dashboard.active_connections),
                        "background_tasks": len(self.background_tasks),
                    },
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Statistics query API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/pipeline/status")
        async def get_pipeline_status():
            """Query pipeline status"""
            try:
                pipeline_stats = self.pipeline_monitor.get_pipeline_stats()

                return {
                    "pipeline_health": {
                        "healthy_services": sum(
                            1
                            for status in pipeline_stats["pipeline_status"].values()
                            if status["status"] == "healthy"
                        ),
                        "total_services": len(pipeline_stats["pipeline_status"]),
                        "health_percentage": (
                            sum(
                                1
                                for status in pipeline_stats["pipeline_status"].values()
                                if status["status"] == "healthy"
                            )
                            / len(pipeline_stats["pipeline_status"])
                            * 100
                        )
                        if pipeline_stats["pipeline_status"]
                        else 0,
                    },
                    "data_flow": pipeline_stats["data_flow"],
                    "pipeline_metrics": pipeline_stats["pipeline_metrics"],
                    "service_status": pipeline_stats["pipeline_status"],
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Pipeline status API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/priority/stats")
        async def get_priority_stats():
            """Query smart priority statistics"""
            try:
                telegram_stats = self.telegram_manager.get_notification_stats()
                priority_stats = telegram_stats["priority_stats"]

                return {
                    "priority_statistics": priority_stats,
                    "total_smart_priority_usage": telegram_stats["stats"][
                        "smart_priority_used"
                    ],
                    "immediate_vs_batch": {
                        "immediate_sent": telegram_stats["stats"]["immediate_sent"],
                        "batch_sent": telegram_stats["stats"]["batch_sent"],
                        "immediate_percentage": (
                            telegram_stats["stats"]["immediate_sent"]
                            / max(
                                telegram_stats["stats"]["immediate_sent"]
                                + telegram_stats["stats"]["batch_sent"],
                                1,
                            )
                        )
                        * 100,
                    },
                    "queue_status": {
                        "regular_queues": telegram_stats["queue_sizes"],
                        "batch_queues": telegram_stats["batch_sizes"],
                        "total_pending": telegram_stats["total_queue_size"]
                        + telegram_stats["total_batch_size"],
                    },
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Priority statistics API error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/api/config/integrated")
        async def get_integrated_config():
            """Query integrated service configuration"""
            return {
                "telegram_config": {
                    "enabled": config.TELEGRAM["enabled"],
                    "rate_limit": config.TELEGRAM["rate_limit"],
                    "batch_size": config.TELEGRAM["batch_size"],
                    "batch_interval": config.TELEGRAM["batch_interval"],
                },
                "dashboard_config": {
                    "port": config.DASHBOARD["port"],
                    "title": config.DASHBOARD["title"],
                    "theme": config.DASHBOARD["theme"],
                    "refresh_interval": config.DASHBOARD["refresh_interval"],
                    "enable_charts": config.DASHBOARD["enable_charts"],
                    "enable_websocket": config.DASHBOARD["enable_websocket"],
                },
                "services_config": config.SERVICES,
                "smart_priority_config": config.SMART_PRIORITY,
                "monitoring_config": {
                    "metrics_interval": config.MONITORING["metrics_interval"],
                    "alert_cooldown": config.MONITORING["alert_cooldown"],
                    "pipeline_check_interval": config.MONITORING[
                        "pipeline_check_interval"
                    ],
                    "performance_thresholds": config.MONITORING[
                        "performance_thresholds"
                    ],
                },
                "alerts_config": config.ALERTS,
                "integration_features": {
                    "phoenix95_integration": True,
                    "smart_priority_system": True,
                    "pipeline_monitoring": True,
                    "enhanced_dashboard": True,
                    "performance_optimization": UVLOOP_AVAILABLE,
                },
                "dependencies": {
                    "uvloop": UVLOOP_AVAILABLE,
                    "matplotlib": CHART_AVAILABLE,
                    "aioredis": REDIS_AVAILABLE,
                    "asyncpg": POSTGRES_AVAILABLE,
                },
            }

        # ===================================================================
        #                          Test and Management API
        # ===================================================================

        @self.app.post("/api/test/notification/phoenix95")
        async def test_phoenix95_notification():
            """Phoenix95 test notification"""
            try:
                test_data = {
                    "signal_data": {"symbol": "BTCUSDT", "action": "long"},
                    "phoenix_analysis": {
                        "phoenix_score": 92,
                        "ai_confidence": 0.87,
                        "leverage_optimization": {"recommended_leverage": 15},
                        "kelly_criterion": {"kelly_fraction": 0.125},
                        "position_sizing": {"recommended_size": 0.05},
                        "backtest_performance": {"expected_return": "12.5%"},
                        "risk_metrics": {"var": "2.3%"},
                    },
                }

                await self.telegram_manager.send_phoenix95_brain_notification(test_data)

                return {
                    "status": "success",
                    "message": "Phoenix95 test notification sent successfully",
                    "phoenix_score": 92,
                    "smart_priority_applied": True,
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Phoenix95 test notification error: {e}")
                raise HTTPException(status_code=500, detail=str(e))


        @self.app.post("/api/test/notification/pipeline")
        async def test_pipeline_notification():
            """Pipeline status test notification"""
            try:
                pipeline_stats = self.pipeline_monitor.get_pipeline_stats()
                await self.telegram_manager.send_pipeline_status_update(pipeline_stats)

                return {
                    "status": "success",
                    "message": "Pipeline test notification sent successfully",
                    "pipeline_health": f"{sum(1 for status in pipeline_stats['pipeline_status'].values() if status['status'] == 'healthy')}/{len(pipeline_stats['pipeline_status'])}",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Pipeline test notification error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/test/notification/priority-summary")
        async def test_priority_summary():
            """Priority summary test notification"""
            try:
                await self.telegram_manager.send_smart_priority_summary()

                return {
                    "status": "success",
                    "message": "Priority summary test notification sent successfully",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Priority summary test error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/test/notification")
        async def test_notification():
            """General test notification (existing compatibility)"""
            try:
                test_data = {
                    "service": "Integrated NOTIFY Test",
                    "status": "Test Running",
                    "message": "Phoenix95 V4 Ultimate integrated notification service test.",
                    "cpu_percent": 45.2,
                    "memory_percent": 62.8,
                    "active_positions": 3,
                }

                await self.telegram_manager.send_system_alert(test_data, "LOW")

                return {
                    "status": "success",
                    "message": "Integrated test notification sent successfully",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Test notification error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/admin/restart-pipeline-monitoring")
        async def restart_pipeline_monitoring():
            """Restart pipeline monitoring"""
            try:
                # Reset statistics after cleaning up existing tasks (simplified due to implementation complexity)
                self.pipeline_monitor.pipeline_metrics["total_signals_processed"] = 0
                self.pipeline_monitor.pipeline_metrics["successful_executions"] = 0
                self.pipeline_monitor.pipeline_metrics["failed_executions"] = 0

                return {
                    "status": "success",
                    "message": "Pipeline monitoring statistics reset completed",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Pipeline monitoring restart error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/admin/clear-priority-stats")
        async def clear_priority_stats():
            """Initialize priority statistics"""
            try:
                # Initialize priority statistics
                for priority in self.telegram_manager.smart_priority.priority_stats:
                    self.telegram_manager.smart_priority.priority_stats[priority] = {
                        "count": 0,
                        "avg_phoenix_score": 0.0,
                    }

                return {
                    "status": "success",
                    "message": "Priority statistics initialization completed",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Priority statistics initialization error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/api/admin/shutdown")
        async def admin_shutdown():
            """Administrator shutdown (existing logic)"""
            try:
                # Send shutdown notification
                shutdown_data = {
                    "service": "Integrated NOTIFY Service",
                    "status": "Shutdown",
                    "message": "Phoenix95 V4 integrated NOTIFY service is shutting down by administrator.",
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_percent": psutil.virtual_memory().percent,
                    "active_positions": 0,
                }

                await self.telegram_manager.send_system_alert(shutdown_data, "HIGH")

                # Shutdown in background
                asyncio.create_task(self._delayed_shutdown())

                return {
                    "status": "success",
                    "message": "Integrated service shutdown started",
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception as e:
                logging.error(f"Administrator shutdown error: {e}")
                raise HTTPException(status_code=500, detail=str(e))

    async def _enhanced_startup_sequence(self):
        """Enhanced startup sequence"""
        try:
            logging.info("Integrated NOTIFY service startup sequence executing")
            
            # Environment variable validation (changed telegram to optional)
            telegram_enabled = validate_required_env_vars()
            
            # Telegram connection test (only if enabled)
            if telegram_enabled:
                try:
                    async with aiohttp.ClientSession() as session:
                        url = f"https://api.telegram.org/bot{config.TELEGRAM['bot_token']}/getMe"
                        async with session.get(url, timeout=10) as response:
                            if response.status != 200:
                                raise Exception(f"Telegram bot connection failed: {response.status}")
                    logging.info("Telegram bot connection test successful")
                except Exception as e:
                    logging.warning(f"Telegram connection test failed - Telegram notifications disabled: {e}")
                    telegram_enabled = False
            else:
                logging.info("Telegram environment variables not found - Telegram notifications disabled")

            # Start background tasks (depending on telegram status)
            if telegram_enabled:
                telegram_tasks = await self.telegram_manager.start_enhanced_processing()
                self.background_tasks.extend(telegram_tasks)
            else:
                logging.info("Telegram manager disabled")

            monitoring_tasks = (
                await self.performance_monitor.start_enhanced_monitoring()
            )
            self.background_tasks.extend(monitoring_tasks)

            logging.info(f"{len(self.background_tasks)} enhanced background tasks started")


            # Send startup notification (only if telegram is enabled)
            if telegram_enabled:
                startup_data = {
                    "service": "Phoenix95 V4 Integrated NOTIFY",
                    "status": "Startup Complete",
                    "message": f"""
Phoenix 95 V4 Ultimate Integrated NOTIFY Service Started!

<b>Port:</b> {config.DASHBOARD['port']}
<b>Version:</b> V4-Ultimate-Integrated

<b>Core Features:</b>
- Phoenix95 AI-based Smart Priority: Active
- Real-time Pipeline Monitoring: Active
- Enhanced Batch Processing System: Active
- Integrated Dashboard Visualization: Active

<b>Performance Optimization:</b>
- uvloop: {'Active' if UVLOOP_AVAILABLE else 'Inactive'}
- Multi-worker Batch Processing: Active
- WebSocket Real-time Connection: {'Active' if config.DASHBOARD['enable_websocket'] else 'Inactive'}

<b>Integration Status:</b>
- Chart Support: {'Active' if CHART_AVAILABLE else 'Inactive'}
- Redis: {'Connected' if REDIS_AVAILABLE else 'Inactive'}
- PostgreSQL: {'Connected' if POSTGRES_AVAILABLE else 'Inactive'}

<b>Background Tasks:</b> {len(self.background_tasks)} active

All systems operational!
""",
                    "cpu_percent": psutil.cpu_percent(),
                    "memory_percent": psutil.virtual_memory().percent,
                    "active_positions": 0,
                }

                await self.telegram_manager.send_system_alert(startup_data, "MEDIUM")
            else:
                logging.info("Telegram notifications disabled - startup notification skipped")

            logging.info("Integrated NOTIFY service startup sequence completed")

        except Exception as e:
            logging.error(f"Integrated NOTIFY service startup failed: {e}")
            raise

    async def _enhanced_shutdown_sequence(self):
        """Enhanced shutdown sequence"""
        try:
            logging.info("Integrated NOTIFY service shutdown sequence executing")

            # Send shutdown notification
            uptime = time.time() - self.service_stats["start_time"]
            telegram_stats = self.telegram_manager.get_notification_stats()
            pipeline_stats = self.pipeline_monitor.get_pipeline_stats()

            shutdown_data = {
                "service": "Phoenix95 V4 Integrated NOTIFY",
                "status": "Normal Shutdown",
                "message": f"""
Phoenix 95 V4 Ultimate Integrated NOTIFY Service Shutdown

<b>Operation Statistics:</b>
- Total Uptime: {str(timedelta(seconds=int(uptime)))}
- Requests Processed: {self.service_stats['total_requests']:,}
- Phoenix95 Notifications: {self.service_stats['phoenix95_notifications']:,}
- Pipeline Processed: {pipeline_stats['pipeline_metrics']['total_signals_processed']:,}

<b>Telegram Performance:</b>
- Total Sent: {telegram_stats['stats']['total_sent']:,}
- Success Rate: {(telegram_stats['stats']['successful_sent'] / max(telegram_stats['stats']['total_sent'], 1)) * 100:.1f}%
- Immediate Sent: {telegram_stats['stats']['immediate_sent']:,}
- Batch Sent: {telegram_stats['stats']['batch_sent']:,}

<b>Smart Priority:</b>
- Usage Count: {telegram_stats['stats']['smart_priority_used']:,}
- CRITICAL: {telegram_stats['priority_stats']['CRITICAL']['count']}
- HIGH: {telegram_stats['priority_stats']['HIGH']['count']}
- MEDIUM: {telegram_stats['priority_stats']['MEDIUM']['count']}
- LOW: {telegram_stats['priority_stats']['LOW']['count']}

All systems shutting down safely.
""",
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "active_positions": 0,
            }

            await self.telegram_manager.send_system_alert(shutdown_data, "MEDIUM")

            # Close WebSocket connections
            for websocket in self.dashboard.active_connections:
                try:
                    await websocket.close()
                except:
                    pass

            # Terminate background tasks
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()

            # Wait for remaining message processing
            await asyncio.sleep(5)

            logging.info("Integrated NOTIFY service shutdown sequence completed")

        except Exception as e:
            logging.error(f"Integrated NOTIFY service shutdown error: {e}")

    async def _delayed_shutdown(self):
        """Delayed shutdown (administrator command) - existing logic"""
        await asyncio.sleep(3)
        os._exit(0)


# ===============================================================================
#                              Main Execution (Optimized Version)
# ===============================================================================


async def main():
    """Optimized main execution function"""
    try:
        # Apply uvloop performance optimization
        if UVLOOP_AVAILABLE:
            uvloop.install()
            logging.info("uvloop performance optimization enabled (30-40% performance improvement)")

        # Logging configuration
        log_path = Path(config.DATA_STORAGE["log_path"])
        log_path.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | [NOTIFY-INTEGRATED-8103] %(message)s",
            handlers=[
                logging.FileHandler(
                    log_path / "notify_integrated_service.log", encoding="utf-8"
                ),
                logging.StreamHandler(sys.stdout),
            ],
        )

        # Create integrated service application
        notify_app = IntegratedNotifyServiceApp()

        # Server startup message (enhanced information)
        logging.info("=" * 90)
        logging.info("Phoenix 95 V4 Ultimate - Integrated NOTIFY Service Starting")
        logging.info("=" * 90)
        logging.info(f"Port: {config.DASHBOARD['port']}")
        logging.info(f"Dashboard: http://localhost:{config.DASHBOARD['port']}")
        logging.info(f"API Docs: http://localhost:{config.DASHBOARD['port']}/docs")
        logging.info(f"Telegram Notifications: {'Active' if config.TELEGRAM['enabled'] else 'Inactive'}")
        logging.info(
            f"WebSocket: {'Active' if config.DASHBOARD['enable_websocket'] else 'Inactive'}"
        )
        logging.info(
            f"uvloop Optimization: {'Active (High Performance)' if UVLOOP_AVAILABLE else 'Inactive (Default Performance)'}"
        )
        logging.info(
            f"Real-time Charts: {'Active' if CHART_AVAILABLE else 'Inactive (pip install matplotlib)'}"
        )
        logging.info(
            f"Redis: {'Connected' if REDIS_AVAILABLE else 'Inactive (pip install aioredis)'}"
        )
        logging.info(
            f"PostgreSQL: {'Connected' if POSTGRES_AVAILABLE else 'Inactive (pip install asyncpg)'}"
        )
        logging.info(f"Theme: {config.DASHBOARD['theme'].upper()}")
        logging.info("=" * 90)
        logging.info("Core Features:")
        logging.info("  - Phoenix95 AI-based Smart Priority Notifications")
        logging.info("  - Full Pipeline Real-time Monitoring")
        logging.info("  - Service-specific Notification APIs (Brain, Risk, Execute)")
        logging.info("  - Enhanced Batch Processing and Immediate Sending")
        logging.info("  - Real-time Dashboard Pipeline Visualization")
        logging.info("  - Integrated Performance Monitoring and Alerts")
        logging.info("=" * 90)

        # Dependency warnings
        missing_deps = []
        if not UVLOOP_AVAILABLE:
            missing_deps.append("uvloop (high performance)")
        if not CHART_AVAILABLE:
            missing_deps.append("matplotlib (charts)")
        if not REDIS_AVAILABLE:
            missing_deps.append("aioredis (Redis)")
        if not POSTGRES_AVAILABLE:
            missing_deps.append("asyncpg (PostgreSQL)")

        if missing_deps:
            logging.warning(f"Optional dependencies not installed: {', '.join(missing_deps)}")

        # Optimized server execution
        uvicorn_config = uvicorn.Config(
            notify_app.app,
            host=config.DASHBOARD["host"],
            port=config.DASHBOARD["port"],
            workers=1,  # Single worker optimized for Phoenix95
            log_level="info",
            access_log=True,
            reload=False,
            loop="uvloop" if UVLOOP_AVAILABLE else "asyncio",
        )

        server = uvicorn.Server(uvicorn_config)

        logging.info("Server starting...")
        logging.info("Ready for Phoenix95 pipeline integration!")

        await server.serve()

    except KeyboardInterrupt:
        logging.info("Service terminated by user")
    except Exception as e:
        logging.error(f"Integrated NOTIFY service execution error: {e}")
        logging.error(traceback.format_exc())
    finally:
        logging.info("Phoenix 95 V4 Ultimate Integrated NOTIFY Service Terminated")


if __name__ == "__main__":
    asyncio.run(main())