"""
Centralized configuration using pydantic-settings.
All values can be overridden via environment variables or a .env file.
"""
from __future__ import annotations

from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ----- Mode -----
    bot_mode: str = Field("paper", description="collect | paper | backtest | live")

    # ----- Binance -----
    binance_api_key: str = Field("", description="Binance API key")
    binance_api_secret: str = Field("", description="Binance API secret")
    binance_taker_fee: float = Field(0.001, description="Taker fee fraction (0.001 = 0.1%)")
    binance_ws_url: str = Field(
        "wss://stream.binance.com:9443/stream",
        description="Binance combined WebSocket stream URL",
    )

    # ----- DEX RPC Endpoints -----
    eth_ws_url: str = Field("", description="Ethereum mainnet WebSocket RPC")
    arb_ws_url: str = Field("", description="Arbitrum WebSocket RPC")
    bsc_ws_url: str = Field("", description="BSC WebSocket RPC")
    solana_ws_url: str = Field("", description="Solana WebSocket RPC (Helius)")

    # ----- Wallet -----
    private_key: str = Field("", description="Ethereum private key for live trading")

    # ----- Strategy -----
    min_profit_pct: float = Field(0.005, description="Min net profit fraction (0.005 = 0.5%)")
    max_position_usd: float = Field(500.0, description="Max USD per trade")
    min_pool_volume_usd: float = Field(100_000.0, description="Min 24h DEX pool volume USD")
    top_n_pairs: int = Field(20, description="Number of top alt pairs to track")

    # ----- Backtest -----
    backtest_start: str = Field("2025-01-01", description="Backtest start date YYYY-MM-DD")
    backtest_end: str = Field("2025-02-01", description="Backtest end date YYYY-MM-DD")
    sweep_thresholds: str = Field(
        "0.001,0.0025,0.005,0.01,0.02",
        description="Comma-separated profit threshold sweep values",
    )

    # ----- Database -----
    db_path: str = Field("./spreadbot.db", description="SQLite database file path")

    # ----- Dashboard -----
    port: int = Field(8080, description="Web dashboard HTTP port")
    dashboard_host: str = Field("0.0.0.0", description="Host to bind the dashboard server")

    # ----- Logging -----
    log_level: str = Field("INFO", description="Logging level")
    log_json: bool = Field(False, description="Emit structured JSON logs")

    # ----- Derived -----
    @property
    def sweep_threshold_list(self) -> List[float]:
        return [float(x.strip()) for x in self.sweep_thresholds.split(",")]

    @field_validator("min_profit_pct")
    @classmethod
    def validate_min_profit(cls, v: float) -> float:
        if not (0 < v < 1.0):
            raise ValueError(f"min_profit_pct must be between 0 and 1, got {v}")
        return v

    @field_validator("bot_mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        valid = {"collect", "paper", "backtest", "live"}
        if v not in valid:
            raise ValueError(f"bot_mode must be one of {valid}, got {v!r}")
        return v

    def is_live(self) -> bool:
        return self.bot_mode == "live"

    def has_binance_creds(self) -> bool:
        return bool(self.binance_api_key and self.binance_api_secret)

    def has_wallet(self) -> bool:
        return bool(self.private_key)


# Singleton — import this everywhere
settings = Config()
