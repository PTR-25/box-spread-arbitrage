use std::path::PathBuf;

use clap::{Parser, Subcommand};
use deribit_tick_collector::config::BookInterval;
use tracing_subscriber::EnvFilter;

use box_spread_arbitrage::config::ArbConfig;
use box_spread_arbitrage::live::{run_live_scanner, run_rates_snapshot};
use box_spread_arbitrage::replay::{print_replay_report, run_replay};

#[derive(Debug, Parser)]
#[command(
    name = "box-spread-arbitrage",
    version,
    about = "Event-driven Rust static options arbitrage scanner & tick-by-tick Parquet backtester for Deribit inverse and linear USDC options"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the live event-driven scanner on Deribit public WebSockets (no API keys required).
    Live {
        /// Optional path to TOML config file.
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Comma-separated markets to scan (e.g. BTC,ETH,BTC_USDC,SOL_USDC or ALL).
        #[arg(long, value_delimiter = ',')]
        markets: Option<Vec<String>>,

        /// Order book granularity (raw, 100ms, agg2).
        #[arg(long, value_enum)]
        book_interval: Option<BookInterval>,

        /// Minimum total post-fee profit across swept L2 depth (USD/USDC).
        #[arg(long)]
        min_profit_usd: Option<f64>,

        /// Minimum BBO net edge in basis points of spot index.
        #[arg(long)]
        min_edge_bps: Option<f64>,

        /// Annualized USDC benchmark financing rate (e.g. 0.05 = 5%).
        #[arg(long)]
        usdc_rate: Option<f64>,

        /// Annualized coin (BTC/ETH) benchmark lending rate (e.g. 0.0 = 0%).
        #[arg(long)]
        coin_rate: Option<f64>,

        /// Disable box spread scanning.
        #[arg(long)]
        no_box: bool,

        /// Disable put-call parity (conversion/reversal) scanning.
        #[arg(long)]
        no_parity: bool,

        /// Disable vertical spread monotonicity & payoff-cap scanning.
        #[arg(long)]
        no_verticals: bool,

        /// Disable butterfly convexity scanning.
        #[arg(long)]
        no_butterflies: bool,

        /// Optional finite run duration in seconds.
        #[arg(long)]
        duration_secs: Option<u64>,

        /// Heartbeat stats log interval in seconds.
        #[arg(long, default_value_t = 15)]
        stats_interval_secs: u64,

        /// Optional JSONL file path to log opened/closed arbitrage episodes.
        #[arg(long)]
        output_jsonl: Option<PathBuf>,

        /// Emit structured JSON logs.
        #[arg(long)]
        json_logs: bool,
    },

    /// Replay a deribit-tick-collector Parquet dataset tick-by-tick with episode & latency-decay stats.
    Replay {
        /// Path to a deribit-tick-collector Parquet directory or file.
        path: PathBuf,

        /// Optional path to TOML config file.
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Comma-separated markets to include (e.g. BTC,BTC_USDC,SOL_USDC or ALL).
        #[arg(long, value_delimiter = ',')]
        markets: Option<Vec<String>>,

        /// Minimum total post-fee profit across swept L2 depth (USD/USDC).
        #[arg(long)]
        min_profit_usd: Option<f64>,

        /// Minimum BBO net edge in basis points of spot index.
        #[arg(long)]
        min_edge_bps: Option<f64>,

        /// Annualized USDC benchmark financing rate.
        #[arg(long)]
        usdc_rate: Option<f64>,

        /// Annualized coin benchmark lending rate.
        #[arg(long)]
        coin_rate: Option<f64>,

        /// Comma-separated synthetic latency horizons in ms (default: 5,20,50,150).
        #[arg(long, value_delimiter = ',')]
        latencies: Option<Vec<i64>>,

        /// Disable box spreads.
        #[arg(long)]
        no_box: bool,

        /// Disable conversions/reversals.
        #[arg(long)]
        no_parity: bool,

        /// Disable verticals.
        #[arg(long)]
        no_verticals: bool,

        /// Disable butterflies.
        #[arg(long)]
        no_butterflies: bool,

        /// Optional JSONL output file for full episode records.
        #[arg(long)]
        output_jsonl: Option<PathBuf>,
    },

    /// Capture a live snapshot of ATM box-implied financing rates vs. futures carry & perp funding.
    Rates {
        /// Optional path to TOML config file.
        #[arg(short, long)]
        config: Option<PathBuf>,

        /// Comma-separated markets to sample (e.g. BTC,ETH,BTC_USDC,SOL_USDC).
        #[arg(long, value_delimiter = ',')]
        markets: Option<Vec<String>>,

        /// Warmup snapshot collection time in seconds (default: 6).
        #[arg(long, default_value_t = 6)]
        warmup_secs: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cli = Cli::parse();

    match cli.command {
        Command::Live {
            config,
            markets,
            book_interval,
            min_profit_usd,
            min_edge_bps,
            usdc_rate,
            coin_rate,
            no_box,
            no_parity,
            no_verticals,
            no_butterflies,
            duration_secs,
            stats_interval_secs,
            output_jsonl,
            json_logs,
        } => {
            init_logging(json_logs);
            let mut cfg = ArbConfig::load(config.as_deref())?;
            if let Some(m) = markets {
                cfg.scanner.markets = m;
            }
            if let Some(i) = book_interval {
                cfg.scanner.book_interval = i;
            }
            if let Some(p) = min_profit_usd {
                cfg.scanner.min_profit_usd = p;
            }
            if let Some(e) = min_edge_bps {
                cfg.scanner.min_edge_bps = e;
            }
            if let Some(r) = usdc_rate {
                cfg.rates.usdc_benchmark_rate = r;
            }
            if let Some(r) = coin_rate {
                cfg.rates.coin_benchmark_rate = r;
            }
            if no_box {
                cfg.scanner.enable_box_spreads = false;
            }
            if no_parity {
                cfg.scanner.enable_parity = false;
            }
            if no_verticals {
                cfg.scanner.enable_verticals = false;
            }
            if no_butterflies {
                cfg.scanner.enable_butterflies = false;
            }
            if let Some(out) = output_jsonl {
                cfg.replay.episodes_output = Some(out);
            }
            cfg.normalize();
            cfg.validate()?;
            run_live_scanner(cfg, duration_secs, stats_interval_secs).await
        }

        Command::Replay {
            path,
            config,
            markets,
            min_profit_usd,
            min_edge_bps,
            usdc_rate,
            coin_rate,
            latencies,
            no_box,
            no_parity,
            no_verticals,
            no_butterflies,
            output_jsonl,
        } => {
            init_logging(false);
            let mut cfg = ArbConfig::load(config.as_deref())?;
            if let Some(m) = markets {
                cfg.scanner.markets = m;
            }
            if let Some(p) = min_profit_usd {
                cfg.scanner.min_profit_usd = p;
            }
            if let Some(e) = min_edge_bps {
                cfg.scanner.min_edge_bps = e;
            }
            if let Some(r) = usdc_rate {
                cfg.rates.usdc_benchmark_rate = r;
            }
            if let Some(r) = coin_rate {
                cfg.rates.coin_benchmark_rate = r;
            }
            if let Some(l) = latencies {
                cfg.replay.latency_horizons_ms = l;
            }
            if no_box {
                cfg.scanner.enable_box_spreads = false;
            }
            if no_parity {
                cfg.scanner.enable_parity = false;
            }
            if no_verticals {
                cfg.scanner.enable_verticals = false;
            }
            if no_butterflies {
                cfg.scanner.enable_butterflies = false;
            }
            if let Some(out) = output_jsonl {
                cfg.replay.episodes_output = Some(out);
            }
            cfg.normalize();
            cfg.validate()?;

            let report = run_replay(&path, &cfg)?;
            print_replay_report(&report);
            Ok(())
        }

        Command::Rates {
            config,
            markets,
            warmup_secs,
        } => {
            init_logging(false);
            let mut cfg = ArbConfig::load(config.as_deref())?;
            if let Some(m) = markets {
                cfg.scanner.markets = m;
            }
            cfg.normalize();
            let _ = run_rates_snapshot(cfg, warmup_secs).await?;
            Ok(())
        }
    }
}

fn init_logging(json: bool) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,deribit_tick_collector=warn"));
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .init();
    }
}
