use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    process::ExitCode,
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use nva2dlna::{
    config::{Cli, PersistedConfig, RuntimeConfig},
    dlna, nva, ssdp,
    state::AppState,
    web,
};
use tokio::{process::Command, task::JoinSet};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("nva2dlna=info,tower_http=info")),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error!(error = %format!("{error:#}"), "NVA2DLNA stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let persisted = PersistedConfig::load_or_create(&cli.config).await?;
    let advertise_ip = match cli.advertise_ip {
        Some(ip) => ip,
        None => detect_lan_ipv4()?,
    };
    let config = RuntimeConfig {
        web_listen: cli.web_listen,
        nva_listen: cli.nva_listen,
        advertise_ip,
        config_path: cli.config,
        web_dir: cli.web_dir,
        ffmpeg: cli.ffmpeg,
        friendly_name: cli.friendly_name,
        device_uuid: persisted.device_uuid,
        selected_udn: persisted.selected_udn,
    };
    let state = AppState::new(&config)?;
    check_ffmpeg(&config).await;
    info!(
        management = %format!("http://{advertise_ip}:{}", config.web_listen.port()),
        nva = %format!("http://{advertise_ip}:{}", config.nva_listen.port()),
        arch = std::env::consts::ARCH,
        "NVA2DLNA starting"
    );

    let mut tasks = JoinSet::new();
    tasks.spawn(web::run(
        state.clone(),
        config.web_listen,
        config.web_dir.clone(),
    ));
    tasks.spawn(nva::run(state.clone(), config.nva_listen));
    tasks.spawn(ssdp::run(state.clone(), config.nva_listen.port()));
    tasks.spawn(async move {
        dlna::periodic_scan(state).await;
        Ok(())
    });

    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal.context("cannot listen for Ctrl+C")?;
            info!("shutdown requested");
        }
        result = tasks.join_next() => {
            match result {
                Some(Ok(Ok(()))) => bail!("a required service stopped unexpectedly"),
                Some(Ok(Err(error))) => return Err(error),
                Some(Err(error)) => return Err(error.into()),
                None => bail!("all services stopped unexpectedly"),
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

async fn check_ffmpeg(config: &RuntimeConfig) {
    let result = Command::new(&config.ffmpeg)
        .arg("-version")
        .kill_on_drop(true)
        .output()
        .await;
    match result {
        Ok(output) if output.status.success() => {
            info!(path = %config.ffmpeg.display(), "FFmpeg DASH merger available");
        }
        Ok(output) => warn!(
            path = %config.ffmpeg.display(),
            status = %output.status,
            "FFmpeg is unavailable; direct media can still be proxied, but DASH/FLV/HLS will fail"
        ),
        Err(error) => warn!(
            path = %config.ffmpeg.display(),
            %error,
            "FFmpeg is unavailable; direct media can still be proxied, but DASH/FLV/HLS will fail"
        ),
    }
}

fn detect_lan_ipv4() -> Result<Ipv4Addr> {
    if let Ok(socket) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))
        && socket.connect((Ipv4Addr::new(192, 0, 2, 1), 9)).is_ok()
        && let Ok(SocketAddr::V4(address)) = socket.local_addr()
        && usable_ipv4(*address.ip())
    {
        return Ok(*address.ip());
    }
    for interface in if_addrs::get_if_addrs().context("cannot enumerate network interfaces")? {
        if let if_addrs::IfAddr::V4(address) = interface.addr
            && usable_ipv4(address.ip)
        {
            return Ok(address.ip);
        }
    }
    bail!("no usable LAN IPv4 address was found; pass --advertise-ip explicitly")
}

fn usable_ipv4(ip: Ipv4Addr) -> bool {
    !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_multicast()
        && !ip.is_broadcast()
        && !ip.is_link_local()
}
