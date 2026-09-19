use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket},
    process::ExitCode,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use nva2dlna::{
    config::{Cli, PersistedConfig, RuntimeConfig, validate_receiver_listeners},
    dlna, lelink, nva, ssdp,
    state::AppState,
    web,
};
use tokio::{process::Command, sync::watch, task::JoinSet};
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
    let web_listen = cli.resolved_web_listen();
    let mut persisted = PersistedConfig::load_or_create(&cli.config).await?;
    let migrated_nva_device_uuid = persisted.migrate_nva_identity(&cli.config).await?;
    let nva_device_uuid = persisted.current_nva_device_uuid();
    let retired_nva_device_uuid = persisted.retired_nva_device_uuid;
    let advertise_ip = match cli.advertise_ip {
        Some(ip) => ip,
        None => detect_lan_ipv4()?,
    };
    validate_receiver_listeners(advertise_ip, cli.nva_listen, cli.lelink_listen)?;
    // The pre-UniLink receiver used NVA2DLNA_NAME/--friendly-name. Keep those
    // deployments working while giving the explicit UniNVA setting precedence.
    let nva_name = cli
        .nva_name
        .or_else(|| std::env::var("NVA2DLNA_NAME").ok())
        .unwrap_or_else(|| "UniNVA".to_owned());
    let config = RuntimeConfig {
        web_listen,
        nva_listen: cli.nva_listen,
        lelink_listen: cli.lelink_listen,
        advertise_ip,
        config_path: cli.config,
        web_dir: cli.web_dir,
        ffmpeg: cli.ffmpeg,
        nva_name,
        dlna_name: cli.dlna_name,
        lelink_name: cli.lelink_name,
        device_uuid: persisted.device_uuid,
        nva_device_uuid,
        retired_nva_device_uuid,
        selected_udn: persisted.selected_udn,
        scan_interface_ids: persisted.scan_interface_ids,
    };
    let state = AppState::new(&config)?;
    check_ffmpeg(&config).await;
    let management_ip = if config.web_listen.ip().is_unspecified() {
        advertise_ip
    } else {
        *config.web_listen.ip()
    };
    info!(
        management = %format!("http://{management_ip}:{}", config.web_listen.port()),
        nva = %format!("{} @ http://{advertise_ip}:{}", config.nva_name, config.nva_listen.port()),
        dlna_output = %config.selected_udn.as_deref().unwrap_or("<not selected>"),
        lelink_preferred = %format!("{} @ {}:{}", config.lelink_name, advertise_ip, config.lelink_listen.port()),
        arch = std::env::consts::ARCH,
        "NVA2DLNA starting"
    );
    if let Some(retired) = migrated_nva_device_uuid {
        info!(%retired, %nva_device_uuid, "migrated NVA identity to clear a stale DLNA classification");
    }

    let mut tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tasks.spawn(web::run(
        state.clone(),
        config.web_listen,
        config.web_dir.clone(),
    ));
    tasks.spawn(nva::run(state.clone(), config.nva_listen));
    tasks.spawn(run_lelink_input(state.clone(), config.lelink_listen));
    let mut ssdp_task = tokio::spawn(ssdp::run(
        state.clone(),
        config.nva_listen.port(),
        shutdown_rx,
    ));
    tasks.spawn(async move {
        dlna::periodic_scan(state).await;
        Ok(())
    });

    let (outcome, ssdp_finished) = tokio::select! {
        signal = shutdown_signal() => {
            match signal {
                Ok(()) => {
                    info!("shutdown requested");
                    (Ok(()), false)
                }
                Err(error) => (Err(error), false),
            }
        }
        result = &mut ssdp_task => {
            let outcome = match result {
                Ok(Ok(())) => Err(anyhow!("NVA SSDP advertiser stopped unexpectedly")),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(error.into()),
            };
            (outcome, true)
        }
        result = tasks.join_next() => {
            let outcome = match result {
                Some(Ok(Ok(()))) => Err(anyhow!("a required service stopped unexpectedly")),
                Some(Ok(Err(error))) => Err(error),
                Some(Err(error)) => Err(error.into()),
                None => Err(anyhow!("all services stopped unexpectedly")),
            };
            (outcome, false)
        }
    };
    let _ = shutdown_tx.send(true);
    if !ssdp_finished {
        match tokio::time::timeout(Duration::from_secs(2), &mut ssdp_task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => warn!(%error, "NVA SSDP advertiser failed during shutdown"),
            Ok(Err(error)) => warn!(%error, "NVA SSDP advertiser task failed during shutdown"),
            Err(_) => {
                warn!("NVA SSDP byebye timed out during shutdown");
                ssdp_task.abort();
                let _ = ssdp_task.await;
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    outcome
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate()).context("cannot listen for SIGTERM")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("cannot listen for Ctrl+C"),
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_close, ctrl_logoff, ctrl_shutdown};

        let mut break_signal = ctrl_break().context("cannot listen for Ctrl+Break")?;
        let mut close_signal = ctrl_close().context("cannot listen for console close")?;
        let mut logoff_signal = ctrl_logoff().context("cannot listen for logoff")?;
        let mut shutdown_signal = ctrl_shutdown().context("cannot listen for system shutdown")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("cannot listen for Ctrl+C"),
            _ = break_signal.recv() => Ok(()),
            _ = close_signal.recv() => Ok(()),
            _ = logoff_signal.recv() => Ok(()),
            _ = shutdown_signal.recv() => Ok(()),
        }
    }

    #[cfg(not(any(unix, windows)))]
    tokio::signal::ctrl_c()
        .await
        .context("cannot listen for Ctrl+C")
}

const LELINK_RETRY_INITIAL: Duration = Duration::from_secs(5);
const LELINK_RETRY_MAX: Duration = Duration::from_secs(60);

/// LeLink is an independent input face. Bind control before starting discovery so a
/// sender is never given a dead port. On Windows the conventional port can already
/// be an outbound connection's ephemeral source port; `bind_control` then chooses a
/// free port and this supervisor passes that exact port to the browse record.
async fn run_lelink_input(state: AppState, preferred: SocketAddrV4) -> Result<()> {
    let mut retry_delay = LELINK_RETRY_INITIAL;
    loop {
        match lelink::bind_control(preferred).await {
            Ok((listener, preferred_error)) => {
                let listening = listener
                    .local_addr()
                    .context("LeLink control socket has no address after bind")?;
                if let Some(error) = preferred_error {
                    warn!(
                        %preferred,
                        %listening,
                        error = %format!("{error:#}"),
                        "LeLink preferred port is busy; using the advertised dynamic port"
                    );
                } else {
                    info!(%listening, "LeLink control port reserved for discovery");
                }
                retry_delay = LELINK_RETRY_INITIAL;

                let control = lelink::run_on(state.clone(), listener);
                let discovery = run_lelink_browse(state.clone(), listening.port());
                tokio::pin!(control);
                tokio::pin!(discovery);
                let (component, outcome) = tokio::select! {
                    outcome = &mut control => ("control", outcome),
                    outcome = &mut discovery => ("discovery", outcome),
                };
                match outcome {
                    Ok(()) => warn!(
                        %component,
                        %listening,
                        retry_seconds = retry_delay.as_secs(),
                        "LeLink input stopped; NVA remains online"
                    ),
                    Err(error) => warn!(
                        %component,
                        %listening,
                        retry_seconds = retry_delay.as_secs(),
                        error = %format!("{error:#}"),
                        "LeLink input unavailable; NVA remains online"
                    ),
                }
            }
            Err(error) => {
                warn!(
                    %preferred,
                    retry_seconds = retry_delay.as_secs(),
                    error = %format!("{error:#}"),
                    "LeLink control listener unavailable; discovery is disabled and NVA remains online"
                );
            }
        }
        tokio::time::sleep(retry_delay).await;
        retry_delay = (retry_delay * 2).min(LELINK_RETRY_MAX);
    }
}

async fn run_lelink_browse(state: AppState, control_port: u16) -> Result<()> {
    let mut retry_delay = LELINK_RETRY_INITIAL;
    loop {
        match lelink::browse(state.clone(), control_port).await {
            Ok(()) => warn!(
                control_port,
                retry_seconds = retry_delay.as_secs(),
                "LeLink discovery listener stopped; retrying"
            ),
            Err(error) => {
                warn!(
                    control_port,
                    retry_seconds = retry_delay.as_secs(),
                    error = %format!("{error:#}"),
                    "LeLink discovery listener unavailable; NVA remains online"
                )
            }
        }
        tokio::time::sleep(retry_delay).await;
        retry_delay = (retry_delay * 2).min(LELINK_RETRY_MAX);
    }
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
