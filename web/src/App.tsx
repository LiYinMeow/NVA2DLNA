import { useCallback, useEffect, useMemo, useState } from 'react';

type Device = {
  udn: string;
  friendly_name: string;
  location: string;
  model_name?: string | null;
  mode: 'lelink' | 'dlna';
  protocols?: Array<'lelink' | 'dlna'>;
  online: boolean;
};

type Status = {
  nva_online: boolean;
  target_udn: string | null;
  target_name: string | null;
};

type NetworkInterface = {
  id: string;
  name: string;
  ipv4: string;
  prefix_length: number;
  selected: boolean;
  up?: boolean;
};

type NetworkInterfaces = {
  interfaces: NetworkInterface[];
  selected_interface_ids: string[];
  receive_interface_id?: string | null;
};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch('/api/v1' + path, {
    ...init,
    headers: {
      Accept: 'application/json',
      'X-NVA2DLNA-Request': '1',
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    let message = '请求失败（HTTP ' + response.status + '）';
    try {
      const body = await response.json() as {
        error?: string | { message?: string };
        message?: string;
      };
      message = body.message
        ?? (typeof body.error === 'string' ? body.error : body.error?.message)
        ?? message;
    } catch {
      // An empty error response is valid.
    }
    throw new Error(message);
  }
  return response.status === 204 ? undefined as T : await response.json() as T;
}

const api = {
  status: () => request<Status>('/status'),
  devices: () => request<Device[]>('/devices'),
  scan: () => request<void>('/discovery/scan', { method: 'POST' }),
  select: (udn: string) =>
    request<void>('/target', { method: 'PUT', body: JSON.stringify({ udn }) }),
  networkInterfaces: () => request<NetworkInterfaces>('/network/interfaces'),
  saveNetworkInterfaces: (selectedInterfaceIds: string[]) =>
    request<void>('/network/interfaces', {
      method: 'PUT',
      body: JSON.stringify({ selected_interface_ids: selectedInterfaceIds }),
    }),
};

function CastIcon() {
  return <svg aria-hidden="true" viewBox="0 0 32 32" className="brand-icon">
    <path d="M6 23.5a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5Z" fill="currentColor" />
    <path d="M4 16.5c6.35 0 11.5 5.15 11.5 11.5M4 9c10.5 0 19 8.5 19 19" fill="none" stroke="currentColor" strokeLinecap="round" strokeWidth="3" />
    <path d="M12.5 4H26a2 2 0 0 1 2 2v13.5" fill="none" stroke="currentColor" strokeLinecap="round" strokeWidth="3" />
  </svg>;
}

function ScreenIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24" className="screen-icon">
    <rect x="3" y="4.5" width="18" height="13" rx="2.25" fill="none" stroke="currentColor" strokeWidth="1.6" />
    <path d="M8.5 21h7M12 17.5V21" fill="none" stroke="currentColor" strokeLinecap="round" strokeWidth="1.6" />
  </svg>;
}

function ScanIcon({ active }: { active: boolean }) {
  return <svg aria-hidden="true" viewBox="0 0 24 24" className={active ? 'scan-icon spinning' : 'scan-icon'}>
    <path d="M20 11a8 8 0 1 0-2.34 5.66" fill="none" stroke="currentColor" strokeLinecap="round" strokeWidth="1.8" />
    <path d="M20 5v6h-6" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
  </svg>;
}

function NetworkIcon() {
  return <svg aria-hidden="true" viewBox="0 0 24 24" className="network-icon">
    <rect x="2.75" y="3.5" width="7" height="5" rx="1.25" fill="none" stroke="currentColor" strokeWidth="1.5" />
    <rect x="14.25" y="15.5" width="7" height="5" rx="1.25" fill="none" stroke="currentColor" strokeWidth="1.5" />
    <rect x="2.75" y="15.5" width="7" height="5" rx="1.25" fill="none" stroke="currentColor" strokeWidth="1.5" />
    <path d="M6.25 8.5v3.25h11.5v3.75M6.25 11.75v3.75" fill="none" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.5" />
  </svg>;
}

function ModeBadge({ mode }: { mode: Device['mode'] }) {
  return <span className={'mode-badge ' + mode}>
    {mode === 'lelink' ? 'LeLink' : 'DLNA'}
  </span>;
}

export default function App() {
  const [status, setStatus] = useState<Status>({
    nva_online: false,
    target_udn: null,
    target_name: null,
  });
  const [devices, setDevices] = useState<Device[]>([]);
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [saving, setSaving] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [network, setNetwork] = useState<NetworkInterfaces>({
    interfaces: [],
    selected_interface_ids: [],
    receive_interface_id: null,
  });
  const [draftInterfaceIds, setDraftInterfaceIds] = useState<string[]>([]);
  const [networkLoading, setNetworkLoading] = useState(true);
  const [networkSaving, setNetworkSaving] = useState(false);

  const refresh = useCallback(async (quiet = false) => {
    try {
      const [nextStatus, nextDevices] = await Promise.all([api.status(), api.devices()]);
      setStatus(nextStatus);
      setDevices(nextDevices);
      if (!quiet) setNotice(null);
    } catch (error) {
      if (!quiet) {
        setNotice(error instanceof Error ? error.message : '无法连接到桥接服务');
      }
    } finally {
      setLoading(false);
    }
  }, []);

  const loadNetworkInterfaces = useCallback(async (quiet = false) => {
    try {
      const nextNetwork = await api.networkInterfaces();
      setNetwork(nextNetwork);
      setDraftInterfaceIds(nextNetwork.selected_interface_ids);
      if (!quiet) setNotice(null);
    } catch (error) {
      if (!quiet) {
        setNotice(error instanceof Error ? error.message : '无法读取网络接口');
      }
    } finally {
      setNetworkLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    void loadNetworkInterfaces();
    const timer = window.setInterval(() => void refresh(true), 5000);
    return () => window.clearInterval(timer);
  }, [loadNetworkInterfaces, refresh]);

  const selected = useMemo(
    () => devices.find((device) => device.udn === status.target_udn),
    [devices, status.target_udn],
  );

  const networkDirty = useMemo(() => {
    if (draftInterfaceIds.length !== network.selected_interface_ids.length) return true;
    const savedIds = new Set(network.selected_interface_ids);
    return draftInterfaceIds.some((id) => !savedIds.has(id));
  }, [draftInterfaceIds, network.selected_interface_ids]);

  async function scan() {
    setScanning(true);
    setNotice(null);
    try {
      await api.scan();
      await refresh();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : '扫描失败');
    } finally {
      setScanning(false);
    }
  }

  async function choose(device: Device) {
    setSaving(device.udn);
    setNotice(null);
    try {
      await api.select(device.udn);
      setStatus((current) => ({
        ...current,
        target_udn: device.udn,
        target_name: device.friendly_name,
      }));
    } catch (error) {
      setNotice(error instanceof Error ? error.message : '无法选择该设备');
    } finally {
      setSaving(null);
    }
  }

  function toggleInterface(id: string) {
    setDraftInterfaceIds((current) => (
      current.includes(id)
        ? current.filter((interfaceId) => interfaceId !== id)
        : [...current, id]
    ));
  }

  async function saveNetworkInterfaces() {
    if (draftInterfaceIds.length === 0) {
      setNotice('请至少选择一个用于发现目标设备的 IPv4 网卡');
      return;
    }
    setNetworkSaving(true);
    setScanning(true);
    setNotice(null);
    let settingsSaved = false;
    try {
      await api.saveNetworkInterfaces(draftInterfaceIds);
      settingsSaved = true;
      await api.scan();
      await Promise.all([loadNetworkInterfaces(true), refresh(true)]);
    } catch (error) {
      const message = error instanceof Error ? error.message : '请求失败';
      setNotice(settingsSaved
        ? '网络设置已保存，但重新扫描失败：' + message
        : '无法保存网络接口设置：' + message);
    } finally {
      setNetworkSaving(false);
      setScanning(false);
    }
  }

  return <div className="app-shell">
    <header className="topbar">
      <div className="brand"><CastIcon /><span>NVA2DLNA</span></div>
      <div className={status.nva_online ? 'receiver-state ready' : 'receiver-state'}>
        <span />{status.nva_online ? 'NVA 已就绪' : 'NVA 未就绪'}
      </div>
    </header>

    <main>
      <section className="device-card" aria-labelledby="device-title">
        <div className="card-heading">
          <div>
            <p className="eyebrow">播放目标</p>
            <h1 id="device-title">选择投屏设备</h1>
            <p className="description">扫描局域网投屏设备；同一台设备同时提供 LeLink 与 DLNA 时将合并显示，并优先使用 LeLink 控制。</p>
          </div>
          <button className="scan-button" onClick={() => void scan()} disabled={scanning}>
            <ScanIcon active={scanning} />
            {scanning ? '扫描中' : '扫描设备'}
          </button>
        </div>

        {(selected || status.target_name) && <div className="selected-summary">
          <span className="summary-dot" />
          当前目标：<strong>{selected?.friendly_name ?? status.target_name}</strong>
          {selected && <ModeBadge mode={selected.mode} />}
        </div>}

        <div className="device-list" aria-live="polite" aria-busy={loading || scanning}>
          {loading
            ? <div className="empty-state"><span className="loader" />正在读取设备…</div>
            : devices.length === 0
              ? <div className="empty-state">
                  <span className="empty-screen"><ScreenIcon /></span>
                  <strong>未发现投屏设备</strong>
                  <span>确认播放器与本机位于同一局域网，然后点击“扫描设备”。</span>
                </div>
              : devices.map((device) => {
                  const isSelected = device.udn === status.target_udn;
                  return <button
                    key={device.udn}
                    className={isSelected ? 'device-row selected' : 'device-row'}
                    disabled={!device.online || saving !== null}
                    onClick={() => void choose(device)}
                    aria-pressed={isSelected}
                  >
                    <span className="device-mark"><ScreenIcon /></span>
                    <span className="device-copy">
                      <span className="device-title-line">
                        <strong>{device.friendly_name}</strong>
                        <ModeBadge mode={device.mode} />
                      </span>
                      <span>{device.model_name || device.location}</span>
                    </span>
                    <span className={device.online ? 'online-state online' : 'online-state'}>
                      {device.online ? '在线' : '离线'}
                    </span>
                    <span className="choice" aria-hidden="true">{saving === device.udn ? '…' : isSelected ? '✓' : ''}</span>
                  </button>;
                })}
        </div>

        <p className="footnote">选择结果会立即保存，重启后仍然有效。</p>
      </section>

      <section className="device-card network-card" aria-labelledby="network-title">
        <div className="card-heading network-heading">
          <div>
            <p className="eyebrow">网络转发</p>
            <h2 id="network-title">mDNS / SSDP 扫描网卡</h2>
            <p className="description">选择用于发现 LeLink 与 DLNA 目标的 IPv4 网卡，可同时扫描多个目标网络。</p>
          </div>
          <button
            className="scan-button save-network-button"
            onClick={() => void saveNetworkInterfaces()}
            disabled={networkLoading || networkSaving || !networkDirty || draftInterfaceIds.length === 0}
          >
            {networkSaving ? <span className="button-loader" /> : null}
            {networkSaving ? '保存并扫描中' : networkDirty ? '保存并扫描' : '已保存'}
          </button>
        </div>

        <div className="gateway-note">
          <NetworkIcon />
          <div>
            <strong>应用层投屏网关</strong>
            <span>输入端仍由当前 advertise 地址接收；服务在所选目标网卡上扫描 mDNS/SSDP，并从目标所在网卡地址提供媒体。无需启用系统 IP 转发或 NAT，但目标设备必须能访问本机 Web/媒体端口。</span>
          </div>
        </div>

        <div className="interface-list" aria-live="polite" aria-busy={networkLoading}>
          {networkLoading
            ? <div className="interface-empty"><span className="loader" />正在读取 IPv4 网卡…</div>
            : network.interfaces.length === 0
              ? <div className="interface-empty">
                  <strong>没有可用的 IPv4 网卡</strong>
                  <span>连接网络后刷新页面再试。</span>
                </div>
              : network.interfaces.map((item) => {
                  const checked = draftInterfaceIds.includes(item.id);
                  const isReceive = item.id === network.receive_interface_id;
                  const unavailable = item.up === false;
                  return <label
                    key={item.id}
                    className={checked ? 'interface-row selected' : 'interface-row'}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={unavailable || networkSaving}
                      onChange={() => toggleInterface(item.id)}
                    />
                    <span className="interface-check" aria-hidden="true">{checked ? '✓' : ''}</span>
                    <span className="interface-copy">
                      <span className="interface-title-line">
                        <strong>{item.name}</strong>
                        {isReceive ? <span className="interface-badge receive">NVA 接收</span> : null}
                        {checked ? <span className="interface-badge discovery">发现出口</span> : null}
                      </span>
                      <span>{item.ipv4}/{item.prefix_length}</span>
                    </span>
                    <span className={unavailable ? 'interface-state' : 'interface-state online'}>
                      {unavailable ? '不可用' : '可用'}
                    </span>
                  </label>;
                })}
        </div>

        <p className="footnote">保存后会立即重新扫描；设置将持久化并在重启后恢复。</p>
      </section>
    </main>

    {notice && <div className="toast" role="alert">{notice}</div>}
  </div>;
}
