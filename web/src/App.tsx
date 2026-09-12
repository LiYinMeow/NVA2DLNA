import { useCallback, useEffect, useMemo, useState } from 'react';

type Device = {
  udn: string;
  friendly_name: string;
  location: string;
  model_name?: string | null;
  online: boolean;
};

type Status = {
  nva_online: boolean;
  target_udn: string | null;
  target_name: string | null;
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

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(true), 5000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  const selected = useMemo(
    () => devices.find((device) => device.udn === status.target_udn),
    [devices, status.target_udn],
  );

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
            <h1 id="device-title">选择 DLNA 设备</h1>
            <p className="description">NVA 收到的投屏会自动转发到选中的设备。</p>
          </div>
          <button className="scan-button" onClick={() => void scan()} disabled={scanning}>
            <ScanIcon active={scanning} />
            {scanning ? '扫描中' : '扫描设备'}
          </button>
        </div>

        {(selected || status.target_name) && <div className="selected-summary">
          <span className="summary-dot" />
          当前目标：<strong>{selected?.friendly_name ?? status.target_name}</strong>
        </div>}

        <div className="device-list" aria-live="polite" aria-busy={loading || scanning}>
          {loading
            ? <div className="empty-state"><span className="loader" />正在读取设备…</div>
            : devices.length === 0
              ? <div className="empty-state">
                  <span className="empty-screen"><ScreenIcon /></span>
                  <strong>未发现 DLNA 设备</strong>
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
                      <strong>{device.friendly_name}</strong>
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
    </main>

    {notice && <div className="toast" role="alert">{notice}</div>}
  </div>;
}
