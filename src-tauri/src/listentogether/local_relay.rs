//! The app owns its bundled relay, started only by an explicit hosting action.
//! A Windows job kills the child even when the app crashes or is force-closed.

use std::path::PathBuf;
#[cfg(windows)]
use std::sync::Mutex;

pub struct LocalRelay {
    executable: PathBuf,
    data_dir: PathBuf,
    #[cfg(windows)]
    child: Mutex<Option<OwnedRelay>>,
}

impl LocalRelay {
    #[cfg(windows)]
    pub fn ensure_any_port(&self) -> Result<u16, String> {
        if let Some(owned) = self.child.lock().unwrap().as_mut() {
            if matches!(owned.child.try_wait(), Ok(None)) {
                return Ok(owned.port);
            }
        }
        let probe = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let port = probe.local_addr().map_err(|e| e.to_string())?.port();
        drop(probe);
        self.ensure_running(port)?;
        Ok(port)
    }

    #[cfg(not(windows))]
    pub fn ensure_any_port(&self) -> Result<u16, String> {
        Err("The bundled local relay is available in the Windows package.".into())
    }
    pub fn bundled() -> Self {
        let directory = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|p| p.to_path_buf()))
            .unwrap_or_default();
        let data_dir = std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join("LimusicRelay");
        Self::new(directory.join("limusic-sync.exe"), data_dir)
    }

    pub(super) fn new(executable: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            executable,
            data_dir,
            #[cfg(windows)]
            child: Mutex::new(None),
        }
    }

    pub fn available(&self) -> bool {
        cfg!(windows) && self.executable.is_file()
    }

    pub fn running(&self) -> bool {
        #[cfg(windows)]
        {
            let mut child = self.child.lock().unwrap();
            child.as_mut().is_some_and(|owned| matches!(owned.child.try_wait(), Ok(None)))
        }
        #[cfg(not(windows))]
        false
    }

    pub fn stop(&self) {
        #[cfg(windows)]
        {
            self.child.lock().unwrap().take();
        }
    }

    #[cfg(not(windows))]
    pub fn ensure_running(&self, _port: u16) -> Result<(), String> {
        Err("Starting the bundled relay is supported by the Windows package.".into())
    }

    #[cfg(windows)]
    pub fn ensure_running(&self, port: u16) -> Result<(), String> {
        use std::fs::{self, File};
        use std::net::{Ipv4Addr, SocketAddr, TcpStream};
        use std::os::windows::process::CommandExt;
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        let mut slot = self.child.lock().unwrap();
        if let Some(owned) = slot.as_mut() {
            if matches!(owned.child.try_wait(), Ok(None)) {
                return if owned.port == port {
                    Ok(())
                } else {
                    Err(format!("Your local relay is already running on port {}.", owned.port))
                };
            }
        }
        slot.take();
        if !self.available() {
            return Err("The bundled relay is missing. Reinstall Limusic Plus.".into());
        }
        let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            return Err(format!("Port {port} is already in use. Disable 'Run relay on this PC' to connect to an existing relay."));
        }
        fs::create_dir_all(&self.data_dir).map_err(|e| e.to_string())?;
        let stdout =
            File::create(self.data_dir.join("relay.stdout.log")).map_err(|e| e.to_string())?;
        let stderr =
            File::create(self.data_dir.join("relay.stderr.log")).map_err(|e| e.to_string())?;
        let job = Job::new()?;
        let child = Command::new(&self.executable)
            .current_dir(&self.data_dir)
            .env("PORT", port.to_string())
            .env("BIND_ADDRESS", "127.0.0.1")
            .env("LIMUSIC_ROOMS_DB", self.data_dir.join("saved-rooms.sqlite3"))
            .creation_flags(0x08000000) // CREATE_NO_WINDOW
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .map_err(|e| format!("Could not start the local relay: {e}"))?;
        // OwnedRelay also kills/reaps the child if assigning its job fails.
        let mut owned = OwnedRelay { child, port, _job: job };
        owned._job.attach(&owned.child)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if owned.child.try_wait().map_err(|e| e.to_string())?.is_some() {
                return Err("The local relay exited. See relay.stderr.log in your LimusicRelay data folder.".into());
            }
            if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
                break;
            }
            if Instant::now() >= deadline {
                return Err("The local relay did not become ready within ten seconds.".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        tracing::info!(pid = owned.child.id(), port, "started local listening relay");
        *slot = Some(owned);
        Ok(())
    }
}

#[cfg(windows)]
struct OwnedRelay {
    child: std::process::Child,
    port: u16,
    _job: Job,
}

#[cfg(windows)]
impl Drop for OwnedRelay {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(windows)]
struct Job(windows::Win32::Foundation::HANDLE);

// HANDLE is process-wide and is only closed when this owner is dropped.
#[cfg(windows)]
unsafe impl Send for Job {}

#[cfg(windows)]
impl Job {
    fn new() -> Result<Self, String> {
        use windows::Win32::System::JobObjects::*;
        let job = Self(
            unsafe { CreateJobObjectW(None, windows::core::PCWSTR::null()) }
                .map_err(|e| e.to_string())?,
        );
        let mut info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        unsafe {
            SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                std::mem::size_of_val(&info) as u32,
            )
        }
        .map_err(|e| e.to_string())?;
        Ok(job)
    }

    fn attach(&self, child: &std::process::Child) -> Result<(), String> {
        use std::os::windows::io::AsRawHandle;
        unsafe {
            windows::Win32::System::JobObjects::AssignProcessToJobObject(
                self.0,
                windows::Win32::Foundation::HANDLE(child.as_raw_handle()),
            )
        }
        .map_err(|e| e.to_string())
    }
}

#[cfg(windows)]
impl Drop for Job {
    fn drop(&mut self) {
        let _ = unsafe { windows::Win32::Foundation::CloseHandle(self.0) };
    }
}

/// The loopback form identifies saved rooms created before on-demand hosting.
pub fn loopback_port(url: &str) -> Option<u16> {
    let url = tauri::Url::parse(url).ok()?;
    if url.scheme() != "ws" {
        return None;
    }
    let local = url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    local.then(|| url.port_or_known_default()).flatten()
}

pub fn hosting_port(url: &str) -> u16 {
    tauri::Url::parse(url)
        .ok()
        .filter(|url| url.scheme() == "ws")
        .and_then(|url| url.port_or_known_default())
        .unwrap_or(8080)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_and_advertised_addresses_choose_the_right_listener() {
        assert_eq!(loopback_port("ws://localhost:8091/ws"), Some(8091));
        assert_eq!(loopback_port("ws://127.0.0.1:8080/ws"), Some(8080));
        assert_eq!(loopback_port("ws://[::1]:8080/ws"), Some(8080));
        assert_eq!(loopback_port("ws://192.168.1.20:8080/ws"), None);
        assert_eq!(loopback_port("wss://remote.example/ws"), None);
        assert_eq!(hosting_port("ws://192.168.1.20:8091/ws"), 8091);
        assert_eq!(hosting_port("wss://my-tunnel.example/ws"), 8080);
    }

    #[cfg(windows)]
    fn relay() -> LocalRelay {
        let executable =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/limusic-sync.exe");
        assert!(executable.is_file(), "run cargo build -p sync-server first");
        LocalRelay::new(
            executable,
            std::env::temp_dir().join(format!("limusic-lazy-relay-{}", rand::random::<u64>())),
        )
    }

    #[cfg(windows)]
    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    #[cfg(windows)]
    #[tokio::test]
    #[ignore = "requires cargo build -p sync-server; run scripts/tests/local-relay.check.ps1"]
    async fn lazy_start_reuses_child_and_preserves_saved_identity_after_stop() {
        use futures_util::{SinkExt, StreamExt};
        use listen_protocol::{ClientMessage, ServerMessage};
        use tokio_tungstenite::tungstenite::Message;
        let manager = relay();
        let port = free_port();
        assert!(!manager.running());
        assert!(
            !manager.data_dir.exists(),
            "normal/guest startup must not even create a relay database"
        );
        manager.ensure_running(port).unwrap();
        let pid = manager.child.lock().unwrap().as_ref().unwrap().child.id();
        manager.ensure_running(port).unwrap();
        assert_eq!(
            manager.child.lock().unwrap().as_ref().unwrap().child.id(),
            pid,
            "hosting twice reuses one relay"
        );

        let url = format!("ws://127.0.0.1:{port}/ws");
        let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        socket
            .send(Message::Text(ClientMessage::CreateRoom { username: "host".into() }.to_json()))
            .await
            .unwrap();
        let _created = socket.next().await.unwrap().unwrap();
        socket.send(Message::Text(ClientMessage::SaveRoom.to_json())).await.unwrap();
        let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
            panic!("expected reservation")
        };
        let ServerMessage::RoomSaved { room_code, owner_token } =
            serde_json::from_str(&text).unwrap()
        else {
            panic!("expected saved room")
        };
        socket.close(None).await.unwrap();
        manager.stop();
        assert!(!manager.running());
        assert!(manager.data_dir.join("saved-rooms.sqlite3").is_file());
        manager.ensure_running(port).unwrap();
        let (mut socket, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        socket
            .send(Message::Text(
                ClientMessage::OpenSavedRoom {
                    room_code: room_code.clone(),
                    owner_token,
                    username: "host".into(),
                }
                .to_json(),
            ))
            .await
            .unwrap();
        let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
            panic!("expected reopened room")
        };
        assert!(
            matches!(serde_json::from_str::<ServerMessage>(&text).unwrap(), ServerMessage::RoomCreated { room_code: code, state, .. } if code == room_code && state.saved)
        );
        socket.close(None).await.unwrap();
        manager.stop();
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "requires cargo build -p sync-server; run scripts/tests/local-relay.check.ps1"]
    fn occupied_ports_are_refused_without_killing_or_adopting_the_listener() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let manager = relay();
        assert!(manager.ensure_running(port).unwrap_err().contains("already in use"));
        manager.stop();
        assert!(!manager.running());
        assert!(std::net::TcpStream::connect(("127.0.0.1", port)).is_ok());
        assert!(!manager.data_dir.exists());
    }

    #[cfg(windows)]
    #[test]
    #[ignore = "requires cargo build -p sync-server; run scripts/tests/local-relay.check.ps1"]
    fn closing_the_job_handle_kills_the_relay_without_rust_child_cleanup() {
        use std::os::windows::process::CommandExt;
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};
        let manager = relay();
        std::fs::create_dir_all(&manager.data_dir).unwrap();
        let job = Job::new().unwrap();
        let mut child = Command::new(&manager.executable)
            .env("PORT", free_port().to_string())
            .env("LIMUSIC_ROOMS_DB", manager.data_dir.join("saved-rooms.sqlite3"))
            .creation_flags(0x08000000)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        job.attach(&child).unwrap();
        assert!(child.try_wait().unwrap().is_none());
        drop(job);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("job left an orphan relay")
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
