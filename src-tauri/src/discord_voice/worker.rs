use std::io::{BufRead, BufReader};
use std::os::windows::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const BRIDGE: &str = include_str!("../../../discord-voice/bridge.mjs");
const CATALOG: &str = include_str!("../../../discord-voice/catalog.mjs");

pub(super) struct Runtime {
    folder: PathBuf,
    node: PathBuf,
}

impl Runtime {
    pub fn find() -> Option<Self> {
        let exe = std::env::current_exe().ok()?;
        let folder = exe.parent()?.join("discord-voice");
        let node = folder.join("node.exe");
        if node.is_file() && has_dependencies(&folder) {
            return Some(Self { folder, node });
        }

        #[cfg(debug_assertions)]
        {
            let folder = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../discord-voice");
            if has_dependencies(&folder) {
                return Some(Self { folder, node: "node.exe".into() });
            }
        }
        None
    }

    fn command(&self, script: &str, token: String) -> Command {
        let mut command = Command::new(&self.node);
        command
            .creation_flags(0x08000000) // CREATE_NO_WINDOW
            .current_dir(&self.folder)
            .args(["--no-global-search-paths", "--input-type=module", "-e", script])
            .env_remove("NODE_OPTIONS")
            .env_remove("NODE_PATH")
            .env("LIMUSIC_DISCORD_BOT_TOKEN", token);
        command
    }

    pub fn discover(&self, token: String) -> Result<serde_json::Value, &'static str> {
        let output = self.command(CATALOG, token).output().map_err(|_| "node")?;
        if !output.status.success() {
            return Err("connection");
        }
        let result: serde_json::Value =
            serde_json::from_slice(&output.stdout).map_err(|_| "connection")?;
        match result.get("error").and_then(|error| error.as_str()) {
            Some("token") => Err("token"),
            Some("dependencies") => Err("dependencies"),
            Some(_) => Err("connection"),
            None => Ok(result),
        }
    }
}

fn has_dependencies(folder: &std::path::Path) -> bool {
    folder.join("node_modules/@discordjs/voice/package.json").is_file()
}

pub(super) struct Session {
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<&'static str>>,
    worker: JoinHandle<()>,
}

impl Session {
    pub fn start(
        runtime: Runtime,
        token: String,
        channel: String,
        source: player::BroadcastSource,
    ) -> Result<Self, &'static str> {
        let token = token.trim().to_owned();
        if token.is_empty() || token.contains('\0') {
            return Err("token");
        }
        let channel = channel.trim().parse::<u64>().ok().filter(|id| *id != 0).ok_or("channel")?;
        let stop = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new("connecting"));
        let worker_stop = stop.clone();
        let worker_status = status.clone();
        let worker = thread::Builder::new()
            .name("discord-voice".into())
            .spawn(move || {
                let result = run(runtime, token, channel, source, &worker_stop, &worker_status);
                *worker_status.lock().unwrap() = result;
            })
            .map_err(|_| "failed")?;
        Ok(Self { stop, status, worker })
    }

    pub fn running(&self) -> bool {
        !self.worker.is_finished()
    }

    pub fn status(&self) -> &'static str {
        if self.running() && self.stop.load(Ordering::Relaxed) {
            "stopping"
        } else {
            *self.status.lock().unwrap()
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn run(
    runtime: Runtime,
    token: String,
    channel: u64,
    source: player::BroadcastSource,
    stop: &AtomicBool,
    status: &Mutex<&'static str>,
) -> &'static str {
    let Ok(mut child) = runtime
        .command(BRIDGE, token)
        .env("LIMUSIC_DISCORD_CHANNEL", channel.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return "node";
    };
    let mut input = child.stdin.take();
    let output = child.stdout.take().expect("piped stdout");
    let (send, receive) = mpsc::channel();
    let capture_messages = send.clone();
    let reader = thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            let message = match line.as_str() {
                "ready" => "ready",
                "reconnecting" => "reconnecting",
                "dependencies" => "dependencies",
                "token" => "token",
                "channel" => "channel",
                "occupied" => "occupied",
                "connection" => "connection",
                "audio" => "audio",
                _ => continue,
            };
            if send.send(message).is_err() {
                break;
            }
        }
        let _ = send.send("failed");
    });

    let capture_stop = Arc::new(AtomicBool::new(false));
    let mut capture = None;
    let mut capture_ready = false;
    let mut voice_ready = false;
    let deadline = Instant::now() + Duration::from_secs(45);
    let result = loop {
        if stop.load(Ordering::Relaxed) {
            break "idle";
        }
        match receive.recv_timeout(Duration::from_millis(100)) {
            Ok("ready") => {
                voice_ready = true;
                if let Some(input) = input.take() {
                    let capture_stop = capture_stop.clone();
                    let messages = capture_messages.clone();
                    let source = source.clone();
                    capture = Some(thread::spawn(move || {
                        if let Err(error) =
                            super::pcm_capture::capture(source, input, &capture_stop, || {
                                let _ = messages.send("capturing");
                            })
                        {
                            if !capture_stop.load(Ordering::Relaxed) {
                                tracing::warn!(%error, "Discord broadcast capture stopped");
                                let _ = messages.send("capture");
                            }
                        }
                    }));
                } else if capture_ready {
                    *status.lock().unwrap() = "live";
                }
            }
            Ok("capturing") => {
                capture_ready = true;
                if voice_ready {
                    *status.lock().unwrap() = "live";
                }
            }
            Ok("reconnecting") => {
                voice_ready = false;
                *status.lock().unwrap() = "reconnecting";
            }
            Ok(error) => break error,
            Err(_) if capture.is_none() && Instant::now() >= deadline => break "connection",
            Err(_) => {}
        }
    };
    capture_stop.store(true, Ordering::Relaxed);
    drop(input);
    // Closing stdin asks the bridge to leave voice before disconnecting its gateway.
    let shutdown = Instant::now() + Duration::from_secs(2);
    while matches!(child.try_wait(), Ok(None)) && Instant::now() < shutdown {
        thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader.join();
    if let Some(capture) = capture {
        let _ = capture.join();
    }
    result
}
