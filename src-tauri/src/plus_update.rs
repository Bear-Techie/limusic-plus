use sha2::{Digest, Sha256};
use std::{
    fs,
    os::windows::process::CommandExt,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};
use tauri::{AppHandle, Manager};
use tauri_plugin_updater::UpdaterExt;

const PUBLIC_KEY: &str = include_str!("../../packaging/windows/update.pub");
const CREATE_NO_WINDOW: u32 = 0x08000000;

struct Staged {
    version: String,
    directory: tempfile::TempDir,
    sha256: String,
}

#[derive(Default)]
pub struct PendingUpdate {
    normal_quit: AtomicBool,
    download: tokio::sync::Mutex<()>,
    staged: Mutex<Option<Staged>>,
}

fn installation_dir() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    if !exe.file_name()?.to_str()?.eq_ignore_ascii_case("Limusic Plus.exe") {
        return None;
    }
    let root = exe.parent()?;
    (root.join("LimusicPlus.exe").is_file() && root.join("unins000.exe").is_file())
        .then(|| root.to_owned())
}

pub fn can_install() -> bool {
    cfg!(target_arch = "x86_64") && installation_dir().is_some()
}

pub async fn stage(app: &AppHandle, repo: &str, version: &str) -> anyhow::Result<()> {
    let root =
        installation_dir().ok_or_else(|| anyhow::anyhow!("The Windows installer is required"))?;
    let pending = app.state::<PendingUpdate>();
    let _download = pending.download.lock().await;
    if pending.staged.lock().unwrap().as_ref().is_some_and(|staged| staged.version == version) {
        return Ok(());
    }
    let base = format!("https://github.com/{repo}/releases/download/v{version}");
    let filename = format!("Limusic-Plus-{version}-setup-x64.exe");
    let manifest = format!("{base}/{filename}.update.json").parse()?;
    let mut update = app
        .updater_builder()
        .endpoints(vec![manifest])?
        .pubkey(PUBLIC_KEY.trim())
        .timeout(Duration::from_secs(30))
        .build()?
        .check()
        .await?
        .ok_or_else(|| anyhow::anyhow!("The signed update is unavailable"))?;
    anyhow::ensure!(
        update.version == version && update.download_url.as_str() == format!("{base}/{filename}"),
        "The signed update does not match the selected release"
    );
    update.timeout = Some(Duration::from_secs(300));
    // Tauri verifies the downloaded bytes with the embedded key. Inno handles the installation.
    let bytes = update.download(|_, _| {}, || {}).await?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let directory = tempfile::Builder::new().prefix("limusic-plus-update-").tempdir()?;
    fs::write(directory.path().join("setup.exe"), bytes)?;
    fs::copy(root.join("LimusicPlus.exe"), directory.path().join("LimusicPlus.exe"))?;
    *pending.staged.lock().unwrap() =
        Some(Staged { version: version.to_owned(), directory, sha256 });
    Ok(())
}

pub fn record_exit(app: &AppHandle, code: Option<i32>) {
    app.state::<PendingUpdate>().normal_quit.store(code == Some(0), Ordering::Relaxed);
}

pub fn install_on_quit(app: &AppHandle) {
    let pending = app.state::<PendingUpdate>();
    if !pending.normal_quit.load(Ordering::Relaxed) {
        return;
    }
    let Some(root) = installation_dir() else { return };
    let Some(staged) = pending.staged.lock().unwrap().take() else { return };
    let result = Command::new(staged.directory.path().join("LimusicPlus.exe"))
        .arg("--apply-update")
        .arg(std::process::id().to_string())
        .arg(root)
        .arg(staged.sha256)
        .arg(staged.version)
        .creation_flags(CREATE_NO_WINDOW)
        .spawn();
    match result {
        Ok(_) => {
            let _ = staged.directory.keep();
        }
        Err(error) => tracing::warn!("could not launch the Plus updater: {error}"),
    }
}
