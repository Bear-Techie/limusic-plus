use serde::{Deserialize, Serialize};
use windows::core::{w, PCWSTR, PWSTR};
use windows::Win32::Foundation::ERROR_NOT_FOUND;
use windows::Win32::Security::Credentials::*;

const TARGET: PCWSTR = w!("Limusic Plus/Discord Voice Bot");
const PREFIX: &str = "Limusic Plus/Discord Voice Bots/";
const FILTER: PCWSTR = w!("Limusic Plus/Discord Voice Bots/*");

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Bot {
    pub id: String,
    pub name: String,
    pub channel: String,
    #[serde(default)]
    pub channel_name: String,
    pub token: String,
}

impl Bot {
    pub(super) fn public(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id, "name": self.name, "channel": self.channel, "channel_name": self.channel_name
        })
    }
}

fn read(target: PCWSTR) -> Result<Option<String>, &'static str> {
    let mut credential = std::ptr::null_mut();
    match unsafe { CredReadW(target, CRED_TYPE_GENERIC, None, &mut credential) } {
        Ok(()) => {}
        Err(error) if error.code() == ERROR_NOT_FOUND.to_hresult() => return Ok(None),
        Err(_) => return Err("credentials"),
    }
    // CredReadW owns both the returned structure and the blob in the same allocation.
    let value = unsafe {
        let token = blob(&*credential).map(str::to_owned);
        CredFree(credential.cast());
        token
    };
    value.map(Some).ok_or("credentials")
}

fn blob(entry: &CREDENTIALW) -> Option<&str> {
    if entry.CredentialBlobSize == 0 || entry.CredentialBlob.is_null() {
        return None;
    }
    let bytes = unsafe {
        std::slice::from_raw_parts(entry.CredentialBlob, entry.CredentialBlobSize as usize)
    };
    std::str::from_utf8(bytes).ok()
}

fn target(id: &str) -> Result<Vec<u16>, &'static str> {
    if id.is_empty() || id.len() > 20 || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err("token");
    }
    Ok(PREFIX.encode_utf16().chain(id.encode_utf16()).chain([0]).collect())
}

pub(super) fn list() -> Result<Vec<Bot>, &'static str> {
    let mut count = 0;
    let mut entries = std::ptr::null_mut();
    let result = unsafe { CredEnumerateW(FILTER, None, &mut count, &mut entries) };
    let mut bots: Vec<Bot> = match result {
        Ok(()) => unsafe {
            let mut bots = Vec::new();
            for index in 0..count as usize {
                if let Some(bot) = blob(&**entries.add(index))
                    .and_then(|text| serde_json::from_str::<Bot>(text).ok())
                {
                    bots.push(bot);
                }
            }
            CredFree(entries.cast());
            bots
        },
        Err(error) if error.code() == ERROR_NOT_FOUND.to_hresult() => Vec::new(),
        Err(_) => return Err("credentials"),
    };
    if let Some(legacy) = load("legacy")? {
        if !bots.iter().any(|bot| bot.token == legacy.token) {
            bots.push(legacy);
        }
    }
    bots.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()).then(a.id.cmp(&b.id)));
    Ok(bots)
}

pub(super) fn load(id: &str) -> Result<Option<Bot>, &'static str> {
    if id == "legacy" {
        return read(TARGET).map(|value| {
            value.map(|token| Bot {
                id: "legacy".into(),
                name: "Previously saved bot".into(),
                channel: String::new(),
                channel_name: String::new(),
                token,
            })
        });
    }
    let target = target(id)?;
    read(PCWSTR(target.as_ptr()))?
        .map(|text| serde_json::from_str(&text).map_err(|_| "credentials"))
        .transpose()
}

pub(super) fn clean_token(token: &str) -> Result<&str, &'static str> {
    let token = token.trim();
    if token.is_empty()
        || token.len() > CRED_MAX_CREDENTIAL_BLOB_SIZE as usize
        || !token.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err("token");
    }
    Ok(token)
}

pub(super) fn save(bot: &Bot) -> Result<(), &'static str> {
    clean_token(&bot.token)?;
    let mut target = target(&bot.id)?;
    let mut payload = serde_json::to_vec(bot).map_err(|_| "credentials")?;
    if payload.len() > CRED_MAX_CREDENTIAL_BLOB_SIZE as usize {
        return Err("credentials");
    }
    let entry = CREDENTIALW {
        Type: CRED_TYPE_GENERIC,
        TargetName: PWSTR(target.as_mut_ptr()),
        CredentialBlobSize: payload.len() as u32,
        CredentialBlob: payload.as_mut_ptr(),
        Persist: CRED_PERSIST_LOCAL_MACHINE,
        ..Default::default()
    };
    unsafe { CredWriteW(&entry, 0) }.map_err(|_| "credentials")?;
    if load("legacy")?.is_some_and(|legacy| legacy.token == bot.token) {
        forget("legacy")?;
    }
    Ok(())
}

pub(super) fn forget(id: &str) -> Result<(), &'static str> {
    let wide;
    let target = if id == "legacy" {
        TARGET
    } else {
        wide = target(id)?;
        PCWSTR(wide.as_ptr())
    };
    match unsafe { CredDeleteW(target, CRED_TYPE_GENERIC, None) } {
        Ok(()) => Ok(()),
        Err(error) if error.code() == ERROR_NOT_FOUND.to_hresult() => Ok(()),
        Err(_) => Err("credentials"),
    }
}
