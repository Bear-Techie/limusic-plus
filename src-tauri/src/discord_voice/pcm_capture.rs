use player::{BroadcastSource, BroadcastTrack, PcmDecoder};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use windows::core::PCWSTR;
use windows::Win32::Foundation::{
    ERROR_NO_DATA, ERROR_PIPE_CONNECTED, ERROR_PIPE_LISTENING, HANDLE,
};
use windows::Win32::Storage::FileSystem::{FILE_FLAG_FIRST_PIPE_INSTANCE, PIPE_ACCESS_INBOUND};
use windows::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, PIPE_NOWAIT, PIPE_REJECT_REMOTE_CLIENTS,
};

const FRAME_TIME: Duration = Duration::from_millis(20);
const FRAME_BYTES: usize = 48_000 * 4 / 50; // 48 kHz, signed 16-bit stereo.
const SILENCE: [u8; FRAME_BYTES] = [0; FRAME_BYTES];
const DISCORD_GAIN: f64 = 0.70;

struct Pipe {
    file: File,
    connected: bool,
    ended: bool,
}

impl Pipe {
    fn new() -> io::Result<(Self, String)> {
        let name = format!(r"\\.\pipe\limusic-discord-{:032x}", rand::random::<u128>());
        let wide: Vec<u16> = name.encode_utf16().chain([0]).collect();
        let handle = unsafe {
            CreateNamedPipeW(
                PCWSTR(wide.as_ptr()),
                PIPE_ACCESS_INBOUND | FILE_FLAG_FIRST_PIPE_INSTANCE,
                PIPE_NOWAIT | PIPE_REJECT_REMOTE_CLIENTS,
                1,
                0,
                (FRAME_BYTES * 3) as u32,
                0,
                None,
            )
        };
        if handle.is_invalid() {
            return Err(io::Error::last_os_error());
        }
        let file = unsafe { File::from_raw_handle(handle.0) };
        Ok((Self { file, connected: false, ended: false }, name))
    }

    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if !self.connected {
            let handle = HANDLE(self.file.as_raw_handle());
            match unsafe { ConnectNamedPipe(handle, None) } {
                Ok(()) => {}
                Err(error) if error.code() == ERROR_PIPE_CONNECTED.to_hresult() => {}
                Err(error) if error.code() == ERROR_PIPE_LISTENING.to_hresult() => return Ok(0),
                Err(error) => return Err(error.into()),
            }
            self.connected = true;
        }
        match self.file.read(bytes) {
            Err(error) if error.raw_os_error() == Some(ERROR_NO_DATA.0 as i32) => Ok(0),
            Ok(0) => {
                self.ended = true;
                Ok(0)
            }
            result => result,
        }
    }
}

struct Track {
    // Close the reader first so a pending decoder write can finish before mpv is destroyed.
    pipe: Pipe,
    _decoder: PcmDecoder,
    source: BroadcastTrack,
    opened: Instant,
    position: f64,
    last_position: f64,
    frame: [u8; FRAME_BYTES],
    filled: usize,
}

impl Track {
    fn new(source: BroadcastTrack) -> anyhow::Result<Self> {
        let (pipe, name) = Pipe::new()?;
        let decoder = source.decode_pcm(&name).map_err(|error| anyhow::anyhow!("{error}"))?;
        let position = source.position;
        Ok(Self {
            pipe,
            _decoder: decoder,
            source,
            opened: Instant::now(),
            position,
            last_position: position,
            frame: SILENCE,
            filled: 0,
        })
    }

    fn follows(&mut self, source: &BroadcastTrack) -> bool {
        let follows = self.source.same_source(source)
            && !self.pipe.ended
            && (source.position - self.last_position).abs() < 0.35
            && self.position - source.position < 0.35
            && (self.pipe.connected || self.opened.elapsed() < Duration::from_secs(5));
        self.last_position = source.position;
        self.source.gain = source.gain;
        follows
    }

    fn next_frame(&mut self) -> io::Result<bool> {
        while self.filled < FRAME_BYTES {
            let len = self.pipe.read(&mut self.frame[self.filled..])?;
            if len == 0 {
                return Ok(false);
            }
            self.filled += len;
        }
        self.filled = 0;
        self.position += FRAME_TIME.as_secs_f64() * self.source.speed;
        Ok(true)
    }
}

#[derive(Default)]
struct Deck {
    track: Option<Track>,
    retry_after: Option<Instant>,
}

impl Deck {
    fn frame(&mut self, current: Option<BroadcastTrack>) -> anyhow::Result<Option<(&[u8], f64)>> {
        let Some(current) = current else {
            self.track = None;
            self.retry_after = None;
            return Ok(None);
        };
        if self.track.as_mut().is_some_and(|t| !t.follows(&current)) {
            self.track = None;
        }
        if self.track.is_none() && self.retry_after.is_none_or(|when| Instant::now() >= when) {
            self.track = Some(Track::new(current)?);
            self.retry_after = Some(Instant::now() + Duration::from_millis(300));
        }
        let Some(track) = self.track.as_mut() else { return Ok(None) };
        // Drain stale frames in bounded batches if the independent decoder took time to open.
        for _ in 0..20 {
            if !track.next_frame()? {
                break;
            }
            if track.last_position - track.position < 0.20 {
                return Ok(Some((&track.frame, track.source.gain)));
            }
        }
        Ok(None)
    }
}

fn mix(output: &mut [u8], a: Option<(&[u8], f64)>, b: Option<(&[u8], f64)>) {
    let sample = |frame: Option<(&[u8], f64)>, i| {
        frame.map_or(0.0, |(bytes, gain)| {
            f64::from(i16::from_le_bytes([bytes[i], bytes[i + 1]])) * gain
        })
    };
    for i in (0..output.len()).step_by(2) {
        let value = ((sample(a, i) + sample(b, i)) * DISCORD_GAIN).round() as i16;
        output[i..i + 2].copy_from_slice(&value.to_le_bytes());
    }
}

#[cfg(test)]
#[test]
fn discord_gain_applies_to_single_tracks_and_crossfades() {
    let track = [i16::MAX.to_le_bytes(), i16::MIN.to_le_bytes()].concat();
    let mut output = [0; 4];
    let samples = |frame: [u8; 4]| {
        [i16::from_le_bytes([frame[0], frame[1]]), i16::from_le_bytes([frame[2], frame[3]])]
    };
    mix(&mut output, Some((&track, 1.0)), None);
    assert_eq!(samples(output), [22_937, -22_938]);
    mix(&mut output, Some((&track, 0.5)), Some((&track, 0.5)));
    assert_eq!(samples(output), [22_937, -22_938]);
}

pub(super) fn capture(
    source: BroadcastSource,
    mut output: impl Write,
    stop: &AtomicBool,
    started: impl FnOnce(),
) -> anyhow::Result<()> {
    // Check the pipe API before reporting ready, even if playback is currently paused.
    drop(Pipe::new()?);
    let [mut a, mut b] = [Deck::default(), Deck::default()];
    let mut mixed = SILENCE;
    let mut tick = Instant::now();
    started();
    while !stop.load(Ordering::Relaxed) {
        if let Some(wait) = tick.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        tick = (tick + FRAME_TIME).max(Instant::now());
        let [source_a, source_b] = source.playing();
        let a = a.frame(source_a)?;
        let b = b.frame(source_b)?;
        let frame = match (a, b) {
            (None, None) => &SILENCE[..],
            _ => {
                mix(&mut mixed, a, b);
                &mixed
            }
        };
        output.write_all(frame)?;
    }
    Ok(())
}
