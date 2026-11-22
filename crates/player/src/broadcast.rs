//! A decoder with a raw PCM output, separate from the local Windows audio session.
use super::{loadfile_args, loadfile_has_index, Decks, Error, Mpv, Player};
use std::sync::Arc;

#[derive(Clone)]
pub struct BroadcastSource(Arc<Decks>);

pub struct BroadcastTrack {
    pub position: f64,
    pub speed: f64,
    pub gain: f64,
    path: String,
    deck: usize,
    options: [String; 4],
}

pub struct PcmDecoder {
    _mpv: Mpv,
}

impl Player {
    pub fn broadcast_source(&self) -> BroadcastSource {
        BroadcastSource(self.decks.clone())
    }
}

impl BroadcastSource {
    pub fn playing(&self) -> [Option<BroadcastTrack>; 2] {
        use std::sync::atomic::Ordering;
        let active = self.0.active.load(Ordering::SeqCst);
        let fading = self.0.fading.load(Ordering::Acquire);
        let angle = f64::from_bits(self.0.fade_angle.load(Ordering::Relaxed));
        std::array::from_fn(|deck| {
            if deck == active {
                self.track(deck, if fading { angle.sin() } else { 1.0 })
            } else if fading {
                self.track(deck, angle.cos())
            } else {
                None
            }
        })
    }

    fn track(&self, deck: usize, gain: f64) -> Option<BroadcastTrack> {
        let mpv = self.0.mpv(deck)?;
        if mpv.get_property::<bool>("pause").ok()?
            || mpv.get_property::<bool>("idle-active").ok()?
            || mpv.get_property::<bool>("paused-for-cache").unwrap_or(false)
            || mpv.get_property::<bool>("seeking").unwrap_or(false)
        {
            return None;
        }
        let path = mpv.get_property("path").ok()?;
        let position: f64 = mpv.get_property("time-pos").ok()?;
        if !position.is_finite() || position < 0.0 {
            return None;
        }
        let options = OPTIONS.map(|key| mpv.get_property(key).unwrap_or_default());
        Some(BroadcastTrack { path, deck, position, speed: self.0.speed(), gain, options })
    }
}

// Normalization and effects are in the audio filter graph. The local volume and mute are
// separate mpv properties and are intentionally absent here.
const OPTIONS: [&str; 4] = ["user-agent", "http-header-fields", "http-proxy", "af"];

impl BroadcastTrack {
    pub fn same_source(&self, other: &Self) -> bool {
        self.path == other.path
            && self.deck == other.deck
            && self.speed == other.speed
            && self.options == other.options
    }

    pub fn decode_pcm(&self, output: &str) -> Result<PcmDecoder, Error> {
        let mpv = Mpv::new()?;
        for (key, value) in [
            ("vid", "no"),
            ("ytdl", "no"),
            ("demuxer", "lavf"),
            ("ao", "pcm"),
            ("ao-pcm-file", output),
            ("ao-pcm-waveheader", "no"),
            ("audio-samplerate", "48000"),
            ("audio-channels", "stereo"),
            ("audio-format", "s16"),
            ("audio-buffer", "0.05"),
            ("cache-secs", "4"),
            ("cache-pause", "no"),
        ] {
            mpv.set_property(key, value)?;
        }
        for (key, value) in OPTIONS.into_iter().zip(&self.options) {
            if !value.is_empty() {
                mpv.set_property(key, value.as_str())?;
            }
        }
        mpv.set_property("speed", self.speed)?;
        mpv.set_property("volume", 100.0)?;
        mpv.set_property("mute", false)?;
        let args = loadfile_args(&self.path, Some(self.position), &[], loadfile_has_index(&mpv));
        mpv.command("loadfile", &args.iter().map(String::as_str).collect::<Vec<_>>())?;
        Ok(PcmDecoder { _mpv: mpv })
    }
}
