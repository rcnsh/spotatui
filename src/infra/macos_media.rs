//! macOS Now Playing / Media Key integration
//!
//! Exposes spotatui as a controllable media player via macOS's MediaPlayer framework, enabling:
//! - Media key support (play/pause, next, previous)
//! - Control Center / Touch Bar Now Playing widget
//! - Headphone button controls
//!
//! This module is only available on macOS with the `macos-media` feature enabled.

use anyhow::Result;
use block2::RcBlock;
use log::info;
use objc2::msg_send;
use objc2::runtime::{AnyClass, AnyObject};
use objc2::AnyThread;
use objc2_app_kit::NSImage;
use objc2_foundation::{NSData, NSDate, NSMutableDictionary, NSNumber, NSRunLoop, NSString};
use objc2_media_player::{
  MPChangePlaybackPositionCommandEvent, MPChangeRepeatModeCommandEvent,
  MPChangeShuffleModeCommandEvent, MPMediaItemArtwork, MPMediaItemPropertyAlbumTitle,
  MPMediaItemPropertyArtist, MPMediaItemPropertyArtwork, MPMediaItemPropertyPlaybackDuration,
  MPMediaItemPropertyTitle, MPNowPlayingInfoCenter, MPNowPlayingInfoPropertyElapsedPlaybackTime,
  MPNowPlayingInfoPropertyPlaybackRate, MPNowPlayingPlaybackState, MPRemoteCommandCenter,
  MPRemoteCommandEvent, MPRemoteCommandHandlerStatus, MPRepeatType, MPShuffleType,
};
use std::ptr::NonNull;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;

/// How an external controller asked us to repeat.
///
/// Mirrors `MPRepeatType` rather than re-using rspotify's `RepeatState`, so this
/// module stays free of the Spotify types: the mapping onto whichever player
/// owns playback belongs to the caller, exactly as it does for MPRIS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacRepeatMode {
  Off,
  /// Repeat the current track.
  One,
  /// Repeat the whole context.
  All,
}

/// Events that can be received from external macOS media controls (media keys, Control Center, etc.)
#[derive(Debug, Clone)]
pub enum MacMediaEvent {
  PlayPause,
  Play,
  Pause,
  Next,
  Previous,
  Stop,
  /// Absolute seek target in milliseconds, from a Now Playing scrubber.
  ///
  /// `MPChangePlaybackPositionCommandEvent` reports an absolute time, unlike
  /// MPRIS `Seek`, which is a relative offset — so this needs no current
  /// position to resolve against.
  Seek(u32),
  SetShuffle(bool),
  SetRepeat(MacRepeatMode),
}

/// Commands to send TO the Now Playing center to update its state
#[derive(Debug, Clone)]
#[allow(dead_code, clippy::enum_variant_names)]
pub enum MacMediaCommand {
  SetMetadata {
    title: String,
    artists: Vec<String>,
    album: String,
    duration_ms: u32,
    art_url: Option<String>,
  },
  SetPlaybackStatus(bool), // true = playing, false = paused
  SetPosition(u64),        // position in milliseconds
  SetVolume(u8),           // 0-100 (not directly supported by Now Playing, but kept for API parity)
  SetStopped,
  /// Publish the shuffle state back to the shuffle command.
  ///
  /// Now Playing reads shuffle and repeat off the *command* objects rather than
  /// the info dictionary, so a client only ever sees these once they are pushed
  /// here — without it a notch or Control Center draws both toggles as off no
  /// matter what the player is doing.
  SetShuffle(bool),
  SetRepeat(MacRepeatMode),
}

/// Manager for the macOS Now Playing integration
pub struct MacMediaManager {
  event_rx: std::sync::Mutex<Option<mpsc::UnboundedReceiver<MacMediaEvent>>>,
  command_tx: mpsc::UnboundedSender<MacMediaCommand>,
}

impl MacMediaManager {
  /// Create and start the macOS media integration
  ///
  /// Registers command handlers with MPRemoteCommandCenter and sets up Now Playing info
  /// The handler runs in a dedicated thread because it requires the main run loop
  pub fn new() -> Result<Self> {
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<MacMediaCommand>();

    // Clone event_tx for use in callbacks
    let event_tx = Arc::new(event_tx);

    // MPRemoteCommandCenter requires an initialized NSApplication to route media key events;
    // without one, macOS ignores the handlers and falls through to Music.app.
    thread::spawn(move || {
      // Initialize NSApplication with raw msg_send because objc2-app-kit's
      // sharedApplication() requires MainThreadMarker (unavailable in CLI apps).
      unsafe {
        let cls = AnyClass::get(c"NSApplication").expect("NSApplication class not found");
        let app: objc2::rc::Retained<AnyObject> = msg_send![cls, sharedApplication];
        // NSApplicationActivationPolicyProhibited = 2 (no Dock icon, no menu bar)
        let _activation_policy_set: bool = msg_send![&app, setActivationPolicy: 2isize];
      }
      info!("macos media: NSApplication initialized with Prohibited activation policy");

      // Get the shared command center
      let command_center = unsafe { MPRemoteCommandCenter::sharedCommandCenter() };

      // Set up play command handler
      let tx = Arc::clone(&event_tx);
      let play_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |_event: NonNull<MPRemoteCommandEvent>| {
        info!("macos media: received Play event");
        let _ = tx.send(MacMediaEvent::Play);
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        command_center
          .playCommand()
          .addTargetWithHandler(&play_handler);
      }

      // Set up pause command handler
      let tx = Arc::clone(&event_tx);
      let pause_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |_event: NonNull<MPRemoteCommandEvent>| {
        info!("macos media: received Pause event");
        let _ = tx.send(MacMediaEvent::Pause);
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        command_center
          .pauseCommand()
          .addTargetWithHandler(&pause_handler);
      }

      // Set up toggle play/pause command handler
      let tx = Arc::clone(&event_tx);
      let toggle_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |_event: NonNull<MPRemoteCommandEvent>| {
        info!("macos media: received PlayPause event");
        let _ = tx.send(MacMediaEvent::PlayPause);
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        command_center
          .togglePlayPauseCommand()
          .addTargetWithHandler(&toggle_handler);
      }

      // Set up next track command handler
      let tx = Arc::clone(&event_tx);
      let next_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |_event: NonNull<MPRemoteCommandEvent>| {
        info!("macos media: received Next event");
        let _ = tx.send(MacMediaEvent::Next);
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        command_center
          .nextTrackCommand()
          .addTargetWithHandler(&next_handler);
      }

      // Set up previous track command handler
      let tx = Arc::clone(&event_tx);
      let prev_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |_event: NonNull<MPRemoteCommandEvent>| {
        info!("macos media: received Previous event");
        let _ = tx.send(MacMediaEvent::Previous);
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        command_center
          .previousTrackCommand()
          .addTargetWithHandler(&prev_handler);
      }

      // Set up stop command handler
      let tx = Arc::clone(&event_tx);
      let stop_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |_event: NonNull<MPRemoteCommandEvent>| {
        info!("macos media: received Stop event");
        let _ = tx.send(MacMediaEvent::Stop);
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        command_center
          .stopCommand()
          .addTargetWithHandler(&stop_handler);
      }

      // Scrubbing a Now Playing progress bar arrives here as an absolute
      // position, in seconds. Without this target the command stays disabled and
      // every client — Control Center, a notch, the Touch Bar — refuses the
      // drag rather than sending it.
      let tx = Arc::clone(&event_tx);
      let position_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |event: NonNull<MPRemoteCommandEvent>| {
        // The command centre hands each command its own event subclass; this
        // target is only ever attached to `changePlaybackPositionCommand`.
        let position_s = unsafe {
          event
            .cast::<MPChangePlaybackPositionCommandEvent>()
            .as_ref()
            .positionTime()
        };
        if !position_s.is_finite() || position_s < 0.0 {
          return MPRemoteCommandHandlerStatus::CommandFailed;
        }
        let position_ms = (position_s * 1000.0) as u32;
        info!("macos media: received Seek event ({position_ms} ms)");
        let _ = tx.send(MacMediaEvent::Seek(position_ms));
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        let command = command_center.changePlaybackPositionCommand();
        command.setEnabled(true);
        command.addTargetWithHandler(&position_handler);
      }

      let tx = Arc::clone(&event_tx);
      let shuffle_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |event: NonNull<MPRemoteCommandEvent>| {
        let shuffle_type = unsafe {
          event
            .cast::<MPChangeShuffleModeCommandEvent>()
            .as_ref()
            .shuffleType()
        };
        // `Collections` shuffles groups rather than tracks; spotatui has one
        // shuffle, so anything that is not `Off` turns it on.
        let enabled = shuffle_type != MPShuffleType::Off;
        info!("macos media: received SetShuffle event ({enabled})");
        let _ = tx.send(MacMediaEvent::SetShuffle(enabled));
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        let command = command_center.changeShuffleModeCommand();
        command.setEnabled(true);
        command.addTargetWithHandler(&shuffle_handler);
      }

      let tx = Arc::clone(&event_tx);
      let repeat_handler: RcBlock<
        dyn Fn(NonNull<MPRemoteCommandEvent>) -> MPRemoteCommandHandlerStatus,
      > = RcBlock::new(move |event: NonNull<MPRemoteCommandEvent>| {
        let repeat_type = unsafe {
          event
            .cast::<MPChangeRepeatModeCommandEvent>()
            .as_ref()
            .repeatType()
        };
        let mode = match repeat_type {
          MPRepeatType::One => MacRepeatMode::One,
          MPRepeatType::All => MacRepeatMode::All,
          // `Off`, and any value a future macOS adds: repeating nothing is the
          // safe reading of a mode we do not recognise.
          _ => MacRepeatMode::Off,
        };
        info!("macos media: received SetRepeat event ({mode:?})");
        let _ = tx.send(MacMediaEvent::SetRepeat(mode));
        MPRemoteCommandHandlerStatus::Success
      });
      unsafe {
        let command = command_center.changeRepeatModeCommand();
        command.setEnabled(true);
        command.addTargetWithHandler(&repeat_handler);
      }

      info!("macos media: remote command handlers registered");

      // Get the now playing info center
      let info_center = unsafe { MPNowPlayingInfoCenter::defaultCenter() };

      // Interleave command processing with NSRunLoop ticks so macOS can deliver
      // MPRemoteCommandCenter events to our handler blocks.
      let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create macOS media runtime");

      rt.block_on(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
          tokio::select! {
            Some(cmd) = command_rx.recv() => {
              handle_now_playing_command(&cmd, &info_center, &command_center).await;
            }
            _ = interval.tick() => {
              NSRunLoop::currentRunLoop()
                .runUntilDate(&NSDate::dateWithTimeIntervalSinceNow(0.01));
            }
          }
        }
      });
    });

    Ok(Self {
      event_rx: std::sync::Mutex::new(Some(event_rx)),
      command_tx,
    })
  }

  /// Take the event receiver for handling external control requests
  ///
  /// This can only be called once; subsequent calls return None
  pub fn take_event_rx(&self) -> Option<mpsc::UnboundedReceiver<MacMediaEvent>> {
    self.event_rx.lock().ok()?.take()
  }

  /// Update track metadata
  pub fn set_metadata(
    &self,
    title: &str,
    artists: &[String],
    album: &str,
    duration_ms: u32,
    art_url: Option<String>,
  ) {
    let _ = self.command_tx.send(MacMediaCommand::SetMetadata {
      title: title.to_string(),
      artists: artists.to_vec(),
      album: album.to_string(),
      duration_ms,
      art_url,
    });
  }

  /// Update playback status
  pub fn set_playback_status(&self, is_playing: bool) {
    let _ = self
      .command_tx
      .send(MacMediaCommand::SetPlaybackStatus(is_playing));
  }

  /// Update playback position
  pub fn set_position(&self, position_ms: u64) {
    let _ = self
      .command_tx
      .send(MacMediaCommand::SetPosition(position_ms));
  }

  /// Update volume (0-100) - kept for API parity with MPRIS
  #[allow(dead_code)]
  pub fn set_volume(&self, volume_percent: u8) {
    let _ = self
      .command_tx
      .send(MacMediaCommand::SetVolume(volume_percent));
  }

  /// Mark playback as stopped
  pub fn set_stopped(&self) {
    let _ = self.command_tx.send(MacMediaCommand::SetStopped);
  }

  /// Publish the current shuffle state so external controllers draw it correctly.
  pub fn set_shuffle(&self, enabled: bool) {
    let _ = self.command_tx.send(MacMediaCommand::SetShuffle(enabled));
  }

  /// Publish the current repeat mode so external controllers draw it correctly.
  pub fn set_repeat(&self, mode: MacRepeatMode) {
    let _ = self.command_tx.send(MacMediaCommand::SetRepeat(mode));
  }
}

/// Process a single Now Playing command, updating the info center state.
/// Must be called from the dedicated macOS media thread that owns `info_center`
/// and `command_center`.
async fn handle_now_playing_command(
  cmd: &MacMediaCommand,
  info_center: &MPNowPlayingInfoCenter,
  command_center: &MPRemoteCommandCenter,
) {
  match cmd {
    MacMediaCommand::SetMetadata {
      title,
      artists,
      album,
      duration_ms,
      art_url,
    } => {
      let artwork = match art_url.as_deref() {
        Some(url) => fetch_artwork_from_url(url).await,
        None => None,
      };

      unsafe {
        let dict: objc2::rc::Retained<NSMutableDictionary<NSString, AnyObject>> =
          NSMutableDictionary::new();

        let title_ns = NSString::from_str(title);
        dict.insert(MPMediaItemPropertyTitle, &*title_ns);

        let artist_ns = NSString::from_str(&artists.join(", "));
        dict.insert(MPMediaItemPropertyArtist, &*artist_ns);

        let album_ns = NSString::from_str(album);
        dict.insert(MPMediaItemPropertyAlbumTitle, &*album_ns);

        let duration = NSNumber::numberWithDouble(f64::from(*duration_ms) / 1000.0);
        dict.insert(MPMediaItemPropertyPlaybackDuration, &*duration);

        let rate = NSNumber::numberWithDouble(1.0);
        dict.insert(MPNowPlayingInfoPropertyPlaybackRate, &*rate);

        if let Some(artwork) = artwork.as_ref() {
          dict.insert(MPMediaItemPropertyArtwork, &**artwork);
        }

        info_center.setNowPlayingInfo(Some(&dict));
      }
    }
    MacMediaCommand::SetPlaybackStatus(is_playing) => unsafe {
      let state = if *is_playing {
        MPNowPlayingPlaybackState::Playing
      } else {
        MPNowPlayingPlaybackState::Paused
      };
      info_center.setPlaybackState(state);

      // Update playback rate in the existing nowPlayingInfo so macOS
      // knows whether to advance the elapsed time counter.
      if let Some(existing) = info_center.nowPlayingInfo() {
        let dict: objc2::rc::Retained<NSMutableDictionary<NSString, AnyObject>> =
          NSMutableDictionary::dictionaryWithDictionary(&existing);
        let rate = NSNumber::numberWithDouble(if *is_playing { 1.0 } else { 0.0 });
        dict.insert(MPNowPlayingInfoPropertyPlaybackRate, &*rate);
        info_center.setNowPlayingInfo(Some(&dict));
      }
    },
    MacMediaCommand::SetPosition(position_ms) => unsafe {
      // Update elapsed playback time in the existing nowPlayingInfo dict
      if let Some(existing) = info_center.nowPlayingInfo() {
        let dict: objc2::rc::Retained<NSMutableDictionary<NSString, AnyObject>> =
          NSMutableDictionary::dictionaryWithDictionary(&existing);
        let elapsed = NSNumber::numberWithDouble(*position_ms as f64 / 1000.0);
        dict.insert(MPNowPlayingInfoPropertyElapsedPlaybackTime, &*elapsed);
        info_center.setNowPlayingInfo(Some(&dict));
      }
    },
    MacMediaCommand::SetVolume(_) => {
      // Volume is not directly supported by Now Playing center
    }
    MacMediaCommand::SetShuffle(enabled) => unsafe {
      // `Items` is the only shuffle spotatui has; `Collections` would claim we
      // shuffle groups of tracks, which no source does.
      let shuffle_type = if *enabled {
        MPShuffleType::Items
      } else {
        MPShuffleType::Off
      };
      command_center
        .changeShuffleModeCommand()
        .setCurrentShuffleType(shuffle_type);
    },
    MacMediaCommand::SetRepeat(mode) => unsafe {
      let repeat_type = match mode {
        MacRepeatMode::Off => MPRepeatType::Off,
        MacRepeatMode::One => MPRepeatType::One,
        MacRepeatMode::All => MPRepeatType::All,
      };
      command_center
        .changeRepeatModeCommand()
        .setCurrentRepeatType(repeat_type);
    },
    MacMediaCommand::SetStopped => unsafe {
      info_center.setPlaybackState(MPNowPlayingPlaybackState::Stopped);
      info_center.setNowPlayingInfo(None);
    },
  }
}

async fn fetch_artwork_from_url(art_url: &str) -> Option<objc2::rc::Retained<MPMediaItemArtwork>> {
  let response = reqwest::get(art_url).await.ok()?;
  if !response.status().is_success() {
    return None;
  }

  let bytes = response.bytes().await.ok()?;
  if bytes.is_empty() {
    return None;
  }

  unsafe {
    let data = NSData::dataWithBytes_length(bytes.as_ptr().cast(), bytes.len());
    let image = NSImage::initWithData(NSImage::alloc(), &data)?;
    let image_for_handler = image.clone();
    let request_handler =
      RcBlock::new(move |_requested_size| NonNull::from(image_for_handler.as_ref()));

    Some(MPMediaItemArtwork::initWithBoundsSize_requestHandler(
      MPMediaItemArtwork::alloc(),
      image.size(),
      &request_handler,
    ))
  }
}
