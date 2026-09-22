use super::*;

/// Leave the rest of the lease available for a timed-out request and a retry.
pub fn coalesce_gap(requested_ms: u64, interval_ms: u64) -> Duration {
    Duration::from_millis(requested_ms.min(interval_ms).min(30_000))
}

pub(super) async fn coalesce(shared: &Shared, started: tokio::time::Instant, interruptible: bool) {
    let gap = coalesce_gap(
        shared.coalesce_ms,
        shared.interval_ms.load(Ordering::Relaxed),
    );
    let delay = gap.saturating_sub(started.elapsed());
    if !delay.is_zero() {
        if interruptible {
            let _ = tokio::time::timeout(delay, shared.wake.notified()).await;
        } else {
            tokio::time::sleep(delay).await;
        }
    }
}

/// The serial loop must retry a lost request well before its lease expires.
pub(super) fn beat_timeout(interval_ms: u64, hold_ms: u64) -> Duration {
    if hold_ms == 0 {
        return Duration::from_millis(interval_ms.clamp(50, 30_000) * 3 / 4);
    }
    // The registry may sit on this for `hold_ms` plus its own jitter, and that
    // is the answer arriving on time rather than late. Giving up before then
    // would turn every quiet interval into a failed beat.
    //
    // Keep the normal network margin, but bound it by the hold at short
    // leases. At the 200ms TTL floor this gives 100ms, not 275ms: waiting
    // longer than the lease before retrying expires a healthy upstream.
    Duration::from_millis(hold_ms + hold_ms / 2 + (hold_ms / 2).min(200))
}

pub(super) fn configure_registry_stream(stream: &TcpStream) -> Result<(), String> {
    stream
        .set_nodelay(true)
        .map_err(|e| format!("the connection came up but TCP_NODELAY failed: {e}"))
}

pub(super) struct RegistryConnection {
    stream: TcpStream,
    #[cfg(unix)]
    shared: Arc<Shared>,
    #[cfg(unix)]
    fd: i32,
}

impl RegistryConnection {
    async fn connect(
        shared: Arc<Shared>,
        deadline: tokio::time::Instant,
        budget: Duration,
    ) -> Result<Self, String> {
        let stream = tokio::time::timeout_at(deadline, TcpStream::connect(&shared.endpoint))
            .await
            .map_err(|_| format!("no reply within {}ms", budget.as_millis()))?
            .map_err(|e| format!("cannot reach it: {e}"))?;
        configure_registry_stream(&stream)?;
        #[cfg(unix)]
        {
            let fd = stream.as_raw_fd();
            shared.registry_fds.register(fd);
            shared.registry_connects.fetch_add(1, Ordering::Relaxed);
            Ok(Self { stream, shared, fd })
        }
        #[cfg(not(unix))]
        {
            shared.registry_connects.fetch_add(1, Ordering::Relaxed);
            Ok(Self { stream })
        }
    }
}

impl Drop for RegistryConnection {
    fn drop(&mut self) {
        #[cfg(unix)]
        self.shared.registry_fds.unregister(self.fd);
    }
}

pub(super) async fn read_reply_body<R: AsyncRead + Unpin>(
    reader: &mut R,
    length: usize,
    deadline: tokio::time::Instant,
    budget: Duration,
) -> Result<Vec<u8>, String> {
    tokio::time::timeout_at(deadline, read_frame_body(reader, length))
        .await
        .map_err(|_| format!("reply body stalled past {}ms", budget.as_millis()))?
        .map_err(|e| format!("reply body broke off: {e}"))
}

pub(super) async fn post(
    shared: Arc<Shared>,
    beat: &Beat,
    budget: Duration,
    connection: Option<RegistryConnection>,
) -> Result<(BeatAck, RegistryConnection), String> {
    static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let deadline = tokio::time::Instant::now() + budget;
    let mut connection = match connection {
        Some(connection) => {
            shared.registry_reuses.fetch_add(1, Ordering::Relaxed);
            connection
        }
        None => RegistryConnection::connect(shared, deadline, budget).await?,
    };

    let request = RegistryEnvelope::new(request_id, OP_BEAT, beat);
    tokio::time::timeout_at(
        deadline,
        write_frame(&mut connection.stream, &request, MAX_REQUEST_FRAME_BYTES),
    )
    .await
    .map_err(|_| format!("request write stalled past {}ms", budget.as_millis()))?
    .map_err(|error| match error {
        FrameError::Encode(_) | FrameError::FrameTooLarge { .. } | FrameError::EmptyFrame => {
            format!("cannot encode the beat: {error}")
        }
        _ => format!("the connection came up but the beat write broke off: {error}"),
    })?;

    let length = tokio::time::timeout_at(
        deadline,
        read_frame_length(&mut connection.stream, MAX_RESPONSE_FRAME_BYTES),
    )
    .await
    .map_err(|_| format!("no reply within {}ms", budget.as_millis()))?
    .map_err(|error| {
        format!(
            "the connection came up but the reply was not a bounded registry frame: {error}; \
             whatever answered must speak the native length-prefixed MessagePack protocol"
        )
    })?
    .ok_or_else(|| {
        "the connection came up but closed without a reply; whatever answered must speak the \
         native length-prefixed MessagePack protocol"
            .to_string()
    })?;

    let bytes = read_reply_body(&mut connection.stream, length, deadline, budget).await?;

    let header: RegistryEnvelopeHeader = decode_message(&bytes).map_err(|error| {
        format!(
            "the reply is not a registry envelope: {error}; whatever answered must speak the \
             native length-prefixed MessagePack protocol"
        )
    })?;
    if header.request_id != request_id {
        return Err(format!(
            "the registry replied to request {} while this beat was request {request_id}",
            header.request_id
        ));
    }
    match header.operation.as_str() {
        OP_BEAT_ACK => {
            let envelope: RegistryEnvelope<BeatAck> = decode_message(&bytes)
                .map_err(|e| format!("the reply is not a BeatAck envelope: {e}"))?;
            Ok((envelope.payload, connection))
        }
        OP_ERROR => {
            let envelope: RegistryEnvelope<RegistryProtocolError> = decode_message(&bytes)
                .map_err(|e| format!("the registry's protocol error was malformed: {e}"))?;
            Err(format!(
                "registry protocol error {}: {}",
                envelope.payload.code, envelope.payload.message
            ))
        }
        operation => Err(format!(
            "the registry answered operation {operation:?}, not {OP_BEAT_ACK:?}"
        )),
    }
}

pub fn spawn(shared: Arc<Shared>) -> tokio::runtime::Runtime {
    // Two workers, fixed. tokio defaults to one per core, which on a 128-core
    // trainer node means 128 threads competing with the job for CPU.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("tinyray")
        .build()
        .expect("tokio runtime");

    rt.spawn(async move {
        let mut cancelled_last = false;
        let mut connection = None;
        loop {
            if shared.leaving.load(Ordering::Relaxed) && shared.beats_ok.load(Ordering::Relaxed) > 0
            {
                // One final beat carrying `leaving` was already sent by leave().
                return;
            }
            let started = tokio::time::Instant::now();
            let (mut beat, showing) = shared.compose();
            let interval = shared.interval_ms.load(Ordering::Relaxed);
            // The request that replaces a cancelled one is never parked. It
            // carries something the last one did not, so there is nothing to
            // wait for, and completing it at round-trip speed is what bounds
            // the cost of not being allowed to cancel it: one RTT rather than
            // a whole hold. Measured as subscription latency going from 432ms
            // against a 500ms interval back under 125ms.
            let hold = if cancelled_last {
                0
            } else {
                shared.hold_ms.load(Ordering::Relaxed)
            };
            beat.hold_ms = hold;
            let budget = beat_timeout(interval, hold);
            // A held request is the loop's resting place, so this is also
            // where a publish has to be able to interrupt it. Dropping the
            // future resets the stream; the registry has already renewed the
            // lease from the request that arrived, and a beat is idempotent,
            // so nothing is lost by abandoning the answer.
            let outcome = {
                let sending = post(shared.clone(), &beat, budget, connection.take());
                tokio::pin!(sending);
                tokio::select! {
                    done = &mut sending => Some(done),
                    _ = shared.wake.notified(), if hold > 0 => None,
                }
            };
            // The exchange future owns the stream. End its scope before any
            // coalescing so a publication wake closes a cancelled held beat
            // immediately, rather than leaving its late reply in flight.
            // A publisher faster than the round trip always has something
            // newer to say, so if every publish could cancel, no request would
            // ever be answered: the cache stops updating and flush() never
            // returns, while the member stays registered and looks fine. The
            // replacement is sent unparked (above), which both makes it
            // uncancellable here and lets it complete at round-trip speed.
            cancelled_last = outcome.is_none();
            let Some(outcome) = outcome else {
                // Something to publish. Go straight back round with it, but
                // no faster than the configured gap: a process publishing flat out would
                // otherwise turn the loop into a request generator bounded
                // only by the round trip. Measured under an unthrottled
                // publisher, this halves it -- 24 to 11 requests a second.
                // What keeps such a publisher *alive* is the unparked
                // replacement above, not this.
                coalesce(&shared, started, false).await;
                continue;
            };
            match outcome {
                Ok((ack, returned)) => {
                    let previous_epoch = shared.seen_epoch.load(Ordering::Relaxed);
                    let restarted = previous_epoch != 0 && previous_epoch != ack.epoch;
                    shared.interval_ms.store(ack.ttl_ms / 4, Ordering::Relaxed);
                    // Ask to be parked for the interval we would have slept.
                    // Same number of requests as the polling this replaces;
                    // the difference is that the answer now arrives when
                    // something happens rather than when the timer says so.
                    shared
                        .hold_ms
                        .store((ack.ttl_ms / 4).clamp(50, 30_000), Ordering::Relaxed);
                    shared.note_registry(&ack);
                    let (alive, changed) = shared.apply(&ack);
                    shared.beats_ok.fetch_add(1, Ordering::Relaxed);
                    shared.mark_ok();
                    // Only an accepted beat left the state anywhere. A refusal
                    // -- the seat taken by a later tenure, a state over the
                    // size cap, a pool whose shape we disagree with -- comes
                    // back as an ordinary reply with `accepted: false`, and
                    // the registry never stored what it carried. Counting it
                    // as confirmed made flush() report that the registry had
                    // the state when it had refused it: measured as
                    // test_flush_says_the_seat_was_taken returning instead of
                    // raising, intermittently, depending on whether the loop
                    // got a refusal in before it stopped.
                    if alive {
                        shared.confirmed.fetch_max(showing, Ordering::Relaxed);
                    }
                    shared.acked.notify_one();
                    shared.note_beat();
                    if changed {
                        shared.ring();
                    }
                    if !alive {
                        // Superseded. Beating on would only be waiting for the
                        // replacement to die so we could take the seat back.
                        return;
                    }
                    if !restarted {
                        connection = Some(returned);
                    }
                }
                // Losing the registry is survivable: lookups keep working from
                // cache, and the roster regrows within one interval when it
                // comes back. Nothing here needs to escalate.
                Err(why) => {
                    shared.beats_failed.fetch_add(1, Ordering::Relaxed);
                    *shared.last_error.lock().unwrap() = why;
                    shared.note_beat();
                }
            }
            // Whether to sleep is decided by what this loop *intends* to do,
            // not by what the last request happened to ask for. Reading the
            // per-request `hold` here confused an unparked replacement with a
            // registry too old to park anything, and sent the loop to sleep
            // for a whole interval -- unparked, so the registry could no
            // longer reach it. Measured on a superseded member: it took 940ms
            // to notice it had been fenced, against 1-2ms when parked.
            if shared.hold_ms.load(Ordering::Relaxed) == 0 {
                shared.short_polls.fetch_add(1, Ordering::Relaxed);
                let ms = shared.interval_ms.load(Ordering::Relaxed).clamp(50, 30_000);
                // Wake early if the process has something new to say.
                let _ =
                    tokio::time::timeout(Duration::from_millis(ms), shared.wake.notified()).await;
            } else {
                // The wait now happens inside the request, so the only reason
                // to pause is to keep a pool that changes constantly from
                // turning this into a spin: the answer would come back at once
                // every time, and we would ask again just as fast.
                coalesce(&shared, started, true).await;
            }
        }
    });
    rt
}

/// Send one beat synchronously, used by join() and leave() so that arrival and
/// departure are visible immediately instead of at the next tick.
///
/// `stop_when_registered` is for join(): the loop is already beating alongside
/// this call, so the caller can be registered by an ack this request knows
/// nothing about. Measured on a 40%-loss link, `join(timeout=30)` six times:
/// the loop was acked at 0.01s and this call still sat there until its 5s
/// budget ran out, three times out of six. leave() passes false -- its beat
/// carries `leaving`, and no other beat can say that for it.
pub fn beat_once(
    rt: &tokio::runtime::Runtime,
    shared: &Arc<Shared>,
    budget: Duration,
    stop_when_registered: bool,
) -> bool {
    let s = shared.clone();
    rt.block_on(async move {
        // Already done by the loop before we even started: nothing to send.
        if stop_when_registered && s.beats_ok.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let (mut beat, showing) = s.compose();
        // One-shot, with a caller waiting: never parked, and given the
        // caller's budget rather than the loop's. A fixed five seconds here
        // meant join(timeout=) could not make the call shorter -- only longer.
        beat.hold_ms = 0;
        let sending = post(s.clone(), &beat, budget, None);
        tokio::pin!(sending);
        // `notify_one` leaves a permit when nobody is waiting, so an ack that
        // lands between the check above and this select is not missed. A
        // permit left over from an older ack cannot mislead us: it implies
        // beats_ok > 0, which returned already.
        let landed = tokio::select! {
            done = &mut sending => Some(done),
            _ = s.acked.notified(), if stop_when_registered => None,
        };
        let Some(landed) = landed else {
            // The loop got there first. Dropping the request in flight is what
            // the loop already does to swap a parked beat for a fresher one:
            // a beat is idempotent and the lease is renewed by the one that
            // arrived, so there is nothing to finish.
            return true;
        };
        let out = match landed {
            Ok((ack, _connection)) => {
                s.interval_ms.store(ack.ttl_ms / 4, Ordering::Relaxed);
                s.hold_ms
                    .store((ack.ttl_ms / 4).clamp(50, 30_000), Ordering::Relaxed);
                s.note_registry(&ack);
                let (alive, changed) = s.apply(&ack);
                s.beats_ok.fetch_add(1, Ordering::Relaxed);
                s.mark_ok();
                // Same rule as the loop: a refusal is an ordinary reply that
                // stored nothing, so it confirms nothing.
                if alive {
                    s.confirmed.fetch_max(showing, Ordering::Relaxed);
                }
                s.note_beat();
                if changed {
                    s.ring();
                }
                true
            }
            Err(why) => {
                s.beats_failed.fetch_add(1, Ordering::Relaxed);
                *s.last_error.lock().unwrap() = why;
                s.note_beat();
                // The request failed, but the loop may have landed one while
                // it was failing -- and the caller asked to be registered,
                // not to have this particular packet arrive.
                stop_when_registered && s.beats_ok.load(Ordering::Relaxed) > 0
            }
        };
        out
    })
}
