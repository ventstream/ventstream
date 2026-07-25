//! Reproducible EventBus throughput harness for single and batched receives.

use std::hint::black_box;
use std::time::Instant;

use ventstream_core::{Event, EventBus, Payload, ShutdownToken, SourceUri, Subject};

const DEFAULT_EVENTS: usize = 5_000_000;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_CAPACITY: usize = 65_536;
const DEFAULT_RECEIVE_BATCH: usize = 2_000;
const DEFAULT_PRODUCERS: usize = 4;

fn arg_usize(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn test_event(payload_bytes: usize) -> Event {
    let source = SourceUri::new("bench://event-bus").unwrap_or_else(|error| {
        eprintln!("invalid benchmark source URI: {error}");
        std::process::exit(2);
    });
    let subject = Subject::new("bench.events.insert").unwrap_or_else(|error| {
        eprintln!("invalid benchmark subject: {error}");
        std::process::exit(2);
    });
    Event::builder(source, subject)
        .payload(Payload::from_vec(vec![b'x'; payload_bytes]))
        .build()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "batch".into());
    let event_count = arg_usize(2, DEFAULT_EVENTS);
    let payload_bytes = arg_usize(3, DEFAULT_PAYLOAD_BYTES);
    let capacity = arg_usize(4, DEFAULT_CAPACITY);
    let receive_batch = arg_usize(5, DEFAULT_RECEIVE_BATCH).max(1);
    let producer_count = arg_usize(6, DEFAULT_PRODUCERS).max(1);
    let template = test_event(payload_bytes);
    let shutdown = ShutdownToken::new();
    let (sender, mut receiver) = EventBus::new(capacity.max(1)).split();

    let started = Instant::now();
    let mut producers = Vec::with_capacity(producer_count);
    for producer_index in 0..producer_count {
        let producer_sender = sender.clone();
        let producer_shutdown = shutdown.clone();
        let producer_template = template.clone();
        let producer_events = event_count / producer_count
            + usize::from(producer_index < event_count % producer_count);
        producers.push(tokio::spawn(async move {
            for _ in 0..producer_events {
                if let Err(error) = producer_sender
                    .send(producer_template.clone(), &producer_shutdown)
                    .await
                {
                    eprintln!("benchmark producer stopped: {error}");
                    return;
                }
            }
        }));
    }
    drop(sender);

    let mut received = 0usize;
    let mut checksum = 0usize;
    match mode.as_str() {
        "single" => {
            while let Some(event) = receiver.recv().await {
                checksum = checksum.wrapping_add(black_box(event.payload.as_slice().len()));
                received += 1;
            }
        }
        "batch" => {
            let mut buffer = Vec::with_capacity(receive_batch);
            loop {
                let count = receiver.recv_many(&mut buffer, receive_batch).await;
                if count == 0 {
                    break;
                }
                for event in buffer.drain(..) {
                    checksum = checksum.wrapping_add(black_box(event.payload.as_slice().len()));
                }
                received += count;
            }
        }
        other => {
            eprintln!("unknown mode '{other}'; expected single or batch");
            std::process::exit(2);
        }
    }
    for producer in producers {
        if let Err(error) = producer.await {
            eprintln!("benchmark producer task failed: {error}");
            std::process::exit(2);
        }
    }

    let elapsed = started.elapsed();
    let events_per_second = received as f64 / elapsed.as_secs_f64();
    let mib_per_second = events_per_second * payload_bytes as f64 / (1024.0 * 1024.0);
    println!(
        "mode={mode} events={received} payload_bytes={payload_bytes} capacity={capacity} \
         receive_batch={receive_batch} producers={producer_count} elapsed_s={:.6} \
         events_per_s={:.0} payload_mib_per_s={:.1} checksum={checksum}",
        elapsed.as_secs_f64(),
        events_per_second,
        mib_per_second,
    );
}
