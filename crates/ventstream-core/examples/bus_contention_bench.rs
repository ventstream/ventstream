//! Isolates event creation/cloning from channel transfer and compares one
//! shared MPSC queue with one bounded queue per producer.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Barrier};
use ventstream_core::{Event, EventBus, EventSender, Payload, ShutdownToken, SourceUri, Subject};

const DEFAULT_EVENTS: usize = 1_000_000;
const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_CAPACITY: usize = 65_536;
const DEFAULT_RECEIVE_BATCH: usize = 2_000;
const DEFAULT_PRODUCERS: usize = 4;

#[derive(Clone, Copy)]
enum Workload {
    Clone,
    PrebuiltMove,
    Build,
}

enum ProducerInput {
    Clone {
        template: Event,
        count: usize,
    },
    PrebuiltMove(Vec<Event>),
    Build {
        source: SourceUri,
        subject: Subject,
        count: usize,
        payload_bytes: usize,
    },
}

fn arg_usize(position: usize, default: usize) -> usize {
    std::env::args()
        .nth(position)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn template_event(payload_bytes: usize) -> Event {
    let source = SourceUri::new("bench://bus-contention").unwrap_or_else(|error| {
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

fn producer_counts(event_count: usize, producer_count: usize) -> Vec<usize> {
    (0..producer_count)
        .map(|index| {
            event_count / producer_count + usize::from(index < event_count % producer_count)
        })
        .collect()
}

fn producer_inputs(
    workload: Workload,
    event_count: usize,
    producer_count: usize,
    payload_bytes: usize,
) -> Vec<ProducerInput> {
    let template = template_event(payload_bytes);
    producer_counts(event_count, producer_count)
        .into_iter()
        .map(|count| match workload {
            Workload::Clone => ProducerInput::Clone {
                template: template.clone(),
                count,
            },
            Workload::PrebuiltMove => ProducerInput::PrebuiltMove(vec![template.clone(); count]),
            Workload::Build => ProducerInput::Build {
                source: template.source.clone(),
                subject: template.subject.clone(),
                count,
                payload_bytes,
            },
        })
        .collect()
}

async fn produce(
    sender: EventSender,
    input: ProducerInput,
    shutdown: ShutdownToken,
    barrier: Arc<Barrier>,
) -> usize {
    barrier.wait().await;
    let mut sent = 0usize;
    match input {
        ProducerInput::Clone { template, count } => {
            for _ in 0..count {
                if sender.send(template.clone(), &shutdown).await.is_err() {
                    break;
                }
                sent += 1;
            }
        }
        ProducerInput::PrebuiltMove(events) => {
            for event in events {
                if sender.send(event, &shutdown).await.is_err() {
                    break;
                }
                sent += 1;
            }
        }
        ProducerInput::Build {
            source,
            subject,
            count,
            payload_bytes,
        } => {
            for _ in 0..count {
                let event = Event::builder(source.clone(), subject.clone())
                    .payload(Payload::from_vec(vec![b'x'; payload_bytes]))
                    .build();
                if sender.send(event, &shutdown).await.is_err() {
                    break;
                }
                sent += 1;
            }
        }
    }
    sent
}

async fn consume(
    mut receiver: ventstream_core::EventReceiver,
    receive_batch: usize,
    expected: usize,
    barrier: Arc<Barrier>,
) -> Vec<Event> {
    let mut received = Vec::with_capacity(expected);
    barrier.wait().await;
    while received.len() < expected {
        if receiver.recv_many(&mut received, receive_batch).await == 0 {
            break;
        }
    }
    received
}

async fn drain_shard(
    mut receiver: ventstream_core::EventReceiver,
    receive_batch: usize,
    expected: usize,
    batches: mpsc::Sender<Vec<Event>>,
    barrier: Arc<Barrier>,
) -> usize {
    let mut drained = 0usize;
    barrier.wait().await;
    while drained < expected {
        let mut batch = Vec::with_capacity(receive_batch.min(expected - drained));
        let count = receiver.recv_many(&mut batch, receive_batch).await;
        if count == 0 {
            break;
        }
        drained += count;
        if batches.send(batch).await.is_err() {
            break;
        }
    }
    drained
}

async fn merge_batches(
    mut batches: mpsc::Receiver<Vec<Event>>,
    expected: usize,
    barrier: Arc<Barrier>,
) -> Vec<Event> {
    let mut merged = Vec::with_capacity(expected);
    barrier.wait().await;
    while merged.len() < expected {
        let Some(mut batch) = batches.recv().await else {
            break;
        };
        merged.append(&mut batch);
    }
    merged
}

async fn run_shared(
    inputs: Vec<ProducerInput>,
    capacity: usize,
    receive_batch: usize,
    event_count: usize,
) -> (Duration, Vec<Vec<Event>>, usize) {
    let producer_count = inputs.len();
    let (sender, receiver) = EventBus::new(capacity).split();
    let shutdown = ShutdownToken::new();
    let barrier = Arc::new(Barrier::new(producer_count + 2));
    let consumer = tokio::spawn(consume(
        receiver,
        receive_batch,
        event_count,
        Arc::clone(&barrier),
    ));
    let producers = inputs
        .into_iter()
        .map(|input| {
            tokio::spawn(produce(
                sender.clone(),
                input,
                shutdown.clone(),
                Arc::clone(&barrier),
            ))
        })
        .collect::<Vec<_>>();
    drop(sender);

    let started = Instant::now();
    barrier.wait().await;
    let received = consumer.await.unwrap_or_default();
    let elapsed = started.elapsed();
    let mut sent = 0usize;
    for producer in producers {
        sent += producer.await.unwrap_or(0);
    }
    (elapsed, vec![received], sent)
}

async fn run_sharded(
    inputs: Vec<ProducerInput>,
    capacity: usize,
    receive_batch: usize,
    event_count: usize,
) -> (Duration, Vec<Vec<Event>>, usize) {
    let producer_count = inputs.len();
    let shard_capacity = capacity.div_ceil(producer_count).max(1);
    let counts = producer_counts(event_count, producer_count);
    let barrier = Arc::new(Barrier::new(producer_count.saturating_mul(2) + 1));
    let shutdown = ShutdownToken::new();
    let mut producers = Vec::with_capacity(producer_count);
    let mut consumers = Vec::with_capacity(producer_count);
    for (input, expected) in inputs.into_iter().zip(counts) {
        let (sender, receiver) = EventBus::new(shard_capacity).split();
        consumers.push(tokio::spawn(consume(
            receiver,
            receive_batch,
            expected,
            Arc::clone(&barrier),
        )));
        producers.push(tokio::spawn(produce(
            sender,
            input,
            shutdown.clone(),
            Arc::clone(&barrier),
        )));
    }

    let started = Instant::now();
    barrier.wait().await;
    let mut received = Vec::with_capacity(producer_count);
    for consumer in consumers {
        received.push(consumer.await.unwrap_or_default());
    }
    let elapsed = started.elapsed();
    let mut sent = 0usize;
    for producer in producers {
        sent += producer.await.unwrap_or(0);
    }
    (elapsed, received, sent)
}

async fn run_merged(
    inputs: Vec<ProducerInput>,
    capacity: usize,
    receive_batch: usize,
    event_count: usize,
) -> (Duration, Vec<Vec<Event>>, usize) {
    let producer_count = inputs.len();
    let shard_capacity = capacity.div_ceil(producer_count).max(1);
    let counts = producer_counts(event_count, producer_count);
    let barrier = Arc::new(Barrier::new(producer_count.saturating_mul(2) + 2));
    let shutdown = ShutdownToken::new();
    let (batch_sender, batch_receiver) = mpsc::channel(producer_count.saturating_mul(2).max(1));
    let merger = tokio::spawn(merge_batches(
        batch_receiver,
        event_count,
        Arc::clone(&barrier),
    ));
    let mut producers = Vec::with_capacity(producer_count);
    let mut drainers = Vec::with_capacity(producer_count);
    for (input, expected) in inputs.into_iter().zip(counts) {
        let (sender, receiver) = EventBus::new(shard_capacity).split();
        drainers.push(tokio::spawn(drain_shard(
            receiver,
            receive_batch,
            expected,
            batch_sender.clone(),
            Arc::clone(&barrier),
        )));
        producers.push(tokio::spawn(produce(
            sender,
            input,
            shutdown.clone(),
            Arc::clone(&barrier),
        )));
    }
    drop(batch_sender);

    let started = Instant::now();
    barrier.wait().await;
    let received = merger.await.unwrap_or_default();
    let elapsed = started.elapsed();
    let mut sent = 0usize;
    for producer in producers {
        sent += producer.await.unwrap_or(0);
    }
    for drainer in drainers {
        let _ = drainer.await;
    }
    (elapsed, vec![received], sent)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "shared-clone".into());
    let event_count = arg_usize(2, DEFAULT_EVENTS).max(1);
    let payload_bytes = arg_usize(3, DEFAULT_PAYLOAD_BYTES);
    let capacity = arg_usize(4, DEFAULT_CAPACITY).max(1);
    let receive_batch = arg_usize(5, DEFAULT_RECEIVE_BATCH).max(1);
    let producer_count = arg_usize(6, DEFAULT_PRODUCERS).max(1);
    let rounds = arg_usize(7, 1).max(1);
    let Some((bus_mode, workload_mode)) = mode.split_once('-') else {
        eprintln!("mode must be shared|sharded|merged plus clone|move|build");
        std::process::exit(2);
    };
    let workload = match workload_mode {
        "clone" => Workload::Clone,
        "move" => Workload::PrebuiltMove,
        "build" => Workload::Build,
        _ => {
            eprintln!("unknown workload '{workload_mode}'; expected clone, move, or build");
            std::process::exit(2);
        }
    };
    let mut elapsed = Duration::ZERO;
    let mut sent = 0usize;
    let mut received = 0usize;
    let mut checksum = 0usize;
    for _ in 0..rounds {
        // Prebuilt events are intentionally materialized before each timer so
        // the move workload measures channel transfer without clone/build cost.
        let inputs = producer_inputs(workload, event_count, producer_count, payload_bytes);
        let (round_elapsed, retained, round_sent) = match bus_mode {
            "shared" => run_shared(inputs, capacity, receive_batch, event_count).await,
            "sharded" => run_sharded(inputs, capacity, receive_batch, event_count).await,
            "merged" => run_merged(inputs, capacity, receive_batch, event_count).await,
            _ => {
                eprintln!("unknown bus '{bus_mode}'; expected shared, sharded, or merged");
                std::process::exit(2);
            }
        };
        elapsed += round_elapsed;
        sent += round_sent;
        received += retained.iter().map(Vec::len).sum::<usize>();
        checksum = retained.iter().flatten().fold(checksum, |sum, event| {
            sum.wrapping_add(black_box(event.payload.len()))
        });
        black_box(retained);
    }
    let expected = event_count.saturating_mul(rounds);
    if sent != expected || received != expected {
        eprintln!("benchmark lost events: expected={expected} sent={sent} received={received}");
        std::process::exit(1);
    }
    let events_per_second = received as f64 / elapsed.as_secs_f64();
    let payload_mib_per_second = events_per_second * payload_bytes as f64 / (1024.0 * 1024.0);
    println!(
        "mode={mode} events={received} event_size_bytes={} payload_bytes={payload_bytes} \
         capacity={capacity} receive_batch={receive_batch} producers={producer_count} \
         rounds={rounds} elapsed_s={:.6} events_per_s={events_per_second:.0} \
         payload_mib_per_s={payload_mib_per_second:.1} checksum={checksum}",
        std::mem::size_of::<Event>(),
        elapsed.as_secs_f64(),
    );
}
