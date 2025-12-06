//! Custom Perfetto tracing layer with task-level track support.
//!
//! This module provides a tracing layer that outputs Perfetto format traces
//! with separate tracks for each spawned task.
//!
//! Track ID management is handled by `pluvio_runtime::track`. When spawning tasks
//! with a runtime that has `enable_perfetto_tracks` enabled in its SchedulingConfig,
//! each task will automatically be assigned a unique track ID.

use bytes::BytesMut;
use prost::Message;
use std::cell::RefCell;
use std::collections::HashSet;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::field::{Field, Visit};
use tracing::{span, Event, Id, Subscriber};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

// Re-export track ID functions from pluvio_runtime for convenience
pub use pluvio_runtime::track::{
    acquire_track_id, get_current_track_id, release_track_id, set_current_track_id, tracked,
    TrackedFuture,
};

#[path = "perfetto_protos.rs"]
#[allow(clippy::all)]
#[rustfmt::skip]
mod idl;

thread_local! {
    /// Set of track UUIDs that have had descriptors sent
    static SENT_TRACK_DESCRIPTORS: RefCell<HashSet<u64>> = RefCell::new(HashSet::new());
    /// Base UUID for generating track UUIDs (unique per thread)
    static BASE_TRACK_UUID: u64 = rand::random::<u64>();
}

/// Convert a track ID to a unique track UUID.
fn track_id_to_uuid(track_id: u64) -> u64 {
    BASE_TRACK_UUID.with(|base| {
        base.wrapping_add(track_id.wrapping_mul(0x9E3779B97F4A7C15))
    })
}

/// Check if track descriptor has been sent for this track UUID.
fn is_track_descriptor_sent(track_uuid: u64) -> bool {
    SENT_TRACK_DESCRIPTORS.with(|sent| sent.borrow().contains(&track_uuid))
}

/// Mark track descriptor as sent.
fn mark_track_descriptor_sent(track_uuid: u64) {
    SENT_TRACK_DESCRIPTORS.with(|sent| {
        sent.borrow_mut().insert(track_uuid);
    });
}

// Process descriptor is sent once per process
static PROCESS_DESCRIPTOR_SENT: AtomicBool = AtomicBool::new(false);

/// Writes encoded records into provided instance.
pub trait PerfettoWriter {
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()>;
}

impl<W: for<'writer> MakeWriter<'writer> + 'static> PerfettoWriter for W {
    fn write_log(&self, buf: BytesMut) -> std::io::Result<()> {
        self.make_writer().write_all(&buf)
    }
}

/// A `Layer` that records spans as Perfetto's `TYPE_SLICE_BEGIN`/`TYPE_SLICE_END`,
/// and events as `TYPE_INSTANT`, with task-level track support.
///
/// Each spawned task (when using runtime with `enable_perfetto_tracks`) will
/// appear on a separate track in the Perfetto UI.
pub struct TaskPerfettoLayer<W = fn() -> std::io::Stdout> {
    sequence_id: u64,
    process_track_uuid: u64,
    writer: W,
    with_debug_annotations: bool,
}

impl<W: PerfettoWriter> TaskPerfettoLayer<W> {
    /// Create a new TaskPerfettoLayer with the given writer.
    pub fn new(writer: W) -> Self {
        Self {
            sequence_id: rand::random::<u64>(),
            process_track_uuid: rand::random::<u64>(),
            writer,
            with_debug_annotations: true,
        }
    }

    /// Configure whether to include debug annotations (field values).
    pub fn with_debug_annotations(mut self, value: bool) -> Self {
        self.with_debug_annotations = value;
        self
    }

    fn process_descriptor(&self) -> Option<idl::TracePacket> {
        let sent = PROCESS_DESCRIPTOR_SENT.fetch_or(true, Ordering::SeqCst);
        if sent {
            return None;
        }

        let mut process = idl::ProcessDescriptor::default();
        process.pid = Some(std::process::id() as i32);

        let mut track_desc = idl::TrackDescriptor::default();
        track_desc.uuid = Some(self.process_track_uuid);
        track_desc.process = Some(process);

        let mut packet = idl::TracePacket::default();
        packet.data = Some(idl::trace_packet::Data::TrackDescriptor(track_desc));
        Some(packet)
    }

    fn track_descriptor(&self, track_uuid: u64, track_name: &str) -> Option<idl::TracePacket> {
        if is_track_descriptor_sent(track_uuid) {
            return None;
        }
        mark_track_descriptor_sent(track_uuid);

        let mut thread = idl::ThreadDescriptor::default();
        thread.pid = Some(std::process::id() as i32);
        thread.tid = Some(thread_id::get() as i32);
        thread.thread_name = Some(track_name.to_string());

        let mut track_desc = idl::TrackDescriptor::default();
        track_desc.uuid = Some(track_uuid);
        track_desc.parent_uuid = Some(self.process_track_uuid);
        track_desc.static_or_dynamic_name = Some(
            idl::track_descriptor::StaticOrDynamicName::Name(track_name.to_string())
        );
        track_desc.thread = Some(thread);

        let mut packet = idl::TracePacket::default();
        packet.data = Some(idl::trace_packet::Data::TrackDescriptor(track_desc));
        Some(packet)
    }

    fn write_log(&self, mut log: idl::Trace) {
        let mut buf = BytesMut::new();

        // Insert process descriptor at the beginning if needed
        if let Some(p) = self.process_descriptor() {
            log.packet.insert(0, p);
        }

        let Ok(_) = log.encode(&mut buf) else {
            return;
        };
        let _ = self.writer.write_log(buf);
    }

    fn create_event(
        &self,
        track_uuid: u64,
        name: Option<&str>,
        location: Option<(&str, u32)>,
        debug_annotations: Vec<idl::DebugAnnotation>,
        event_type: Option<idl::track_event::Type>,
    ) -> idl::TrackEvent {
        let mut event = idl::TrackEvent::default();
        event.track_uuid = Some(track_uuid);
        event.categories = vec!["".to_string()];
        if let Some(name) = name {
            event.name_field = Some(idl::track_event::NameField::Name(name.to_string()));
        }
        if let Some(t) = event_type {
            event.set_type(t);
        }
        if !debug_annotations.is_empty() {
            event.debug_annotations = debug_annotations;
        }
        if let Some((file, line)) = location {
            let mut source_location = idl::SourceLocation::default();
            source_location.file_name = Some(file.to_owned());
            source_location.line_number = Some(line);
            event.source_location_field = Some(
                idl::track_event::SourceLocationField::SourceLocation(source_location)
            );
        }
        event
    }
}

/// Data stored with each span to associate it with a track.
struct SpanData {
    track_uuid: u64,
    trace: idl::Trace,
}

/// Visitor for collecting debug annotations from span/event fields.
#[derive(Default)]
struct DebugAnnotations {
    annotations: Vec<idl::DebugAnnotation>,
}

impl Visit for DebugAnnotations {
    fn record_bool(&mut self, field: &Field, value: bool) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::BoolValue(value));
        self.annotations.push(annotation);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::StringValue(value.to_string()));
        self.annotations.push(annotation);
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::DoubleValue(value));
        self.annotations.push(annotation);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::IntValue(value));
        self.annotations.push(annotation);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::IntValue(value as i64));
        self.annotations.push(annotation);
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::StringValue(value.to_string()));
        self.annotations.push(annotation);
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::StringValue(value.to_string()));
        self.annotations.push(annotation);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::StringValue(format!("{value:?}")));
        self.annotations.push(annotation);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        let mut annotation = idl::DebugAnnotation::default();
        annotation.name_field = Some(idl::debug_annotation::NameField::Name(field.name().to_string()));
        annotation.value = Some(idl::debug_annotation::Value::StringValue(format!("{value}")));
        self.annotations.push(annotation);
    }
}

impl<W, S: Subscriber> Layer<S> for TaskPerfettoLayer<W>
where
    S: for<'a> LookupSpan<'a>,
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        // Get current track ID from pluvio_runtime and convert to UUID
        let track_id = get_current_track_id();
        let track_uuid = track_id_to_uuid(track_id);

        // Collect debug annotations
        let mut debug_annotations = DebugAnnotations::default();
        if self.with_debug_annotations {
            attrs.record(&mut debug_annotations);
        }

        // Create the track event
        let event = self.create_event(
            track_uuid,
            Some(span.name()),
            span.metadata().file().zip(span.metadata().line()),
            debug_annotations.annotations,
            Some(idl::track_event::Type::SliceBegin),
        );

        // Create trace packet
        let mut packet = idl::TracePacket::default();
        packet.data = Some(idl::trace_packet::Data::TrackEvent(event));
        packet.timestamp = chrono::Local::now().timestamp_nanos_opt().map(|t| t as u64);
        packet.trusted_pid = Some(std::process::id() as i32);
        packet.optional_trusted_packet_sequence_id = Some(
            idl::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(
                self.sequence_id as u32,
            ),
        );

        // Create trace with track descriptor if needed
        let mut packets = vec![packet];
        let track_name = format!("Task-{}", track_id);
        if let Some(desc) = self.track_descriptor(track_uuid, &track_name) {
            packets.insert(0, desc);
        }

        // Store span data for later
        span.extensions_mut().insert(SpanData {
            track_uuid,
            trace: idl::Trace { packet: packets },
        });
    }

    fn on_record(&self, span_id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(span_id) else {
            return;
        };

        if let Some(data) = span.extensions_mut().get_mut::<SpanData>() {
            if let Some(packet) = data.trace.packet.last_mut() {
                if let Some(idl::trace_packet::Data::TrackEvent(event)) = &mut packet.data {
                    let mut debug_annotations = DebugAnnotations::default();
                    values.record(&mut debug_annotations);
                    event.debug_annotations.append(&mut debug_annotations.annotations);
                }
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let location = metadata.file().zip(metadata.line());

        // Get current track ID from pluvio_runtime
        let track_id = get_current_track_id();
        let track_uuid = track_id_to_uuid(track_id);

        // Collect debug annotations
        let mut debug_annotations = DebugAnnotations::default();
        if self.with_debug_annotations {
            event.record(&mut debug_annotations);
        }

        // Create track event
        let track_event = self.create_event(
            track_uuid,
            Some(metadata.name()),
            location,
            debug_annotations.annotations,
            Some(idl::track_event::Type::Instant),
        );

        // Create packet
        let mut packet = idl::TracePacket::default();
        packet.data = Some(idl::trace_packet::Data::TrackEvent(track_event));
        packet.trusted_pid = Some(std::process::id() as i32);
        packet.timestamp = chrono::Local::now().timestamp_nanos_opt().map(|t| t as u64);
        packet.optional_trusted_packet_sequence_id = Some(
            idl::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(
                self.sequence_id as u32,
            ),
        );

        // If we're in a span, add to that span's trace
        if let Some(span) = ctx.event_span(event) {
            if let Some(data) = span.extensions_mut().get_mut::<SpanData>() {
                data.trace.packet.push(packet);
                return;
            }
        }

        // Otherwise write directly
        let mut packets = vec![packet];
        let track_name = format!("Task-{}", track_id);
        if let Some(desc) = self.track_descriptor(track_uuid, &track_name) {
            packets.insert(0, desc);
        }
        let trace = idl::Trace { packet: packets };
        self.write_log(trace);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };

        let Some(mut data) = span.extensions_mut().remove::<SpanData>() else {
            return;
        };

        // Create closing event
        let meta = span.metadata();
        let event = self.create_event(
            data.track_uuid,
            Some(meta.name()),
            meta.file().zip(meta.line()),
            vec![],
            Some(idl::track_event::Type::SliceEnd),
        );

        // Create closing packet
        let mut packet = idl::TracePacket::default();
        packet.data = Some(idl::trace_packet::Data::TrackEvent(event));
        packet.timestamp = chrono::Local::now().timestamp_nanos_opt().map(|t| t as u64);
        packet.trusted_pid = Some(std::process::id() as i32);
        packet.optional_trusted_packet_sequence_id = Some(
            idl::trace_packet::OptionalTrustedPacketSequenceId::TrustedPacketSequenceId(
                self.sequence_id as u32,
            ),
        );
        data.trace.packet.push(packet);

        // Write the complete trace
        self.write_log(data.trace);
    }
}
