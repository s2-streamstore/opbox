#[cfg(not(tokio_unstable))]
compile_error!("opbox-sim requires --cfg tokio_unstable so turmoil can seed Tokio's runtime RNG");

mod fault_injecting_file_io;
mod in_memory_file_io;
mod s2_connector;
mod s2_server;

use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use eyre::eyre;
use fault_injecting_file_io::FaultInjectingFileIO;
use in_memory_file_io::{InMemoryFileIO, InMemoryFileIOStats};
use opbox_core::app::connectivity::{ConnectivitySnapshot, LinkState};
use opbox_core::app::db::{initialize_database, open_memory_database, semantic_pool};
use opbox_core::app::runtime::{AppRuntime, AppRuntimeConfig, RunMode};
use opbox_core::crdt::types::ObjectId;
use opbox_core::engine::actor::{EngineCommand, EngineStatusConfig};
use opbox_core::fs::types::{RelativePath, ScanScope};
use opbox_core::notify::nio::channel_notify_io;
use opbox_core::semantic::service::{SemanticDebugSnapshot, SemanticService};
use opbox_core::semantic::table::daemon_state;
use opbox_core::types::{DaemonWriterId, OutboxId, WorkspaceId};
use rand::SeedableRng;
use s2_sdk::S2;
use s2_sdk::types::{
    AccountEndpoint, BasinConfig, BasinEndpoint, BasinName, BasinReconfiguration, CreateBasinInput,
    EncryptionAlgorithm, EncryptionKey, ListStreamsInput, ReconfigureBasinInput, S2Config,
    S2Endpoints, S2Error,
};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const SIM_BASIN: &str = "opbox-sim";
const SAME_FILE_MIN_EDITS_PER_DAEMON: u64 = 5;
const SAME_FILE_MAX_EDITS_PER_DAEMON: u64 = 1000;
const DAEMON_A_S2_LINK_LATENCY_MS: u64 = 30;
const DAEMON_B_S2_LINK_LATENCY_MS: u64 = 200;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Single(SingleArgs),
    Meta(MetaArgs),
    Parallel(ParallelArgs),
    Sweep(SweepArgs),
}

impl Args {
    fn quiet(&self) -> bool {
        match &self.command {
            Command::Single(args) => args.quiet,
            Command::Meta(_) => false,
            Command::Parallel(_) => false,
            Command::Sweep(_) => false,
        }
    }
}

#[derive(Parser, Debug, Clone)]
struct CommonRunArgs {
    #[arg(value_enum, default_value_t = Workload::General)]
    workload: Workload,
    #[arg(long)]
    seed: Option<u64>,
    /// Upper bound on 250ms controller poll iterations; runs end early on
    /// convergence. Must comfortably exceed the daemon's periodic full-scan
    /// interval (120s) for workloads that rely on it to notice local writes.
    #[arg(long, default_value_t = 720)]
    max_steps: u64,
    #[arg(long, default_value_t = 0.0)]
    failure_rate: f64,
}

#[derive(Parser, Debug)]
struct SingleArgs {
    #[command(flatten)]
    common: CommonRunArgs,
    #[arg(long)]
    quiet: bool,
}

#[derive(Parser, Debug)]
struct MetaArgs {
    #[command(flatten)]
    common: CommonRunArgs,
    /// RUST_LOG filter to use for both child single-run processes.
    #[arg(long, value_name = "FILTER", default_value = "trace")]
    child_proc_log_level: String,
}

#[derive(Parser, Debug)]
struct ParallelArgs {
    #[command(flatten)]
    common: CommonRunArgs,
    #[arg(long, default_value_t = 100)]
    trials: u64,
    #[arg(long)]
    jobs: Option<usize>,
    /// Directory for per-seed child stdout/stderr logs.
    #[arg(long)]
    output_dir: Option<PathBuf>,
    /// RUST_LOG filter to pass to child single-run processes.
    #[arg(long, value_name = "FILTER")]
    child_proc_log_level: Option<String>,
}

/// Run `parallel` across every workload (or a chosen subset), continuing past
/// per-workload failures and summarizing at the end.
#[derive(Parser, Debug)]
struct SweepArgs {
    /// Workloads to sweep; all of them when omitted.
    #[arg(value_enum, num_args = 0..)]
    workloads: Vec<Workload>,
    /// Starting seed for every workload's trial range; random per workload
    /// when omitted.
    #[arg(long)]
    seed: Option<u64>,
    /// See CommonRunArgs::max_steps.
    #[arg(long, default_value_t = 720)]
    max_steps: u64,
    #[arg(long, default_value_t = 0.0)]
    failure_rate: f64,
    #[arg(long, default_value_t = 25)]
    trials: u64,
    #[arg(long)]
    jobs: Option<usize>,
    /// Directory for per-seed child stdout/stderr logs, in per-workload
    /// subdirectories.
    #[arg(long)]
    output_dir: Option<PathBuf>,
    /// RUST_LOG filter to pass to child single-run processes.
    #[arg(long, value_name = "FILTER")]
    child_proc_log_level: Option<String>,
}

#[derive(Debug, Clone, Copy, strum::IntoStaticStr, ValueEnum)]
#[strum(serialize_all = "kebab-case")]
enum Workload {
    General,
    ProjectionStorm,
    SamePathCreateConflict,
    ManyFileConflictStorm,
    DeleteVsEdit,
    ConflictPlusLaterEdit,
    SafeSaveAfterQuiescence,
    RenameAfterQuiescence,
    ScopedScan,
    LargeTextMultipart,
    OfflineReconnectMerge,
    SameFileEdits,
    SameFileSplitEdits,
    OrphanedProjectionWrite,
    FaultReadChangedBetweenStats,
    FaultProjectionConflictBeforeSwap,
    FaultProjectionConflictAfterSwap,
    FaultProjectionTempLeak,
    ClearExistingFile,
    ClearViaSafeSave,
    ClearBeforeQuiescence,
    PartitionBothDaemons,
    FlappingConnectivity,
    AsymmetricPartition,
    PartitionDuringProjection,
    MessageHoldAndRelease,
    TransientHighPacketLoss,
    SustainedEditingStress,
    DeleteWhileOffline,
    BinaryFileIgnore,
    PermissionDeniedIgnore,
    TooLargeIgnore,
    WrongCipherClone,
}

impl Workload {
    fn as_str(self) -> &'static str {
        self.into()
    }
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    init_tracing(args.quiet());
    match args.command {
        Command::Single(args) => run_single(args),
        Command::Meta(args) => run_meta(args),
        Command::Parallel(args) => run_parallel(args),
        Command::Sweep(args) => run_sweep(args),
    }
}

fn run_single(args: SingleArgs) -> eyre::Result<()> {
    let seed = args.common.seed.unwrap_or_else(random_seed);
    seed_rng(seed);
    let _clock_guard = mad_turmoil::time::SimClocksGuard::init();
    info!(seed, ?args.common.workload, max_steps = args.common.max_steps, "opbox sim starting");

    let mut sim = init_sim(seed, args.common.failure_rate);
    sim.host("s2-lite", move || async move {
        s2_server::run_s2_lite_server(seed)
            .await
            .map_err(|err| Box::new(std::io::Error::other(err.to_string())) as Box<_>)?;
        Ok(())
    });

    match args.common.workload {
        Workload::General => run_general_workload(&mut sim, seed, args.common.max_steps)?,
        Workload::ProjectionStorm => {
            run_projection_storm_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::SamePathCreateConflict => {
            run_same_path_create_conflict_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::ManyFileConflictStorm => {
            run_many_file_conflict_storm_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::DeleteVsEdit => {
            run_delete_vs_edit_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::ConflictPlusLaterEdit => {
            run_conflict_plus_later_edit_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::SafeSaveAfterQuiescence => {
            run_safe_save_after_quiescence_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::RenameAfterQuiescence => {
            run_rename_after_quiescence_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::ScopedScan => run_scoped_scan_workload(&mut sim, seed, args.common.max_steps)?,
        Workload::LargeTextMultipart => {
            run_large_text_multipart_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::OfflineReconnectMerge => {
            run_offline_reconnect_merge_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::SameFileEdits => run_same_file_edits_workload(
            &mut sim,
            seed,
            args.common.max_steps,
            SameFileEditPattern::BothAppend,
        )?,
        Workload::SameFileSplitEdits => run_same_file_edits_workload(
            &mut sim,
            seed,
            args.common.max_steps,
            SameFileEditPattern::PrependAAppendB,
        )?,
        Workload::OrphanedProjectionWrite => {
            run_orphaned_projection_write_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::FaultReadChangedBetweenStats => {
            run_fault_read_changed_between_stats_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::FaultProjectionConflictBeforeSwap => {
            run_fault_projection_conflict_before_swap_workload(
                &mut sim,
                seed,
                args.common.max_steps,
            )?
        }
        Workload::FaultProjectionConflictAfterSwap => {
            run_fault_projection_conflict_after_swap_workload(
                &mut sim,
                seed,
                args.common.max_steps,
            )?
        }
        Workload::FaultProjectionTempLeak => {
            run_fault_projection_temp_leak_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::ClearExistingFile => {
            run_clear_existing_file_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::ClearViaSafeSave => {
            run_clear_via_safe_save_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::ClearBeforeQuiescence => {
            run_clear_before_quiescence_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::PartitionBothDaemons => {
            run_partition_both_daemons_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::FlappingConnectivity => {
            run_flapping_connectivity_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::AsymmetricPartition => {
            run_asymmetric_partition_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::PartitionDuringProjection => {
            run_partition_during_projection_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::MessageHoldAndRelease => {
            run_message_hold_and_release_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::TransientHighPacketLoss => {
            run_transient_high_packet_loss_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::SustainedEditingStress => {
            run_sustained_editing_stress_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::DeleteWhileOffline => {
            run_delete_while_offline_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::BinaryFileIgnore => {
            run_binary_file_ignore_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::PermissionDeniedIgnore => {
            run_permission_denied_ignore_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::TooLargeIgnore => {
            run_too_large_ignore_workload(&mut sim, seed, args.common.max_steps)?
        }
        Workload::WrongCipherClone => {
            run_wrong_cipher_clone_workload(&mut sim, seed, args.common.max_steps)?
        }
    }

    sim.run().map_err(|err| eyre!("simulation failed: {err}"))?;
    Ok(())
}

fn run_meta(args: MetaArgs) -> eyre::Result<()> {
    let seed = args.common.seed.unwrap_or_else(random_seed);
    let exe = std::env::current_exe()?;
    let options = ChildRunOptions {
        quiet: false,
        rust_log: Some(args.child_proc_log_level.clone()),
    };
    let comparison = run_meta_child_comparison(&exe, &args.common, seed, &options)?;

    if let Some(mismatch) = comparison.stdout.first_mismatch {
        eyre::bail!(
            "meta stdout mismatch for seed {seed} at line {}\n--- first ---\n{}\n--- second ---\n{}",
            mismatch.line,
            mismatch.first_preview,
            mismatch.second_preview,
        );
    }
    if let Some(mismatch) = comparison.stderr.first_mismatch {
        eyre::bail!(
            "meta stderr mismatch for seed {seed} at line {}\n--- first ---\n{}\n--- second ---\n{}",
            mismatch.line,
            mismatch.first_preview,
            mismatch.second_preview,
        );
    }
    if comparison.first_status != comparison.second_status {
        eyre::bail!(
            "meta child status mismatch for seed {seed}: first={} second={}",
            comparison.first_status,
            comparison.second_status,
        );
    }
    if !comparison.first_success {
        eyre::bail!(
            "meta child failed for seed {seed}: status={}",
            comparison.first_status,
        );
    }

    let line_count = comparison.stdout.line_count + comparison.stderr.line_count;

    println!(
        "META_OK workload={} seed={seed} lines={line_count} rust_log={}",
        args.common.workload.as_str(),
        args.child_proc_log_level,
    );
    Ok(())
}

struct MetaChildComparison {
    first_status: String,
    first_success: bool,
    second_status: String,
    stdout: StreamComparison,
    stderr: StreamComparison,
}

struct StreamComparison {
    first_mismatch: Option<StreamMismatch>,
    line_count: usize,
}

struct StreamMismatch {
    line: usize,
    first_preview: String,
    second_preview: String,
}

#[derive(Clone)]
struct MetaOutputNormalizer {
    first_dir: Vec<u8>,
    second_dir: Vec<u8>,
}

impl MetaOutputNormalizer {
    fn new(first_dir: &Path, second_dir: &Path) -> eyre::Result<Self> {
        Ok(Self {
            first_dir: std::fs::canonicalize(first_dir)?
                .to_string_lossy()
                .as_bytes()
                .to_vec(),
            second_dir: std::fs::canonicalize(second_dir)?
                .to_string_lossy()
                .as_bytes()
                .to_vec(),
        })
    }

    fn normalize_line(&self, line: &[u8]) -> Vec<u8> {
        let line = normalize_turso_temp_paths(line);
        let line = replace_all(&line, &self.first_dir, b"<META_CHILD_DIR>");
        replace_all(&line, &self.second_dir, b"<META_CHILD_DIR>")
    }
}

fn run_meta_child_comparison(
    exe: &Path,
    args: &CommonRunArgs,
    seed: u64,
    options: &ChildRunOptions,
) -> eyre::Result<MetaChildComparison> {
    let temp_dir = create_meta_temp_dir(seed)?;
    let result = (|| {
        let first_dir = temp_dir.join("first");
        let second_dir = temp_dir.join("second");
        std::fs::create_dir(&first_dir)?;
        std::fs::create_dir(&second_dir)?;
        let normalizer = MetaOutputNormalizer::new(&first_dir, &second_dir)?;
        let mut first = spawn_single_child(exe, args, seed, options, &first_dir)?;
        let mut second = spawn_single_child(exe, args, seed, options, &second_dir)?;

        let first_stdout = take_child_pipe(first.stdout.take(), "first stdout")?;
        let second_stdout = take_child_pipe(second.stdout.take(), "second stdout")?;
        let first_stderr = take_child_pipe(first.stderr.take(), "first stderr")?;
        let second_stderr = take_child_pipe(second.stderr.take(), "second stderr")?;

        let stdout_normalizer = normalizer.clone();
        let stderr_normalizer = normalizer;
        let stdout_thread = std::thread::spawn(move || {
            compare_streams(first_stdout, second_stdout, stdout_normalizer)
        });
        let stderr_thread = std::thread::spawn(move || {
            compare_streams(first_stderr, second_stderr, stderr_normalizer)
        });

        let first_status = first.wait()?;
        let second_status = second.wait()?;
        let stdout = stdout_thread
            .join()
            .map_err(|_| eyre!("stdout comparison thread panicked"))??;
        let stderr = stderr_thread
            .join()
            .map_err(|_| eyre!("stderr comparison thread panicked"))??;

        Ok(MetaChildComparison {
            first_status: first_status.to_string(),
            first_success: first_status.success(),
            second_status: second_status.to_string(),
            stdout,
            stderr,
        })
    })();
    let _ = std::fs::remove_dir_all(&temp_dir);
    result
}

fn create_meta_temp_dir(seed: u64) -> eyre::Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "opbox-sim-meta-{}-{seed}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir(&path)?;
    Ok(path)
}

fn spawn_single_child(
    exe: &Path,
    args: &CommonRunArgs,
    seed: u64,
    options: &ChildRunOptions,
    current_dir: &Path,
) -> eyre::Result<Child> {
    let mut command = single_child_command(exe, args, seed, options);
    command.current_dir(current_dir);
    Ok(command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?)
}

fn take_child_pipe<T>(pipe: Option<T>, label: &'static str) -> eyre::Result<T> {
    pipe.ok_or_else(|| eyre!("missing child pipe: {label}"))
}

fn compare_streams<L: Read, R: Read>(
    left: L,
    right: R,
    normalizer: MetaOutputNormalizer,
) -> std::io::Result<StreamComparison> {
    let mut left = BufReader::with_capacity(64 * 1024, left);
    let mut right = BufReader::with_capacity(64 * 1024, right);
    let mut line_count = 0;
    let mut first_mismatch = None;
    let mut left_line = Vec::new();
    let mut right_line = Vec::new();

    loop {
        left_line.clear();
        right_line.clear();
        let left_read = left.read_until(b'\n', &mut left_line)?;
        let right_read = right.read_until(b'\n', &mut right_line)?;
        if left_read == 0 && right_read == 0 {
            break;
        }

        if first_mismatch.is_none() {
            let left_normalized = normalizer.normalize_line(&left_line);
            let right_normalized = normalizer.normalize_line(&right_line);
            if left_normalized == right_normalized {
                if left_read > 0 {
                    line_count += 1;
                }
                continue;
            }
            first_mismatch = Some(StreamMismatch {
                line: line_count + 1,
                first_preview: preview_lossy(&left_normalized, 512),
                second_preview: preview_lossy(&right_normalized, 512),
            });
        }

        if left_read > 0 {
            line_count += 1;
        }
    }

    Ok(StreamComparison {
        first_mismatch,
        line_count,
    })
}

fn replace_all(input: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    if needle.is_empty() {
        return input.to_vec();
    }

    let mut out = Vec::with_capacity(input.len());
    let mut remaining = input;
    while let Some(idx) = remaining
        .windows(needle.len())
        .position(|window| window == needle)
    {
        out.extend_from_slice(&remaining[..idx]);
        out.extend_from_slice(replacement);
        remaining = &remaining[idx + needle.len()..];
    }
    out.extend_from_slice(remaining);
    out
}

fn normalize_turso_temp_paths(input: &[u8]) -> Vec<u8> {
    let marker = b"/tursodb-temp.db";
    let temp_segment = b"/.tmp";
    let replacement = b"/<TURSO_TEMP_DIR>";
    let mut out = Vec::with_capacity(input.len());
    let mut cursor = 0;

    while let Some(marker_idx) = find_bytes(&input[cursor..], marker).map(|idx| cursor + idx) {
        let Some(temp_idx) =
            rfind_bytes(&input[cursor..marker_idx], temp_segment).map(|idx| cursor + idx)
        else {
            out.extend_from_slice(&input[cursor..marker_idx + marker.len()]);
            cursor = marker_idx + marker.len();
            continue;
        };

        out.extend_from_slice(&input[cursor..temp_idx]);
        out.extend_from_slice(replacement);
        out.extend_from_slice(marker);
        cursor = marker_idx + marker.len();
    }

    out.extend_from_slice(&input[cursor..]);
    out
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn rfind_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(haystack.len());
    }
    haystack
        .windows(needle.len())
        .rposition(|window| window == needle)
}

fn preview_lossy(bytes: &[u8], max_len: usize) -> String {
    let mut preview = bytes[..bytes.len().min(max_len)].to_vec();
    if bytes.len() > max_len {
        preview.extend_from_slice(b"...<truncated>");
    }
    String::from_utf8_lossy(&preview).into_owned()
}

fn single_child_command(
    exe: &Path,
    args: &CommonRunArgs,
    seed: u64,
    options: &ChildRunOptions,
) -> ProcessCommand {
    let mut command = ProcessCommand::new(exe);
    command
        .arg("single")
        .arg(args.workload.as_str())
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--max-steps")
        .arg(args.max_steps.to_string())
        .arg("--failure-rate")
        .arg(args.failure_rate.to_string());
    if options.quiet {
        command.arg("--quiet");
    }
    if let Some(rust_log) = &options.rust_log {
        command.env("RUST_LOG", rust_log);
    }
    command
}

fn run_parallel(args: ParallelArgs) -> eyre::Result<()> {
    let seed_start = args.common.seed.unwrap_or_else(random_seed);
    let jobs = args
        .jobs
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
        })
        .max(1);
    let trials = args.trials;
    if let Some(output_dir) = &args.output_dir {
        std::fs::create_dir_all(output_dir)?;
    }
    if trials == 0 {
        println!(
            "PARALLEL_OK workload={} seed_start={seed_start} trials=0 jobs={jobs}",
            args.common.workload.as_str(),
        );
        return Ok(());
    }

    let exe = std::env::current_exe()?;
    let child_options = parallel_child_run_options(&args);
    let (work_tx, work_rx) = std::sync::mpsc::channel::<u64>();
    let work_rx = std::sync::Arc::new(std::sync::Mutex::new(work_rx));
    let (result_tx, result_rx) = std::sync::mpsc::channel::<SeedRunResult>();

    for _ in 0..jobs {
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        let common = args.common.clone();
        let exe = exe.clone();
        let child_options = child_options.clone();
        std::thread::spawn(move || {
            loop {
                let seed = {
                    let work_rx = work_rx.lock().expect("parallel work queue poisoned");
                    work_rx.recv()
                };
                let Ok(seed) = seed else {
                    return;
                };

                let result = run_single_child_with_exe(&exe, &common, seed, &child_options);
                if result_tx.send(result).is_err() {
                    return;
                }
            }
        });
    }
    drop(result_tx);

    for offset in 0..trials {
        let seed = seed_start.checked_add(offset).ok_or_else(|| {
            eyre!("parallel seed range overflow: start={seed_start}, offset={offset}")
        })?;
        work_tx.send(seed)?;
    }
    drop(work_tx);

    let mut completed = 0;
    let mut first_failure = None;
    for result in result_rx {
        completed += 1;
        match result {
            SeedRunResult::Pass { output } => {
                if let Some(output_dir) = &args.output_dir {
                    write_seed_run_output(output_dir, &output)?;
                }
            }
            SeedRunResult::Fail(failure) => {
                if let Some(output_dir) = &args.output_dir {
                    write_seed_run_output(output_dir, &failure.output)?;
                }
                first_failure = Some(failure);
                break;
            }
        }
    }

    if let Some(failure) = first_failure {
        eyre::bail!(
            "parallel sim failed after {completed}/{trials} completed seeds\n{}",
            failure.render()
        );
    }

    println!(
        "PARALLEL_OK workload={} seed_start={seed_start} trials={trials} jobs={jobs}",
        args.common.workload.as_str(),
    );
    Ok(())
}

fn run_sweep(args: SweepArgs) -> eyre::Result<()> {
    let workloads = if args.workloads.is_empty() {
        Workload::value_variants().to_vec()
    } else {
        args.workloads.clone()
    };

    let mut failures = Vec::new();
    for &workload in &workloads {
        let result = run_parallel(ParallelArgs {
            common: CommonRunArgs {
                workload,
                seed: args.seed,
                max_steps: args.max_steps,
                failure_rate: args.failure_rate,
            },
            trials: args.trials,
            jobs: args.jobs,
            output_dir: args
                .output_dir
                .as_ref()
                .map(|dir| dir.join(workload.as_str())),
            child_proc_log_level: args.child_proc_log_level.clone(),
        });
        if let Err(error) = result {
            println!("SWEEP_WORKLOAD_FAILED workload={}", workload.as_str());
            failures.push((workload, error));
        }
    }

    if !failures.is_empty() {
        let details = failures
            .iter()
            .map(|(workload, error)| format!("--- {} ---\n{error}", workload.as_str()))
            .collect::<Vec<_>>()
            .join("\n");
        eyre::bail!(
            "sweep failed for {}/{} workloads\n{details}",
            failures.len(),
            workloads.len(),
        );
    }

    println!(
        "SWEEP_OK workloads={} trials={}",
        workloads.len(),
        args.trials,
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct ChildRunOptions {
    quiet: bool,
    rust_log: Option<String>,
}

impl ChildRunOptions {
    fn quiet_error() -> Self {
        Self {
            quiet: true,
            rust_log: Some("error".to_string()),
        }
    }
}

fn parallel_child_run_options(args: &ParallelArgs) -> ChildRunOptions {
    if let Some(rust_log) = &args.child_proc_log_level {
        return ChildRunOptions {
            quiet: false,
            rust_log: Some(rust_log.clone()),
        };
    }

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        return ChildRunOptions {
            quiet: false,
            rust_log: Some(rust_log),
        };
    }

    ChildRunOptions::quiet_error()
}

fn run_single_child_with_exe(
    exe: &Path,
    args: &CommonRunArgs,
    seed: u64,
    options: &ChildRunOptions,
) -> SeedRunResult {
    let mut command = ProcessCommand::new(exe);
    command
        .arg("single")
        .arg(args.workload.as_str())
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--max-steps")
        .arg(args.max_steps.to_string())
        .arg("--failure-rate")
        .arg(args.failure_rate.to_string());
    if options.quiet {
        command.arg("--quiet");
    }
    if let Some(rust_log) = &options.rust_log {
        command.env("RUST_LOG", rust_log);
    }

    let output = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let output = match output {
        Ok(output) => output,
        Err(err) => {
            return SeedRunResult::Fail(SeedRunFailure {
                output: SeedRunOutput {
                    seed,
                    status: "spawn failed".to_string(),
                    stdout: String::new(),
                    stderr: err.to_string(),
                    normalized_output: String::new(),
                },
            });
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let success = output.status.success();
    let status = output.status.to_string();
    let stdout = stdout.into_owned();
    let stderr = stderr.into_owned();
    let normalized_output = normalize_child_output(&stdout, &stderr);
    let output = SeedRunOutput {
        seed,
        status,
        stdout,
        stderr,
        normalized_output,
    };
    if !success {
        return SeedRunResult::Fail(SeedRunFailure { output });
    }

    SeedRunResult::Pass { output }
}

enum SeedRunResult {
    Pass { output: SeedRunOutput },
    Fail(SeedRunFailure),
}

struct SeedRunOutput {
    seed: u64,
    status: String,
    stdout: String,
    stderr: String,
    normalized_output: String,
}

struct SeedRunFailure {
    output: SeedRunOutput,
}

impl SeedRunFailure {
    fn render(&self) -> String {
        format!(
            "seed={} status={}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            self.output.seed, self.output.status, self.output.stdout, self.output.stderr
        )
    }
}

fn write_seed_run_output(output_dir: &Path, output: &SeedRunOutput) -> eyre::Result<()> {
    let path = output_dir.join(format!("seed-{}.log", output.seed));
    let body = format!(
        "seed={}\nstatus={}\nnormalized_output={}\n--- stdout ---\n{}\n--- stderr ---\n{}\n",
        output.seed, output.status, output.normalized_output, output.stdout, output.stderr
    );
    std::fs::write(path, body)?;
    Ok(())
}

fn normalize_child_output(stdout: &str, stderr: &str) -> String {
    stdout
        .lines()
        .chain(stderr.lines())
        .filter(|line| line.starts_with("SIM_"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn run_general_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000001".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        daemon_a
            .write_file("a.txt", Bytes::from_static(b"from daemon a\n"))
            .await?;
        daemon_b
            .write_file("b.txt", Bytes::from_static(b"from daemon b\n"))
            .await?;

        let expected = BTreeMap::from([
            ("a.txt".to_string(), "from daemon a\n".to_string()),
            ("b.txt".to_string(), "from daemon b\n".to_string()),
        ]);

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            if last_a == expected && last_b == expected {
                println!(
                    "SIM_OK workload=general seed={seed} files={}",
                    expected.len()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "local trees did not converge; a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn run_scoped_scan_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000009".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "scoped.txt";

        daemon_a
            .write_file(path, Bytes::from_static(b"scoped\n"))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), "scoped\n".to_string())]),
            steps,
        )
        .await?;
        let original_object_id =
            wait_for_matching_prior_object_id(&daemon_a, &daemon_b, path, steps).await?;

        daemon_a
            .write_file(path, Bytes::from_static(b"scoped\nmore\n"))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), "scoped\nmore\n".to_string())]),
            steps,
        )
        .await?;
        wait_for_preserved_prior_object_id(&daemon_a, &daemon_b, path, &original_object_id, steps)
            .await?;

        daemon_a.delete_file(path).await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_text_files(&daemon_a, &daemon_b, &BTreeMap::new(), steps).await?;

        println!("SIM_OK workload=scoped-scan seed={seed}");
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_clear_existing_file_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("0000000000000000000000000000000d".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "hello.txt";
        let initial = "whats good\nneat\nhi there\nyo\nyo\n";

        daemon_b
            .write_file(path, Bytes::from(initial.to_string()))
            .await?;
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), initial.to_string())]),
            steps,
        )
        .await?;

        daemon_b
            .replace_file_contents(path, Bytes::from_static(b""))
            .await?;

        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), String::new())]),
            steps,
        )
        .await?;

        println!("SIM_OK workload=clear-existing-file seed={seed}");
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_clear_before_quiescence_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("0000000000000000000000000000000f".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "hello.txt";
        let initial = "whats good\nneat\nhi there\nyo\nyo\n";

        daemon_b
            .write_file(path, Bytes::from(initial.to_string()))
            .await?;
        tokio::time::sleep(Duration::from_millis(deterministic_delay_ms(
            seed,
            0xC1EA_0000_0000_0001,
            0,
        )))
        .await;
        daemon_b
            .replace_file_contents(path, Bytes::from_static(b""))
            .await?;

        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), String::new())]),
            steps,
        )
        .await?;

        println!("SIM_OK workload=clear-before-quiescence seed={seed}");
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_binary_file_ignore_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000014".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "data.bin";

        // Write a binary file (invalid UTF-8) from daemon B.
        let binary_content: Vec<u8> = vec![0xFF, 0xFE, 0x00, 0x01, 0x80, 0x81, 0xAB, 0xCD];
        daemon_b
            .write_file(path, Bytes::from(binary_content))
            .await?;

        // Allow several scan cycles to run. The daemon should NOT enter an
        // infinite retry loop — the file should land in the ignored_files table.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // Verify the binary file is tracked as ignored on daemon B.
        let debug_b = daemon_b.semantic_debug_snapshot().await?;
        assert!(
            debug_b.ignored_files_count >= 1,
            "expected ignored_files_count >= 1 on daemon B, got {}",
            debug_b.ignored_files_count
        );

        // The binary file should NOT appear in daemon A's text snapshot (it was
        // never successfully imported, so it was never synced).
        let files_a = daemon_a.snapshot_utf8_text_files().await?;
        assert!(
            !files_a.contains_key(path),
            "binary file should not sync to daemon A"
        );

        // Now overwrite the binary file with valid UTF-8 text content.
        let text_content = format!("hello from seed {seed}\n");
        daemon_b
            .replace_file_contents(path, Bytes::from(text_content.clone()))
            .await?;

        // The fingerprint changed, so the daemon should retry the import and
        // succeed. Both daemons should converge on the text content.
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), text_content)]),
            steps,
        )
        .await?;

        println!("SIM_OK workload=binary-file-ignore seed={seed}");
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_permission_denied_ignore_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000015".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "secret.txt";
        let content = format!("readable content seed={seed}\n");

        // Inject a permission-denied fault BEFORE writing so the
        // guarded_read triggered by import will fail.
        daemon_b.inject_guarded_read_permission_denied(path).await?;

        // Write a readable text file from daemon B.
        daemon_b
            .write_file(path, Bytes::from(content.clone()))
            .await?;

        // Explicitly request a scan so the engine discovers the file
        // regardless of whether the startup scan already completed.
        daemon_b.request_full_scan().await?;

        // Allow scan cycles to process.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // The file should be tracked as ignored on daemon B.
        let debug_b = daemon_b.semantic_debug_snapshot().await?;
        assert!(
            debug_b.ignored_files_count >= 1,
            "expected ignored_files_count >= 1 on daemon B after permission denied, got {}",
            debug_b.ignored_files_count
        );

        // The file should NOT appear on daemon A (never successfully imported).
        let files_a = daemon_a.snapshot_utf8_text_files().await?;
        assert!(
            !files_a.contains_key(path),
            "permission-denied file should not sync to daemon A"
        );

        // Now "restore permissions" by not injecting any more faults and
        // requesting another scan. Since PermissionDenied entries are always
        // re-attempted (permission changes don't affect stat fingerprint),
        // the daemon should retry, succeed, and remove from ignore table.
        daemon_b.request_full_scan().await?;

        // Both daemons should converge on the text content.
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), content)]),
            steps,
        )
        .await?;

        // After convergence, ignored_files should be cleared.
        let debug_b = daemon_b.semantic_debug_snapshot().await?;
        assert_eq!(
            debug_b.ignored_files_count, 0,
            "expected ignored_files_count == 0 after permission restored, got {}",
            debug_b.ignored_files_count
        );

        println!("SIM_OK workload=permission-denied-ignore seed={seed}");
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_too_large_ignore_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000016".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "huge.txt";

        // 20 MiB + 1 byte — just over the limit.
        let over_limit_size = 20 * 1024 * 1024 + 1;
        let large_content = Bytes::from(vec![b'x'; over_limit_size]);
        daemon_b.write_file(path, large_content).await?;

        // Explicitly request a scan so the engine discovers the file
        // regardless of whether the startup scan already completed.
        daemon_b.request_full_scan().await?;

        // Allow scan cycles to process.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // The file should be tracked as ignored on daemon B.
        let debug_b = daemon_b.semantic_debug_snapshot().await?;
        assert!(
            debug_b.ignored_files_count >= 1,
            "expected ignored_files_count >= 1 on daemon B after too-large file, got {}",
            debug_b.ignored_files_count
        );

        // The file should NOT appear on daemon A.
        let files_a = daemon_a.snapshot_utf8_text_files().await?;
        assert!(
            !files_a.contains_key(path),
            "too-large file should not sync to daemon A"
        );

        // Now shrink the file below the limit. The stat fingerprint (size)
        // changes, so the daemon should re-attempt and succeed.
        let small_content = format!("shrunk content seed={seed}\n");
        daemon_b
            .replace_file_contents(path, Bytes::from(small_content.clone()))
            .await?;

        // Both daemons should converge on the small content.
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), small_content)]),
            steps,
        )
        .await?;

        // After convergence, ignored_files should be cleared.
        let debug_b = daemon_b.semantic_debug_snapshot().await?;
        assert_eq!(
            debug_b.ignored_files_count, 0,
            "expected ignored_files_count == 0 after file shrunk, got {}",
            debug_b.ignored_files_count
        );

        println!("SIM_OK workload=too-large-ignore seed={seed}");
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_projection_storm_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000002".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let file_count = 24;
        let mut expected = BTreeMap::new();

        for idx in 0..file_count {
            let path = format!("storm-{idx:02}.txt");
            let content = format!("storm file {idx}\n");
            daemon_a
                .write_file(path.clone(), Bytes::from(content.clone()))
                .await?;
            expected.insert(path, content);

            // Let periodic scans capture several small independent local updates
            // instead of one giant initial import. This creates multiple remote
            // projection-change events on daemon B.
            tokio::time::sleep(Duration::from_millis(125)).await;
        }

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            if last_a == expected && last_b == expected {
                println!(
                    "SIM_OK workload=projection-storm seed={seed} files={}",
                    expected.len()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "local trees did not converge; a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn run_same_path_create_conflict_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000004".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "collide.txt";
        let daemon_a_content = "from daemon a\n";
        let daemon_b_content = "from daemon b\n";

        let write_a = daemon_a.write_file(path, Bytes::from_static(b"from daemon a\n"));
        let write_b = daemon_b.write_file(path, Bytes::from_static(b"from daemon b\n"));
        let (a_result, b_result) = tokio::join!(write_a, write_b);
        a_result?;
        b_result?;

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;

            if last_a == last_b
                && same_path_create_conflict_converged(
                    &last_a,
                    path,
                    daemon_a_content,
                    daemon_b_content,
                )
            {
                let conflict_path = last_a
                    .keys()
                    .find(|candidate| candidate.as_str() != path)
                    .expect("converged state has one conflict path");
                println!(
                    "SIM_OK workload=same-path-create-conflict seed={seed} winner={path} conflict_path={conflict_path:?}"
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "same-path create conflict did not converge; a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn same_path_create_conflict_converged(
    files: &BTreeMap<String, String>,
    requested_path: &str,
    winner_content: &str,
    loser_content: &str,
) -> bool {
    same_path_create_conflict_path(files, requested_path, winner_content, loser_content).is_some()
}

fn same_path_create_conflict_path(
    files: &BTreeMap<String, String>,
    requested_path: &str,
    winner_content: &str,
    loser_content: &str,
) -> Option<String> {
    if files.len() != 2 || files.get(requested_path).map(String::as_str) != Some(winner_content) {
        return None;
    }

    let Some((conflict_path, conflict_content)) = files
        .iter()
        .find(|(candidate, _)| candidate.as_str() != requested_path)
    else {
        return None;
    };

    if conflict_path_matches_requested(conflict_path, requested_path)
        && conflict_content == loser_content
    {
        Some(conflict_path.clone())
    } else {
        None
    }
}

fn conflict_path_matches_requested(candidate: &str, requested_path: &str) -> bool {
    let (dir, file_name) = match requested_path.rsplit_once('/') {
        Some((dir, file_name)) => (Some(dir), file_name),
        None => (None, requested_path),
    };
    let (stem, ext) = match file_name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => (stem, format!(".{ext}")),
        _ => (file_name, String::new()),
    };
    let prefix = match dir {
        Some(dir) => format!("{dir}/{stem} (conflict "),
        None => format!("{stem} (conflict "),
    };
    candidate.starts_with(&prefix) && candidate.ends_with(&format!("){ext}"))
}

fn run_conflict_plus_later_edit_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000007".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "collide.txt";
        let daemon_a_content = "from daemon a\n";
        let daemon_b_content = "from daemon b\n";

        let write_a = daemon_a.write_file(path, Bytes::from_static(b"from daemon a\n"));
        let write_b = daemon_b.write_file(path, Bytes::from_static(b"from daemon b\n"));
        let (a_result, b_result) = tokio::join!(write_a, write_b);
        a_result?;
        b_result?;

        let mut conflict_path = None;
        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            if last_a == last_b {
                conflict_path = same_path_create_conflict_path(
                    &last_a,
                    path,
                    daemon_a_content,
                    daemon_b_content,
                );
                if conflict_path.is_some() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let Some(conflict_path) = conflict_path else {
            daemon_a.shutdown().await;
            daemon_b.shutdown().await;
            return Err(io_err(format!(
                "conflict-plus-later-edit did not reach initial conflict; a={last_a:?} b={last_b:?}"
            )));
        };

        let winner_edit = format!("winner cross-edit seed {seed}\n");
        let conflict_edit = format!("conflict cross-edit seed {seed}\n");
        let edit_winner_delay_ms = deterministic_delay_ms(seed, 0xC011_1DE0_0000_0001, 0);
        let edit_conflict_delay_ms = deterministic_delay_ms(seed, 0xC011_1DE0_0000_0002, 0);
        let edit_winner = {
            let winner_edit = winner_edit.clone();
            async {
                tokio::time::sleep(Duration::from_millis(edit_winner_delay_ms)).await;
                daemon_b.append_file(path, Bytes::from(winner_edit)).await
            }
        };
        let edit_conflict = {
            let conflict_path = conflict_path.clone();
            let conflict_edit = conflict_edit.clone();
            async {
                tokio::time::sleep(Duration::from_millis(edit_conflict_delay_ms)).await;
                daemon_a
                    .append_file(conflict_path, Bytes::from(conflict_edit))
                    .await
            }
        };
        let (winner_result, conflict_result) = tokio::join!(edit_winner, edit_conflict);
        winner_result?;
        conflict_result?;

        let expected = BTreeMap::from([
            (
                path.to_string(),
                format!("{daemon_a_content}{winner_edit}"),
            ),
            (
                conflict_path.clone(),
                format!("{daemon_b_content}{conflict_edit}"),
            ),
        ]);

        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            if last_a == expected && last_b == expected {
                println!(
                    "SIM_OK workload=conflict-plus-later-edit seed={seed} conflict_path={conflict_path:?} edit_winner_delay_ms={edit_winner_delay_ms} edit_conflict_delay_ms={edit_conflict_delay_ms}"
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "conflict-plus-later-edit did not converge after edits; conflict_path={conflict_path:?} expected={expected:?} a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn run_many_file_conflict_storm_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000005".to_string());
    let (overlap_count, unique_count) = many_file_conflict_storm_dimensions(seed);
    let daemon_a = spawn_daemon_with_initial_files(
        sim,
        "daemon-a",
        0,
        workspace_id.clone(),
        many_file_conflict_storm_initial_files("a", overlap_count, unique_count),
    )?;
    let daemon_b = spawn_daemon_with_initial_files(
        sim,
        "daemon-b",
        1,
        workspace_id,
        many_file_conflict_storm_initial_files("b", overlap_count, unique_count),
    )?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;

            if last_a == last_b
                && many_file_conflict_storm_converged(&last_a, overlap_count, unique_count)
            {
                println!(
                    "SIM_OK workload=many-file-conflict-storm seed={seed} overlap_count={overlap_count} unique_count={unique_count} files={}",
                    last_a.len()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "many-file conflict storm did not converge; overlap_count={overlap_count} unique_count={unique_count} a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn many_file_conflict_storm_dimensions(seed: u64) -> (usize, usize) {
    let overlap_count = 12 + (seed as usize % 21);
    let unique_count = 6 + ((seed.rotate_left(17)) as usize % 13);
    (overlap_count, unique_count)
}

fn many_file_conflict_storm_initial_files(
    side: &'static str,
    overlap_count: usize,
    unique_count: usize,
) -> BTreeMap<String, Bytes> {
    let mut files = BTreeMap::new();
    for idx in 0..overlap_count {
        let path = format!("overlap-{idx:02}.txt");
        let content = format!("overlap {idx:02} from daemon {side}\n");
        files.insert(path, Bytes::from(content));
    }

    for idx in 0..unique_count {
        let path = format!("{side}-only-{idx:02}.txt");
        let content = format!("unique {idx:02} from daemon {side}\n");
        files.insert(path, Bytes::from(content));
    }

    files
}

fn many_file_conflict_storm_converged(
    files: &BTreeMap<String, String>,
    overlap_count: usize,
    unique_count: usize,
) -> bool {
    let expected_file_count = overlap_count * 2 + unique_count * 2;
    if files.len() != expected_file_count {
        return false;
    }

    for idx in 0..unique_count {
        let a_path = format!("a-only-{idx:02}.txt");
        let b_path = format!("b-only-{idx:02}.txt");
        let a_content = format!("unique {idx:02} from daemon a\n");
        let b_content = format!("unique {idx:02} from daemon b\n");
        if files.get(&a_path) != Some(&a_content) || files.get(&b_path) != Some(&b_content) {
            return false;
        }
    }

    for idx in 0..overlap_count {
        let requested_path = format!("overlap-{idx:02}.txt");
        let winner_content = format!("overlap {idx:02} from daemon a\n");
        let loser_content = format!("overlap {idx:02} from daemon b\n");
        if files.get(&requested_path) != Some(&winner_content) {
            return false;
        }

        let conflict_prefix = format!("overlap-{idx:02} (conflict ");
        let conflict_candidates: Vec<_> = files
            .iter()
            .filter(|(path, _)| path.starts_with(&conflict_prefix) && path.ends_with(").txt"))
            .collect();
        if conflict_candidates.len() != 1 || conflict_candidates[0].1 != &loser_content {
            return false;
        }
    }

    true
}

fn run_delete_vs_edit_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000006".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "target.txt";
        let base = "base\n";
        daemon_a
            .write_file(path, Bytes::from_static(b"base\n"))
            .await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, base, steps).await?;

        let delete_delay_ms = deterministic_delay_ms(seed, 0xD311_E7E0_0000_0001, 0);
        let edit_delay_ms = deterministic_delay_ms(seed, 0xED17_0000_0000_0002, 0);
        let deleter = async {
            tokio::time::sleep(Duration::from_millis(delete_delay_ms)).await;
            daemon_a.delete_file(path).await
        };
        let editor = async {
            tokio::time::sleep(Duration::from_millis(edit_delay_ms)).await;
            daemon_b
                .append_file(path, Bytes::from_static(b"from daemon b\n"))
                .await
        };
        let (delete_result, edit_result) = tokio::join!(deleter, editor);
        delete_result?;
        edit_result?;

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;

            if last_a == last_b
                && let Some(resolution) = delete_vs_edit_resolution(&last_a, path)
            {
                println!(
                    "SIM_OK workload=delete-vs-edit seed={seed} resolution={resolution} delete_delay_ms={delete_delay_ms} edit_delay_ms={edit_delay_ms} files={}",
                    last_a.len()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "delete-vs-edit did not converge; delete_delay_ms={delete_delay_ms} edit_delay_ms={edit_delay_ms} a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn delete_vs_edit_resolution(
    files: &BTreeMap<String, String>,
    requested_path: &str,
) -> Option<&'static str> {
    if files.is_empty() {
        return Some("delete_won");
    }

    if files.len() != 1 {
        return None;
    }

    let (path, content) = files.iter().next()?;
    if path == requested_path && content == "base\nfrom daemon b\n" {
        return Some("edit_preserved");
    }
    if path == requested_path && content == "from daemon b\n" {
        return Some("recreated_after_delete");
    }
    if path.starts_with("target (conflict ")
        && path.ends_with(").txt")
        && content.contains("from daemon b\n")
    {
        return Some("edit_conflicted");
    }

    None
}

fn run_safe_save_after_quiescence_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000008".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "a.txt";
        let temp_path = ".a.txt.tmp";
        let base = "base\n";
        let final_content = format!("base\nsafe-save seed {seed}\n");

        daemon_a
            .write_file(path, Bytes::from_static(b"base\n"))
            .await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, base, steps).await?;
        let original_object_id =
            wait_for_matching_prior_object_id(&daemon_a, &daemon_b, path, steps).await?;

        let pause_after_temp_ms = deterministic_safe_save_pause_ms(seed, 0);
        let pause_after_delete_ms = deterministic_safe_save_pause_ms(seed, 1);
        let pause_after_rewrite_ms = deterministic_safe_save_pause_ms(seed, 2);

        daemon_a
            .write_file(temp_path, Bytes::from(final_content.clone()))
            .await?;
        tokio::time::sleep(Duration::from_millis(pause_after_temp_ms)).await;

        daemon_a.delete_file(path).await?;
        tokio::time::sleep(Duration::from_millis(pause_after_delete_ms)).await;

        daemon_a
            .write_file(path, Bytes::from(final_content.clone()))
            .await?;
        tokio::time::sleep(Duration::from_millis(pause_after_rewrite_ms)).await;

        daemon_a.delete_file(temp_path).await?;

        let expected = BTreeMap::from([(path.to_string(), final_content)]);
        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        let mut last_debug_a = None;
        let mut last_debug_b = None;
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            let debug_a = daemon_a.semantic_debug_snapshot().await?;
            let debug_b = daemon_b.semantic_debug_snapshot().await?;
            let object_identity_preserved =
                debug_a.prior_live_paths.get(path) == Some(&original_object_id)
                    && debug_b.prior_live_paths.get(path) == Some(&original_object_id);

            if last_a == expected && last_b == expected && object_identity_preserved {
                println!(
                    "SIM_OK workload=safe-save-after-quiescence seed={seed} object_id={} pause_after_temp_ms={pause_after_temp_ms} pause_after_delete_ms={pause_after_delete_ms} pause_after_rewrite_ms={pause_after_rewrite_ms}",
                    original_object_id.encode_b64()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            last_debug_a = Some(debug_a);
            last_debug_b = Some(debug_b);

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "safe-save-after-quiescence did not converge with preserved object identity; expected={expected:?} original_object_id={} a={last_a:?} b={last_b:?} debug_a={last_debug_a:?} debug_b={last_debug_b:?}",
            original_object_id.encode_b64()
        )))
    });

    Ok(())
}

fn run_clear_via_safe_save_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("0000000000000000000000000000000e".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "hello.txt";
        let temp_path = ".hello.txt.tmp";
        let initial = "whats good\nneat\nhi there\nyo\nyo\n";

        daemon_b
            .write_file(path, Bytes::from(initial.to_string()))
            .await?;
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(path.to_string(), initial.to_string())]),
            steps,
        )
        .await?;
        let original_object_id =
            wait_for_matching_prior_object_id(&daemon_a, &daemon_b, path, steps).await?;

        let pause_after_temp_ms = deterministic_safe_save_pause_ms(seed, 0);
        let pause_after_delete_ms = deterministic_safe_save_pause_ms(seed, 1);
        let pause_after_rewrite_ms = deterministic_safe_save_pause_ms(seed, 2);

        daemon_b
            .write_file(temp_path, Bytes::from_static(b""))
            .await?;
        daemon_b.request_single_file_scan(temp_path).await?;
        tokio::time::sleep(Duration::from_millis(pause_after_temp_ms)).await;

        daemon_b.delete_file(path).await?;
        daemon_b.request_single_file_scan(path).await?;
        tokio::time::sleep(Duration::from_millis(pause_after_delete_ms)).await;

        daemon_b.write_file(path, Bytes::from_static(b"")).await?;
        daemon_b.request_single_file_scan(path).await?;
        tokio::time::sleep(Duration::from_millis(pause_after_rewrite_ms)).await;

        daemon_b.delete_file(temp_path).await?;
        daemon_b.request_single_file_scan(temp_path).await?;

        let expected = BTreeMap::from([(path.to_string(), String::new())]);
        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        let mut last_debug_a = None;
        let mut last_debug_b = None;
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            let debug_a = daemon_a.semantic_debug_snapshot().await?;
            let debug_b = daemon_b.semantic_debug_snapshot().await?;
            let object_identity_preserved =
                debug_a.prior_live_paths.get(path) == Some(&original_object_id)
                    && debug_b.prior_live_paths.get(path) == Some(&original_object_id);

            if last_a == expected && last_b == expected && object_identity_preserved {
                println!(
                    "SIM_OK workload=clear-via-safe-save seed={seed} object_id={} pause_after_temp_ms={pause_after_temp_ms} pause_after_delete_ms={pause_after_delete_ms} pause_after_rewrite_ms={pause_after_rewrite_ms}",
                    original_object_id.encode_b64()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            last_debug_a = Some(debug_a);
            last_debug_b = Some(debug_b);

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "clear-via-safe-save did not converge with preserved object identity; expected={expected:?} original_object_id={} a={last_a:?} b={last_b:?} debug_a={last_debug_a:?} debug_b={last_debug_b:?}",
            original_object_id.encode_b64()
        )))
    });

    Ok(())
}

fn deterministic_safe_save_pause_ms(seed: u64, stage: u64) -> u64 {
    let mixed = seed
        .wrapping_mul(0xD6E8_FD9D_82EF_2D47)
        .wrapping_add(stage.wrapping_mul(0xA076_1D64_78BD_642F));
    25 + (mixed % 900)
}

fn run_rename_after_quiescence_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("0000000000000000000000000000000b".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let from = "sgb.txt";
        let to = "sgb2.txt";
        let content = format!("rename seed {seed}\n");

        daemon_a
            .write_file(from, Bytes::from(content.clone()))
            .await?;
        wait_for_text_files(
            &daemon_a,
            &daemon_b,
            &BTreeMap::from([(from.to_string(), content.clone())]),
            steps,
        )
        .await?;
        let original_object_id =
            wait_for_matching_prior_object_id(&daemon_a, &daemon_b, from, steps).await?;

        daemon_a.rename_file(from, to).await?;
        daemon_a.request_full_scan().await?;

        let expected = BTreeMap::from([(to.to_string(), content)]);
        wait_for_text_files(&daemon_a, &daemon_b, &expected, steps).await?;
        wait_for_preserved_prior_object_id(&daemon_a, &daemon_b, to, &original_object_id, steps)
            .await?;

        println!(
            "SIM_OK workload=rename-after-quiescence seed={seed} object_id={}",
            original_object_id.encode_b64()
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_same_file_edits_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
    pattern: SameFileEditPattern,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000003".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);
    let workload_name = pattern.workload_name();

    sim.client("controller", async move {
        let path = "a.txt";
        let base = "base\n";
        daemon_a.write_file(path, Bytes::from_static(b"base\n")).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, base, steps).await?;

        let edits_per_daemon = deterministic_edits_per_daemon(seed, pattern);
        let mut expected_markers = Vec::new();
        for idx in 0..edits_per_daemon {
            expected_markers.push(format!("A{idx:02}-seed-{seed}\n"));
            expected_markers.push(format!("B{idx:02}-seed-{seed}\n"));
        }

        let writer_a = same_file_writer(
            daemon_a.clone(),
            path,
            "A",
            seed,
            0xA11C_E55E_D17_u64,
            edits_per_daemon,
            pattern.daemon_a_edit(),
        );
        let writer_b = same_file_writer(
            daemon_b.clone(),
            path,
            "B",
            seed,
            0xB00C_E55E_D17_u64,
            edits_per_daemon,
            pattern.daemon_b_edit(),
        );
        let (a_result, b_result) = tokio::join!(writer_a, writer_b);
        a_result?;
        b_result?;

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            let a_text = last_a.get(path).map(String::as_str);
            let b_text = last_b.get(path).map(String::as_str);

            if let (Some(a_text), Some(b_text)) = (a_text, b_text) && a_text == b_text {
                let missing = missing_markers(Some(a_text), &expected_markers);
                let stats = combined_stats(&daemon_a, &daemon_b).await?;
                if missing.is_empty() || stats.guarded_write_conflict_after_swap_count > 0 {
                    let marker_status = if missing.is_empty() {
                        "complete"
                    } else {
                        "loss-after-swap-conflict"
                    };
                    println!(
                        "SIM_OK workload={workload_name} seed={seed} markers={} marker_status={marker_status} write_conflict_before_swap={} write_conflict_after_swap={}",
                        expected_markers.len(),
                        stats.guarded_write_conflict_before_swap_count,
                        stats.guarded_write_conflict_after_swap_count
                    );
                    println!("FINAL TEXT\n{a_text}END FINAL TEXT");
                    daemon_a.shutdown().await;
                    daemon_b.shutdown().await;
                    return Ok(());
                }
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let stats = combined_stats(&daemon_a, &daemon_b).await?;
        let missing_a = missing_markers(last_a.get(path).map(String::as_str), &expected_markers);
        let missing_b = missing_markers(last_b.get(path).map(String::as_str), &expected_markers);
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "{workload_name} did not converge; conflict_after_swap_count={}; missing_a={missing_a:?} missing_b={missing_b:?} a={last_a:?} b={last_b:?}",
            stats.guarded_write_conflict_after_swap_count
        )))
    });

    Ok(())
}

fn run_large_text_multipart_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000010".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let s2_workspace_id = workspace_id.clone();
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "multipart.txt";
        let inline_threshold = opbox_core::log::codec::max_inline_record_size();
        let initial = deterministic_large_text(seed, "initial", inline_threshold * 3 + 257);
        let append = deterministic_large_text(seed, "append", inline_threshold * 2 + 257);
        let final_text = format!("{initial}{append}");
        let expected_min_object_records = initial.len().div_ceil(inline_threshold)
            + append.len().div_ceil(inline_threshold);

        daemon_a
            .write_file(path, Bytes::from(initial.clone()))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &initial, steps).await?;
        let object_id = wait_for_matching_prior_object_id(&daemon_a, &daemon_b, path, steps)
            .await?;

        daemon_b
            .append_file(path, Bytes::from(append.clone()))
            .await?;
        daemon_b.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &final_text, steps).await?;
        wait_for_preserved_prior_object_id(&daemon_a, &daemon_b, path, &object_id, steps)
            .await?;
        let object_stats = multipart_object_stream_stats(&s2_workspace_id).await?;
        if object_stats.stream_count < 2
            || object_stats.record_count < expected_min_object_records as u64
        {
            daemon_a.shutdown().await;
            daemon_b.shutdown().await;
            return Err(io_err(format!(
                "large-text-multipart did not create expected multipart object streams; expected_min_streams=2 expected_min_object_records={expected_min_object_records} actual={object_stats:?}"
            )));
        }

        println!(
            "SIM_OK workload=large-text-multipart seed={seed} inline_threshold={inline_threshold} initial_bytes={} append_bytes={} final_bytes={} object_streams={} object_records={} object_id={}",
            initial.len(),
            append.len(),
            final_text.len(),
            object_stats.stream_count,
            object_stats.record_count,
            object_id.encode_b64()
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_offline_reconnect_merge_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000011".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "shared.txt";
        let initial = format!("shared paragraph seed {seed}\n\n");
        daemon_a
            .write_file(path, Bytes::from(initial.clone()))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &initial, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        turmoil::partition("daemon-b", "s2-lite");
        tokio::time::sleep(Duration::from_millis(250)).await;

        let b_markers = (0..4)
            .map(|idx| format!("B-offline-{idx}-seed-{seed}"))
            .collect::<Vec<_>>();
        for (idx, marker) in b_markers.iter().enumerate() {
            daemon_b
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_b.request_single_file_scan(path).await?;
            wait_for_outbox_at_least(&daemon_b, idx as u64 + 1, steps).await?;
        }
        wait_for_writer_not_online_with_released_outbox(
            &daemon_b,
            u64::try_from(b_markers.len()).expect("marker count fits u64"),
            steps,
        )
        .await?;

        let a_markers = (0..4)
            .map(|idx| format!("A-online-{idx}-seed-{seed}"))
            .collect::<Vec<_>>();
        for marker in &a_markers {
            daemon_a
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_a.request_single_file_scan(path).await?;
        }

        let pre_repair_a = snapshot_file_text(&daemon_a, path).await?;
        let pre_repair_b = snapshot_file_text(&daemon_b, path).await?;
        let pre_repair_b_debug = daemon_b.semantic_debug_snapshot().await?;
        print_document_snapshot("before-repair", "daemon-a", path, &pre_repair_a);
        print_document_snapshot("before-repair", "daemon-b", path, &pre_repair_b);
        println!(
            "DOC_SNAPSHOT_META workload=offline-reconnect-merge seed={seed} moment=before-repair daemon=daemon-b outbox_rows={} outbox_inflight_rows={}",
            pre_repair_b_debug.outbox_rows,
            pre_repair_b_debug.outbox_inflight_rows
        );

        turmoil::repair("daemon-b", "s2-lite");
        wait_for_connectivity_online(&daemon_b, steps).await?;

        let expected_markers = a_markers
            .iter()
            .chain(b_markers.iter())
            .cloned()
            .collect::<Vec<_>>();
        let final_text =
            wait_for_shared_text_with_markers(&daemon_a, &daemon_b, path, &expected_markers, steps)
                .await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;
        print_document_snapshot("after-merge", "both", path, &final_text);

        println!(
            "SIM_OK workload=offline-reconnect-merge seed={seed} markers={} final_bytes={}",
            expected_markers.len(),
            final_text.len()
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn deterministic_large_text(seed: u64, label: &str, min_bytes: usize) -> String {
    let mut text = format!("{label} seed {seed}\n");
    let mut idx = 0usize;
    while text.len() < min_bytes {
        text.push_str(&format!(
            "{label}-{idx:04}-seed-{seed}-abcdefghijklmnopqrstuvwxyz0123456789\n"
        ));
        idx += 1;
    }
    text
}

#[derive(Debug, Clone, Copy, Default)]
struct MultipartObjectStreamStats {
    stream_count: usize,
    record_count: u64,
}

async fn multipart_object_stream_stats(
    workspace_id: &WorkspaceId,
) -> Result<MultipartObjectStreamStats, Box<dyn std::error::Error>> {
    let basin_name: BasinName = SIM_BASIN.parse().map_err(io_err)?;
    let basin = sim_s2_client().map_err(io_err)?.basin(basin_name);
    let prefix = format!("{}/objects/", workspace_id.0)
        .parse()
        .map_err(io_err)?;
    let page = basin
        .list_streams(ListStreamsInput::new().with_prefix(prefix).with_limit(1000))
        .await
        .map_err(io_err)?;
    if page.has_more {
        return Err(io_err(
            "large-text-multipart produced more than 1000 object streams",
        ));
    }

    let mut record_count = 0u64;
    for stream in &page.values {
        let tail = basin
            .stream(stream.name.clone())
            .check_tail()
            .await
            .map_err(io_err)?;
        record_count += tail.seq_num;
    }

    Ok(MultipartObjectStreamStats {
        stream_count: page.values.len(),
        record_count,
    })
}

#[derive(Clone, Copy)]
enum SameFileEditPattern {
    BothAppend,
    PrependAAppendB,
}

impl SameFileEditPattern {
    fn workload_name(self) -> &'static str {
        match self {
            Self::BothAppend => "same-file-edits",
            Self::PrependAAppendB => "same-file-split-edits",
        }
    }

    fn daemon_a_edit(self) -> SameFileEditKind {
        match self {
            Self::BothAppend => SameFileEditKind::Append,
            Self::PrependAAppendB => SameFileEditKind::Prepend,
        }
    }

    fn daemon_b_edit(self) -> SameFileEditKind {
        SameFileEditKind::Append
    }
}

#[derive(Clone, Copy)]
enum SameFileEditKind {
    Append,
    Prepend,
}

async fn combined_stats(
    daemon_a: &SimDaemonHandle,
    daemon_b: &SimDaemonHandle,
) -> Result<InMemoryFileIOStats, Box<dyn std::error::Error>> {
    let stats_a = daemon_a.stats().await?;
    let stats_b = daemon_b.stats().await?;
    Ok(stats_a.combine(stats_b))
}

async fn wait_for_file_text(
    daemon_a: &SimDaemonHandle,
    daemon_b: &SimDaemonHandle,
    path: &str,
    expected: &str,
    steps: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..steps {
        let a = daemon_a.snapshot_text_files().await?;
        let b = daemon_b.snapshot_text_files().await?;
        if a.get(path).map(String::as_str) == Some(expected)
            && b.get(path).map(String::as_str) == Some(expected)
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(io_err(format!(
        "file {path:?} did not converge to bootstrap text"
    )))
}

async fn wait_for_text_files(
    daemon_a: &SimDaemonHandle,
    daemon_b: &SimDaemonHandle,
    expected: &BTreeMap<String, String>,
    steps: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_a = BTreeMap::new();
    let mut last_b = BTreeMap::new();
    for _ in 0..steps {
        last_a = daemon_a.snapshot_text_files().await?;
        last_b = daemon_b.snapshot_text_files().await?;
        if &last_a == expected && &last_b == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(io_err(format!(
        "text files did not converge; expected={expected:?} a={last_a:?} b={last_b:?}"
    )))
}

async fn snapshot_file_text(
    daemon: &SimDaemonHandle,
    path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    daemon
        .snapshot_text_files()
        .await?
        .get(path)
        .cloned()
        .ok_or_else(|| io_err(format!("missing snapshot path {path:?}")))
}

fn print_document_snapshot(moment: &str, daemon: &str, path: &str, text: &str) {
    println!(
        "DOC_SNAPSHOT_BEGIN workload=offline-reconnect-merge moment={moment} daemon={daemon} path={path:?} bytes={}",
        text.len()
    );
    print!("{text}");
    if !text.ends_with('\n') {
        println!();
    }
    println!(
        "DOC_SNAPSHOT_END workload=offline-reconnect-merge moment={moment} daemon={daemon} path={path:?}"
    );
}

async fn wait_for_outbox_empty(
    daemon: &SimDaemonHandle,
    steps: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_debug = None;
    for _ in 0..steps {
        let debug = daemon.semantic_debug_snapshot().await?;
        if debug.outbox_rows == 0 && debug.outbox_inflight_rows == 0 {
            return Ok(());
        }
        last_debug = Some(debug);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(io_err(format!(
        "outbox did not drain; last_debug={last_debug:?}"
    )))
}

async fn wait_for_outbox_at_least(
    daemon: &SimDaemonHandle,
    expected_rows: u64,
    steps: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_debug = None;
    for _ in 0..steps {
        let debug = daemon.semantic_debug_snapshot().await?;
        if debug.outbox_rows >= expected_rows {
            return Ok(());
        }
        last_debug = Some(debug);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(io_err(format!(
        "outbox did not accumulate expected rows; expected_rows={expected_rows} last_debug={last_debug:?}"
    )))
}

async fn wait_for_writer_not_online_with_released_outbox(
    daemon: &SimDaemonHandle,
    expected_rows: u64,
    steps: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_status = None;
    let mut last_debug = None;
    for _ in 0..steps {
        let status = daemon.connectivity_status().await?;
        let debug = daemon.semantic_debug_snapshot().await?;
        if status.writer.state != LinkState::Online
            && debug.outbox_rows >= expected_rows
            && debug.outbox_inflight_rows == 0
        {
            return Ok(());
        }
        last_status = Some(status);
        last_debug = Some(debug);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(io_err(format!(
        "daemon writer did not enter offline/reconnecting with released outbox; expected_rows={expected_rows} last_status={last_status:?} last_debug={last_debug:?}"
    )))
}

async fn wait_for_connectivity_online(
    daemon: &SimDaemonHandle,
    steps: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_status = None;
    for _ in 0..steps {
        let status = daemon.connectivity_status().await?;
        if status.reader.state == LinkState::Online && status.writer.state == LinkState::Online {
            return Ok(());
        }
        last_status = Some(status);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(io_err(format!(
        "daemon connectivity did not become online; last_status={last_status:?}"
    )))
}

async fn wait_for_shared_text_with_markers(
    daemon_a: &SimDaemonHandle,
    daemon_b: &SimDaemonHandle,
    path: &str,
    expected_markers: &[String],
    steps: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut last_a = BTreeMap::new();
    let mut last_b = BTreeMap::new();
    for _ in 0..steps {
        last_a = daemon_a.snapshot_text_files().await?;
        last_b = daemon_b.snapshot_text_files().await?;
        if let (Some(a_text), Some(b_text)) = (last_a.get(path), last_b.get(path))
            && a_text == b_text
            && missing_markers(Some(a_text), expected_markers).is_empty()
        {
            return Ok(a_text.clone());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let missing_a = missing_markers(last_a.get(path).map(String::as_str), expected_markers);
    let missing_b = missing_markers(last_b.get(path).map(String::as_str), expected_markers);
    Err(io_err(format!(
        "shared text did not converge with expected markers; missing_a={missing_a:?} missing_b={missing_b:?} a={last_a:?} b={last_b:?}"
    )))
}

async fn wait_for_matching_prior_object_id(
    daemon_a: &SimDaemonHandle,
    daemon_b: &SimDaemonHandle,
    path: &str,
    steps: u64,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let mut last_debug_a = None;
    let mut last_debug_b = None;
    for _ in 0..steps {
        let debug_a = daemon_a.semantic_debug_snapshot().await?;
        let debug_b = daemon_b.semantic_debug_snapshot().await?;
        match (
            debug_a.prior_live_paths.get(path),
            debug_b.prior_live_paths.get(path),
        ) {
            (Some(object_id_a), Some(object_id_b)) if object_id_a == object_id_b => {
                return Ok(object_id_a.clone());
            }
            _ => {
                last_debug_a = Some(debug_a);
                last_debug_b = Some(debug_b);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }

    Err(io_err(format!(
        "timed out waiting for matching prior object id at path={path:?}; debug_a={last_debug_a:?} debug_b={last_debug_b:?}"
    )))
}

async fn wait_for_preserved_prior_object_id(
    daemon_a: &SimDaemonHandle,
    daemon_b: &SimDaemonHandle,
    path: &str,
    expected_object_id: &ObjectId,
    steps: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut last_debug_a = None;
    let mut last_debug_b = None;
    for _ in 0..steps {
        let debug_a = daemon_a.semantic_debug_snapshot().await?;
        let debug_b = daemon_b.semantic_debug_snapshot().await?;
        if debug_a.prior_live_paths.get(path) == Some(expected_object_id)
            && debug_b.prior_live_paths.get(path) == Some(expected_object_id)
        {
            return Ok(());
        }

        last_debug_a = Some(debug_a);
        last_debug_b = Some(debug_b);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    Err(io_err(format!(
        "timed out waiting for preserved prior object id at path={path:?}; expected={} debug_a={last_debug_a:?} debug_b={last_debug_b:?}",
        expected_object_id.encode_b64()
    )))
}

const ORPHAN_FILE_COUNT: usize = 8;
const ORPHAN_ROUNDS: u64 = 4;
const ORPHAN_EDITS_PER_ROUND: u64 = 24;
/// Lead time before the dense local-edit spray. B's burst reaches A roughly
/// 230-260ms after B's scan (B→s2 200ms + s2→A 30ms + writer linger); the
/// spray must straddle that arrival so some edits land while A's engine is
/// busy scanning/importing/projecting the burst.
const ORPHAN_SPRAY_LEAD_MS: u64 = 180;

fn orphan_file_name(index: usize) -> String {
    format!("f{index:02}.txt")
}

fn orphan_victim_index(seed: u64, round: u64, edit: u64) -> usize {
    let mixed = seed
        .wrapping_mul(0xD6E8_FEB8_6659_FD93)
        .wrapping_add(round.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(edit.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    (mixed % ORPHAN_FILE_COUNT as u64) as usize
}

/// Every expected marker must appear in its file exactly once. Containment
/// checks alone cannot catch text duplication, which converges identically on
/// all replicas (e.g. a daemon re-importing its own orphaned projection write
/// as a user edit).
fn marker_count_violations(
    files: &BTreeMap<String, String>,
    expected: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let mut violations = Vec::new();
    for (path, markers) in expected {
        let text = files.get(path).map(String::as_str).unwrap_or("");
        for marker in markers {
            let count = text.matches(marker.as_str()).count();
            if count != 1 {
                violations.push(format!("{path}: {marker:?} appears {count} times"));
            }
        }
    }
    violations
}

/// Targets the orphaned-projection-write window: daemon B bursts edits to
/// every file (a wide projection epoch on daemon A) while the controller races
/// local edits on A. A local edit landing between A's plan and the victim
/// file's guarded write invalidates the epoch after sibling writes already
/// succeeded; A must then recognize those sibling files as its own writes
/// (write-intent journal) instead of re-importing them as user edits, which
/// would duplicate B's text on every replica.
fn run_orphaned_projection_write_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    max_steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000004".to_string());
    let mut initial_files = BTreeMap::new();
    for index in 0..ORPHAN_FILE_COUNT {
        initial_files.insert(
            orphan_file_name(index),
            Bytes::from(format!("base f{index:02}\n")),
        );
    }
    let daemon_a =
        spawn_daemon_with_initial_files(sim, "daemon-a", 0, workspace_id.clone(), initial_files)?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let base = (0..ORPHAN_FILE_COUNT)
            .map(|index| (orphan_file_name(index), format!("base f{index:02}\n")))
            .collect::<BTreeMap<_, _>>();
        wait_for_text_files(&daemon_a, &daemon_b, &base, max_steps).await?;

        let mut expected: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for round in 0..ORPHAN_ROUNDS {
            for index in 0..ORPHAN_FILE_COUNT {
                let marker = format!("B-r{round}-f{index:02}-seed{seed}\n");
                daemon_b
                    .append_file(orphan_file_name(index), Bytes::from(marker.clone()))
                    .await?;
                expected.entry(orphan_file_name(index)).or_default().push(marker);
            }
            daemon_b.request_full_scan().await?;

            tokio::time::sleep(Duration::from_millis(ORPHAN_SPRAY_LEAD_MS)).await;
            for edit in 0..ORPHAN_EDITS_PER_ROUND {
                let mixed = seed
                    .wrapping_mul(0x9D5C_0FFE_E000_0003)
                    .wrapping_add(round.wrapping_mul(0x9E37_79B9_7F4A_7C15))
                    .wrapping_add(edit.wrapping_mul(0xBF58_476D_1CE4_E5B9));
                tokio::time::sleep(Duration::from_millis(3 + mixed % 10)).await;
                let victim = orphan_victim_index(seed, round, edit);
                let path = orphan_file_name(victim);
                let marker = format!("A-r{round}-e{edit}-f{victim:02}-seed{seed}\n");
                daemon_a
                    .append_file(path.clone(), Bytes::from(marker.clone()))
                    .await?;
                daemon_a.request_single_file_scan(&path).await?;
                expected.entry(path).or_default().push(marker);
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }

        let mut last_violations = Vec::new();
        let mut converged = false;
        for _ in 0..max_steps {
            let a = daemon_a.snapshot_text_files().await?;
            let b = daemon_b.snapshot_text_files().await?;
            if a == b {
                converged = true;
                last_violations = marker_count_violations(&a, &expected);
                if last_violations.is_empty() {
                    let stats = combined_stats(&daemon_a, &daemon_b).await?;
                    let marker_count: usize = expected.values().map(Vec::len).sum();
                    println!(
                        "SIM_OK workload=orphaned-projection-write seed={seed} files={} markers={marker_count} write_conflict_before_swap={} write_conflict_after_swap={}",
                        ORPHAN_FILE_COUNT,
                        stats.guarded_write_conflict_before_swap_count,
                        stats.guarded_write_conflict_after_swap_count,
                    );
                    daemon_a.shutdown().await;
                    daemon_b.shutdown().await;
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "orphaned-projection-write did not pass; converged={converged} violations={last_violations:?}"
        )))
    });

    Ok(())
}

fn run_fault_read_changed_between_stats_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000010".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "racy-read.txt";
        let replacement = format!("replacement after read race seed {seed}\n");
        daemon_a
            .inject_guarded_read_changed_between_stats(path, Bytes::from(replacement.clone()))
            .await?;
        daemon_a
            .write_file(path, Bytes::from_static(b"initial scan evidence\n"))
            .await?;
        daemon_a.request_single_file_scan(path).await?;

        let expected = BTreeMap::from([(path.to_string(), replacement)]);
        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        let mut last_stats = InMemoryFileIOStats::default();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            last_stats = combined_stats(&daemon_a, &daemon_b).await?;
            if last_a == expected
                && last_b == expected
                && last_stats.guarded_read_changed_between_stats_count > 0
            {
                println!(
                    "SIM_OK workload=fault-read-changed-between-stats seed={seed} changed_between_stats={}",
                    last_stats.guarded_read_changed_between_stats_count
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "fault-read-changed-between-stats did not converge; expected={expected:?} a={last_a:?} b={last_b:?} stats={last_stats:?}"
        )))
    });

    Ok(())
}

fn run_fault_projection_conflict_before_swap_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000011".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "fault-before.txt";
        let local_content = format!("local before swap seed {seed}\n");
        let remote_content = format!("remote projection seed {seed}\n");
        daemon_a
            .inject_guarded_write_conflict_before_swap(path, Bytes::from(local_content.clone()))
            .await?;
        daemon_b
            .write_file(path, Bytes::from(remote_content.clone()))
            .await?;
        daemon_b.request_single_file_scan(path).await?;

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        let mut last_stats = InMemoryFileIOStats::default();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            last_stats = combined_stats(&daemon_a, &daemon_b).await?;

            if last_a == last_b
                && same_path_create_conflict_path(
                    &last_a,
                    path,
                    &local_content,
                    &remote_content,
                )
                .is_some()
                && last_stats.guarded_write_conflict_before_swap_count > 0
            {
                println!(
                    "SIM_OK workload=fault-projection-conflict-before-swap seed={seed} write_conflict_before_swap={}",
                    last_stats.guarded_write_conflict_before_swap_count
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "fault-projection-conflict-before-swap did not converge; a={last_a:?} b={last_b:?} stats={last_stats:?}"
        )))
    });

    Ok(())
}

fn run_fault_projection_conflict_after_swap_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000012".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "fault-after.txt";
        let local_content = format!("local after swap seed {seed}\n");
        let remote_content = format!("remote projection seed {seed}\n");
        daemon_a
            .inject_guarded_write_conflict_after_swap(path, Bytes::from(local_content.clone()))
            .await?;
        daemon_b
            .write_file(path, Bytes::from(remote_content.clone()))
            .await?;
        daemon_b.request_single_file_scan(path).await?;

        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        let mut last_stats = InMemoryFileIOStats::default();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            last_stats = combined_stats(&daemon_a, &daemon_b).await?;

            if last_a == last_b
                && same_path_create_conflict_path(
                    &last_a,
                    path,
                    &local_content,
                    &remote_content,
                )
                .is_some()
                && last_stats.guarded_write_conflict_after_swap_count > 0
            {
                println!(
                    "SIM_OK workload=fault-projection-conflict-after-swap seed={seed} write_conflict_after_swap={}",
                    last_stats.guarded_write_conflict_after_swap_count
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "fault-projection-conflict-after-swap did not converge; a={last_a:?} b={last_b:?} stats={last_stats:?}"
        )))
    });

    Ok(())
}

fn run_fault_projection_temp_leak_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000013".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "leaky.txt";
        let temp_path = ".leaky.txt.opbox-tmp-deadbeef";
        let remote_content = format!("remote after temp leak seed {seed}\n");
        let temp_content = format!("orphaned temp seed {seed}\n");
        daemon_a
            .inject_guarded_write_temp_leak_and_fail(
                path,
                temp_path,
                Bytes::from(temp_content.clone()),
            )
            .await?;
        daemon_b
            .write_file(path, Bytes::from(remote_content.clone()))
            .await?;
        daemon_b.request_single_file_scan(path).await?;

        let expected_b = BTreeMap::from([(path.to_string(), remote_content.clone())]);
        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        let mut last_debug_a = None;
        let mut last_debug_b = None;
        let mut last_stats = InMemoryFileIOStats::default();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            let debug_a = daemon_a.semantic_debug_snapshot().await?;
            let debug_b = daemon_b.semantic_debug_snapshot().await?;
            last_stats = combined_stats(&daemon_a, &daemon_b).await?;

            let temp_is_local_only = last_a.get(temp_path).map(String::as_str)
                == Some(temp_content.as_str())
                && !last_b.contains_key(temp_path)
                && !debug_a.stable_paths.contains_key(temp_path)
                && !debug_b.stable_paths.contains_key(temp_path);
            if last_a.get(path).map(String::as_str) == Some(remote_content.as_str())
                && last_b == expected_b
                && temp_is_local_only
                && last_stats.guarded_write_conflict_count > 0
            {
                println!(
                    "SIM_OK workload=fault-projection-temp-leak seed={seed} temp_path={temp_path:?} write_failures={}",
                    last_stats.guarded_write_conflict_count
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            last_debug_a = Some(debug_a);
            last_debug_b = Some(debug_b);

            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "fault-projection-temp-leak did not converge; a={last_a:?} b={last_b:?} debug_a={last_debug_a:?} debug_b={last_debug_b:?} stats={last_stats:?}"
        )))
    });

    Ok(())
}

async fn same_file_writer(
    daemon: SimDaemonHandle,
    path: &'static str,
    writer: &'static str,
    seed: u64,
    salt: u64,
    edits: u64,
    edit_kind: SameFileEditKind,
) -> Result<(), Box<dyn std::error::Error>> {
    for idx in 0..edits {
        let delay_ms = deterministic_delay_ms(seed, salt, idx);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;

        let marker = format!("{writer}{idx:02}-seed-{seed}\n");
        match edit_kind {
            SameFileEditKind::Append => daemon.append_file(path, Bytes::from(marker)).await?,
            SameFileEditKind::Prepend => daemon.prepend_file(path, Bytes::from(marker)).await?,
        }
    }

    Ok(())
}

fn deterministic_delay_ms(seed: u64, salt: u64, idx: u64) -> u64 {
    let mixed = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(salt)
        .wrapping_add(idx.wrapping_mul(0xBF58_476D_1CE4_E5B9));
    40 + (mixed % 180)
}

fn deterministic_edits_per_daemon(seed: u64, pattern: SameFileEditPattern) -> u64 {
    let pattern_salt = match pattern {
        SameFileEditPattern::BothAppend => 0xED17_5A11_E000_0001,
        SameFileEditPattern::PrependAAppendB => 0x5A17_ED17_0000_0002,
    };
    let span = SAME_FILE_MAX_EDITS_PER_DAEMON - SAME_FILE_MIN_EDITS_PER_DAEMON + 1;
    let mixed = seed
        .wrapping_mul(0x94D0_49BB_1331_11EB)
        .wrapping_add(pattern_salt);
    SAME_FILE_MIN_EDITS_PER_DAEMON + (mixed % span)
}

fn missing_markers(text: Option<&str>, expected_markers: &[String]) -> Vec<String> {
    let text = text.unwrap_or("");
    expected_markers
        .iter()
        .filter(|marker| !text.contains(marker.as_str()))
        .cloned()
        .collect()
}

fn configure_daemon_s2_link_latencies(sim: &turmoil::Sim<'static>) {
    sim.set_link_latency(
        "daemon-a",
        "s2-lite",
        Duration::from_millis(DAEMON_A_S2_LINK_LATENCY_MS),
    );
    sim.set_link_latency(
        "daemon-b",
        "s2-lite",
        Duration::from_millis(DAEMON_B_S2_LINK_LATENCY_MS),
    );
}

fn spawn_daemon(
    sim: &mut turmoil::Sim<'static>,
    name: &'static str,
    daemon_index: u8,
    workspace_id: WorkspaceId,
) -> eyre::Result<SimDaemonHandle> {
    spawn_daemon_with_initial_files(sim, name, daemon_index, workspace_id, BTreeMap::new())
}

fn spawn_daemon_with_initial_files(
    sim: &mut turmoil::Sim<'static>,
    name: &'static str,
    daemon_index: u8,
    workspace_id: WorkspaceId,
    initial_files: BTreeMap<String, Bytes>,
) -> eyre::Result<SimDaemonHandle> {
    spawn_daemon_with_config(
        sim,
        name,
        daemon_index,
        workspace_id,
        initial_files,
        sim_encryption_key(),
    )
}

fn sim_encryption_key() -> Option<EncryptionKey> {
    Some(EncryptionKey::new([0x42u8; 32]))
}

fn spawn_daemon_with_config(
    sim: &mut turmoil::Sim<'static>,
    name: &'static str,
    daemon_index: u8,
    workspace_id: WorkspaceId,
    initial_files: BTreeMap<String, Bytes>,
    encryption_key: Option<EncryptionKey>,
) -> eyre::Result<SimDaemonHandle> {
    let daemon_row = daemon_state::Row {
        workspace_id,
        s2_basin: SIM_BASIN.parse()?,
        s2_account_endpoint: None,
        s2_basin_endpoint: None,
        daemon_writer_id: DaemonWriterId(Bytes::copy_from_slice(&[daemon_index; 16])),
        stable_cursor: ..0,
        next_outbox_id: OutboxId::new(0),
        encryption_key,
    };

    let file_io = FaultInjectingFileIO::new(InMemoryFileIO::new());
    for (path, bytes) in initial_files {
        file_io.write_file(path, bytes)?;
    }
    let handle_io = file_io.clone();
    let (command_tx, command_rx) = mpsc::channel(100);
    sim.client(name, async move {
        run_daemon_client(name, daemon_row, file_io, command_rx).await
    });

    Ok(SimDaemonHandle {
        command_tx,
        _io: handle_io,
    })
}

async fn run_daemon_client(
    name: &'static str,
    daemon_row: daemon_state::Row,
    file_io: FaultInjectingFileIO,
    mut command_rx: mpsc::Receiver<SimDaemonCommand>,
) -> Result<(), Box<dyn std::error::Error>> {
    ensure_basin_exists().await?;
    let db = open_memory_database().await.map_err(io_err)?;
    initialize_database(&db, &daemon_row)
        .await
        .map_err(io_err)?;
    let pool = semantic_pool(db.clone()).await.map_err(io_err)?;
    let semantic_service = SemanticService::new(pool);
    let debug_semantic_service = semantic_service.clone();
    let s2_basin = sim_s2_client()
        .map_err(io_err)?
        .basin(daemon_row.s2_basin.clone());
    let (notify_io, notify_handle) = channel_notify_io();
    let engine_status = EngineStatusConfig {
        sync_root: PathBuf::from(format!("/sim/{name}")),
        workspace_id: daemon_row.workspace_id.clone(),
        basin: daemon_row.s2_basin.clone(),
        daemon_writer_id: daemon_row.daemon_writer_id.clone(),
        stable_cursor: daemon_row.stable_cursor.clone(),
        started_at: time::OffsetDateTime::now_utc(),
        warnings: Vec::new(),
    };
    let cancellation_token = CancellationToken::new();
    let runtime = AppRuntime::new(AppRuntimeConfig {
        mode: RunMode::Sync,
        file_io: file_io.clone(),
        notify_io: Some(notify_io),
        semantic_service,
        daemon_row,
        s2_basin,
        clone_log_read_stop: None,
        engine_status: Some(engine_status),
        spy_tx: None,
    });
    let mut actors = runtime.spawn(cancellation_token.clone());
    let engine_tx = actors
        .engine_command_tx()
        .ok_or_else(|| io_err("sync runtime did not expose engine command mailbox"))?;

    loop {
        tokio::select! {
            actor_error = actors.wait_for_actor_stop() => {
                let error = actor_error.unwrap_or_else(|| eyre!("all actors exited"));
                return Err(io_err(error));
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    warn!(name, "daemon command channel closed");
                    if let Some(error) = actors.shutdown(cancellation_token).await {
                        return Err(io_err(error));
                    }
                    return Ok(());
                };

                match command {
                    SimDaemonCommand::WriteFile { path, bytes, reply } => {
                        let _ = reply.send(file_io.write_file(path, bytes).map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::AppendFile { path, bytes, reply } => {
                        let _ = reply.send(file_io.append_file(path, bytes).map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::ReplaceFileContents { path, bytes, reply } => {
                        let _ = reply.send(file_io.replace_file_contents(path, bytes).map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::PrependFile { path, bytes, reply } => {
                        let _ = reply.send(file_io.prepend_file(path, bytes).map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::DeleteFile { path, reply } => {
                        let _ = reply.send(file_io.delete_file(path).map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::RenameFile { from, to, reply } => {
                        let _ = reply.send(file_io.rename_file(from, to).map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::SnapshotTextFiles { reply } => {
                        let _ = reply.send(file_io.snapshot_text_files().map_err(|err| err.to_string()));
                    }
                    SimDaemonCommand::SnapshotUtf8TextFiles { reply } => {
                        let _ = reply.send(file_io.snapshot_utf8_text_files());
                    }
                    SimDaemonCommand::Stats { reply } => {
                        let _ = reply.send(file_io.stats());
                    }
                    SimDaemonCommand::SemanticDebugSnapshot { reply } => {
                        let result = debug_semantic_service
                            .debug_snapshot()
                            .await
                            .map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::ConnectivityStatus { reply } => {
                        let (status_reply, status_rx) = oneshot::channel();
                        let result = match engine_tx.send(EngineCommand::Status {
                            reply: status_reply,
                        }) {
                            Ok(()) => status_rx
                                .await
                                .map_err(|err| err.to_string())
                                .map(|status| status.connectivity),
                            Err(err) => Err(err.to_string()),
                        };
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::RequestScan { scope, reply } => {
                        let result = notify_handle.send_scope(scope).map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::InjectGuardedReadPermissionDenied { path, reply } => {
                        let result = file_io
                            .inject_guarded_read_permission_denied(path)
                            .map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::InjectGuardedReadChangedBetweenStats { path, replacement, reply } => {
                        let result = file_io
                            .inject_guarded_read_changed_between_stats(path, replacement)
                            .map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::InjectGuardedWriteConflictBeforeSwap { path, replacement, reply } => {
                        let result = file_io
                            .inject_guarded_write_conflict_before_swap(path, replacement)
                            .map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::InjectGuardedWriteConflictAfterSwap { path, replacement, reply } => {
                        let result = file_io
                            .inject_guarded_write_conflict_after_swap(path, replacement)
                            .map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::InjectGuardedWriteTempLeakAndFail { path, temp_path, bytes, reply } => {
                        let result = file_io
                            .inject_guarded_write_temp_leak_and_fail(path, temp_path, bytes)
                            .map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::InjectDeleteIfExistsFailure { path, reply } => {
                        let result = file_io
                            .inject_delete_if_exists_failure(path)
                            .map_err(|err| err.to_string());
                        let _ = reply.send(result);
                    }
                    SimDaemonCommand::Shutdown { reply } => {
                        let result = actors.shutdown(cancellation_token).await;
                        let _ = reply.send(result.map(|err| err.to_string()));
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct SimDaemonHandle {
    command_tx: mpsc::Sender<SimDaemonCommand>,
    _io: FaultInjectingFileIO,
}

impl SimDaemonHandle {
    async fn write_file(
        &self,
        path: impl Into<String>,
        bytes: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::WriteFile {
                path: path.into(),
                bytes,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn append_file(
        &self,
        path: impl Into<String>,
        bytes: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::AppendFile {
                path: path.into(),
                bytes,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn replace_file_contents(
        &self,
        path: impl Into<String>,
        bytes: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::ReplaceFileContents {
                path: path.into(),
                bytes,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn prepend_file(
        &self,
        path: impl Into<String>,
        bytes: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::PrependFile {
                path: path.into(),
                bytes,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn delete_file(&self, path: impl Into<String>) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::DeleteFile {
                path: path.into(),
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn rename_file(
        &self,
        from: impl Into<String>,
        to: impl Into<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::RenameFile {
                from: from.into(),
                to: to.into(),
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn snapshot_text_files(
        &self,
    ) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::SnapshotTextFiles { reply })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn snapshot_utf8_text_files(
        &self,
    ) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::SnapshotUtf8TextFiles { reply })
            .await
            .map_err(io_err)?;
        Ok(recv.await.map_err(io_err)?)
    }

    async fn stats(&self) -> Result<InMemoryFileIOStats, Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::Stats { reply })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)
    }

    async fn semantic_debug_snapshot(
        &self,
    ) -> Result<SemanticDebugSnapshot, Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::SemanticDebugSnapshot { reply })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn connectivity_status(
        &self,
    ) -> Result<ConnectivitySnapshot, Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::ConnectivityStatus { reply })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn request_single_file_scan(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let path = RelativePath::parse(path).map_err(io_err)?;
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::RequestScan {
                scope: ScanScope::SingleFile(path),
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn request_full_scan(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::RequestScan {
                scope: ScanScope::Full,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn inject_guarded_read_permission_denied(
        &self,
        path: impl Into<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::InjectGuardedReadPermissionDenied {
                path: path.into(),
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn inject_guarded_read_changed_between_stats(
        &self,
        path: impl Into<String>,
        replacement: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::InjectGuardedReadChangedBetweenStats {
                path: path.into(),
                replacement,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn inject_guarded_write_conflict_before_swap(
        &self,
        path: impl Into<String>,
        replacement: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::InjectGuardedWriteConflictBeforeSwap {
                path: path.into(),
                replacement,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn inject_guarded_write_conflict_after_swap(
        &self,
        path: impl Into<String>,
        replacement: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::InjectGuardedWriteConflictAfterSwap {
                path: path.into(),
                replacement,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn inject_guarded_write_temp_leak_and_fail(
        &self,
        path: impl Into<String>,
        temp_path: impl Into<String>,
        bytes: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::InjectGuardedWriteTempLeakAndFail {
                path: path.into(),
                temp_path: temp_path.into(),
                bytes,
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    #[allow(dead_code)]
    async fn inject_delete_if_exists_failure(
        &self,
        path: impl Into<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (reply, recv) = oneshot::channel();
        self.command_tx
            .send(SimDaemonCommand::InjectDeleteIfExistsFailure {
                path: path.into(),
                reply,
            })
            .await
            .map_err(io_err)?;
        recv.await.map_err(io_err)?.map_err(io_err)
    }

    async fn shutdown(&self) {
        let (reply, recv) = oneshot::channel();
        let _ = self
            .command_tx
            .send(SimDaemonCommand::Shutdown { reply })
            .await;
        let _ = recv.await;
    }
}

enum SimDaemonCommand {
    WriteFile {
        path: String,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    AppendFile {
        path: String,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    ReplaceFileContents {
        path: String,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    PrependFile {
        path: String,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    DeleteFile {
        path: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    RenameFile {
        from: String,
        to: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    SnapshotTextFiles {
        reply: oneshot::Sender<Result<BTreeMap<String, String>, String>>,
    },
    SnapshotUtf8TextFiles {
        reply: oneshot::Sender<BTreeMap<String, String>>,
    },
    Stats {
        reply: oneshot::Sender<InMemoryFileIOStats>,
    },
    SemanticDebugSnapshot {
        reply: oneshot::Sender<Result<SemanticDebugSnapshot, String>>,
    },
    ConnectivityStatus {
        reply: oneshot::Sender<Result<ConnectivitySnapshot, String>>,
    },
    RequestScan {
        scope: ScanScope,
        reply: oneshot::Sender<Result<(), String>>,
    },
    InjectGuardedReadPermissionDenied {
        path: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    InjectGuardedReadChangedBetweenStats {
        path: String,
        replacement: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    InjectGuardedWriteConflictBeforeSwap {
        path: String,
        replacement: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    InjectGuardedWriteConflictAfterSwap {
        path: String,
        replacement: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    InjectGuardedWriteTempLeakAndFail {
        path: String,
        temp_path: String,
        bytes: Bytes,
        reply: oneshot::Sender<Result<(), String>>,
    },
    InjectDeleteIfExistsFailure {
        path: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Shutdown {
        reply: oneshot::Sender<Option<String>>,
    },
}

fn run_wrong_cipher_clone_workload(
    sim: &mut turmoil::Sim<'static>,
    _seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000099".to_string());
    let correct_key = sim_encryption_key();
    let wrong_key = Some(EncryptionKey::new([0x24u8; 32]));

    let daemon_a = spawn_daemon_with_config(
        sim,
        "daemon-a",
        0,
        workspace_id.clone(),
        BTreeMap::new(),
        correct_key,
    )?;
    sim.set_link_latency(
        "daemon-a",
        "s2-lite",
        Duration::from_millis(DAEMON_A_S2_LINK_LATENCY_MS),
    );

    // daemon-b uses the wrong encryption key.
    let wrong_key_clone = wrong_key.clone();
    let workspace_id_b = workspace_id.clone();
    sim.client("daemon-b-wrong-cipher", async move {
        ensure_basin_exists().await?;
        let daemon_row = daemon_state::Row {
            workspace_id: workspace_id_b,
            s2_basin: SIM_BASIN.parse().map_err(io_err)?,
            s2_account_endpoint: None,
            s2_basin_endpoint: None,
            daemon_writer_id: DaemonWriterId(Bytes::copy_from_slice(&[1u8; 16])),
            stable_cursor: ..0,
            next_outbox_id: OutboxId::new(0),
            encryption_key: wrong_key_clone,
        };
        let db = open_memory_database().await.map_err(io_err)?;
        initialize_database(&db, &daemon_row)
            .await
            .map_err(io_err)?;
        let pool = semantic_pool(db).await.map_err(io_err)?;
        let semantic_service = SemanticService::new(pool);
        let s2_basin = sim_s2_client()
            .map_err(io_err)?
            .basin(daemon_row.s2_basin.clone());
        let engine_status = EngineStatusConfig {
            sync_root: PathBuf::from("/sim/daemon-b-wrong-cipher"),
            workspace_id: daemon_row.workspace_id.clone(),
            basin: daemon_row.s2_basin.clone(),
            daemon_writer_id: daemon_row.daemon_writer_id.clone(),
            stable_cursor: daemon_row.stable_cursor.clone(),
            started_at: time::OffsetDateTime::now_utc(),
            warnings: Vec::new(),
        };
        let cancellation_token = CancellationToken::new();
        let (notify_io, _notify_handle) = channel_notify_io();
        let file_io = FaultInjectingFileIO::new(InMemoryFileIO::new());
        let runtime = AppRuntime::new(AppRuntimeConfig {
            mode: RunMode::Sync,
            file_io,
            notify_io: Some(notify_io),
            semantic_service,
            daemon_row,
            s2_basin,
            clone_log_read_stop: None,
            engine_status: Some(engine_status),
            spy_tx: None,
        });
        let mut actors = runtime.spawn(cancellation_token.clone());

        // Wait for the runtime to fail due to wrong encryption key.
        let error = actors.wait_for_actor_stop().await;
        let _ = actors.shutdown(cancellation_token).await;
        match error {
            Some(err)
                if err.to_string().contains("encryption")
                    || err.to_string().contains("decryption") =>
            {
                info!("daemon-b correctly failed with encryption error: {err}");
                Ok(())
            }
            Some(err) => Err(io_err(format!(
                "daemon-b failed with unexpected error (expected encryption error): {err}"
            ))),
            None => Err(io_err("daemon-b did not fail (expected encryption error)")),
        }
    });

    sim.set_link_latency(
        "daemon-b-wrong-cipher",
        "s2-lite",
        Duration::from_millis(DAEMON_B_S2_LINK_LATENCY_MS),
    );

    sim.client("controller", async move {
        let path = "test.txt";
        let content = Bytes::from("encrypted content\n");
        daemon_a.write_file(path, content).await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        // Give daemon-b time to attempt reading and fail.
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        daemon_a.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn sim_s2_client() -> eyre::Result<S2> {
    let endpoints = S2Endpoints::new(
        AccountEndpoint::new("http://s2-lite:80")?,
        BasinEndpoint::new("http://s2-lite:80")?,
    )?;

    S2::new_with_connector(
        S2Config::new("ignored").with_endpoints(endpoints),
        s2_connector::TurmoilConnector,
    )
    .map_err(Into::into)
}

async fn ensure_basin_exists() -> Result<(), Box<dyn std::error::Error>> {
    let basin_name: BasinName = SIM_BASIN.parse().map_err(io_err)?;
    let s2 = sim_s2_client().map_err(io_err)?;
    let config = BasinConfig::default().with_stream_cipher(EncryptionAlgorithm::Aes256Gcm);
    match s2
        .create_basin(CreateBasinInput::new(basin_name.clone()).with_config(config))
        .await
    {
        Ok(_) => Ok(()),
        Err(S2Error::Server(err)) if err.code == "resource_already_exists" => {
            s2.reconfigure_basin(ReconfigureBasinInput::new(
                basin_name,
                BasinReconfiguration::new().with_stream_cipher(EncryptionAlgorithm::Aes256Gcm),
            ))
            .await
            .map_err(io_err)?;
            Ok(())
        }
        Err(err) => Err(io_err(err)),
    }
}

// ---------------------------------------------------------------------------
// New network-fault-injection and stress workloads
// ---------------------------------------------------------------------------

fn run_partition_both_daemons_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000014".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "shared.txt";
        let initial = format!("partition-both seed {seed}\n");
        daemon_a
            .write_file(path, Bytes::from(initial.clone()))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &initial, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        // Partition BOTH daemons from s2
        turmoil::partition("daemon-a", "s2-lite");
        turmoil::partition("daemon-b", "s2-lite");
        tokio::time::sleep(Duration::from_millis(250)).await;

        // daemon-a writes markers while offline
        let a_markers: Vec<String> = (0..4)
            .map(|idx| format!("A-both-offline-{idx}-seed-{seed}"))
            .collect();
        for marker in &a_markers {
            daemon_a
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_a.request_single_file_scan(path).await?;
        }

        // daemon-b writes markers while offline
        let b_markers: Vec<String> = (0..4)
            .map(|idx| format!("B-both-offline-{idx}-seed-{seed}"))
            .collect();
        for marker in &b_markers {
            daemon_b
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_b.request_single_file_scan(path).await?;
        }

        // Wait for outbox accumulation on both
        wait_for_outbox_at_least(&daemon_a, 1, steps).await?;
        wait_for_outbox_at_least(&daemon_b, 1, steps).await?;

        // Repair both simultaneously
        turmoil::repair("daemon-a", "s2-lite");
        turmoil::repair("daemon-b", "s2-lite");

        wait_for_connectivity_online(&daemon_a, steps).await?;
        wait_for_connectivity_online(&daemon_b, steps).await?;

        let expected_markers: Vec<String> =
            a_markers.iter().chain(b_markers.iter()).cloned().collect();
        let final_text =
            wait_for_shared_text_with_markers(&daemon_a, &daemon_b, path, &expected_markers, steps)
                .await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        println!(
            "SIM_OK workload=partition-both-daemons seed={seed} markers={} final_bytes={}",
            expected_markers.len(),
            final_text.len()
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_flapping_connectivity_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000015".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "shared.txt";
        let initial = format!("flapping seed {seed}\n");
        daemon_a
            .write_file(path, Bytes::from(initial.clone()))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &initial, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        let cycles = 5u64;
        let mut all_markers: Vec<String> = Vec::new();

        for cycle in 0..cycles {
            // Partition daemon-b
            turmoil::partition("daemon-b", "s2-lite");

            // daemon-a writes during the partition
            let marker = format!("A-flap-cycle-{cycle}-seed-{seed}");
            daemon_a
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_a.request_single_file_scan(path).await?;
            all_markers.push(marker);

            // Deterministic delay while partitioned
            let delay = deterministic_delay_ms(seed, 0xF1A9_0000_0000_0001, cycle);
            tokio::time::sleep(Duration::from_millis(delay)).await;

            // Repair
            turmoil::repair("daemon-b", "s2-lite");

            // Brief pause after repair for reconnection
            tokio::time::sleep(Duration::from_millis(500)).await;
            wait_for_connectivity_online(&daemon_b, steps).await?;
        }

        // After all cycles, wait for full convergence
        let final_text =
            wait_for_shared_text_with_markers(&daemon_a, &daemon_b, path, &all_markers, steps)
                .await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        println!(
            "SIM_OK workload=flapping-connectivity seed={seed} cycles={cycles} markers={} final_bytes={}",
            all_markers.len(),
            final_text.len()
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_asymmetric_partition_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000016".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "shared.txt";
        let initial = format!("asymmetric seed {seed}\n");
        daemon_a
            .write_file(path, Bytes::from(initial.clone()))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &initial, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        // One-way partition: daemon-b cannot send to s2 but CAN receive
        turmoil::partition_oneway("daemon-b", "s2-lite");
        tokio::time::sleep(Duration::from_millis(250)).await;

        // daemon-a makes edits (should reach daemon-b via s2->daemon-b path)
        let a_markers: Vec<String> = (0..3)
            .map(|idx| format!("A-asym-{idx}-seed-{seed}"))
            .collect();
        for marker in &a_markers {
            daemon_a
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_a.request_single_file_scan(path).await?;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // daemon-b makes edits (should queue in outbox since daemon-b->s2 blocked)
        let b_markers: Vec<String> = (0..3)
            .map(|idx| format!("B-asym-{idx}-seed-{seed}"))
            .collect();
        for marker in &b_markers {
            daemon_b
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_b.request_single_file_scan(path).await?;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // Repair the one-way partition
        turmoil::repair_oneway("daemon-b", "s2-lite");

        wait_for_connectivity_online(&daemon_b, steps).await?;

        let expected_markers: Vec<String> =
            a_markers.iter().chain(b_markers.iter()).cloned().collect();
        let final_text =
            wait_for_shared_text_with_markers(&daemon_a, &daemon_b, path, &expected_markers, steps)
                .await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        println!(
            "SIM_OK workload=asymmetric-partition seed={seed} markers={} final_bytes={}",
            expected_markers.len(),
            final_text.len()
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_partition_during_projection_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000017".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        // daemon-b creates a batch of files
        let file_count = 8u64;
        let mut expected_files: BTreeMap<String, String> = BTreeMap::new();
        for i in 0..file_count {
            let path = format!("batch-{i}.txt");
            let content = format!("batch file {i} seed {seed}\n");
            daemon_b
                .write_file(&path, Bytes::from(content.clone()))
                .await?;
            expected_files.insert(path.clone(), content);
            daemon_b.request_single_file_scan(&path).await?;
        }

        // Brief delay to let projection start arriving at daemon-a
        let delay = deterministic_delay_ms(seed, 0xAD17_0000_0000_0001, 0);
        tokio::time::sleep(Duration::from_millis(delay)).await;

        // Partition daemon-a mid-projection
        turmoil::partition("daemon-a", "s2-lite");
        tokio::time::sleep(Duration::from_millis(
            deterministic_delay_ms(seed, 0xAD17_0000_0000_0002, 0) + 500,
        ))
        .await;

        // Repair
        turmoil::repair("daemon-a", "s2-lite");
        wait_for_connectivity_online(&daemon_a, steps).await?;

        // Verify all files eventually converge on both daemons
        wait_for_text_files(&daemon_a, &daemon_b, &expected_files, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        println!("SIM_OK workload=partition-during-projection seed={seed} files={file_count}",);
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_message_hold_and_release_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000018".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "shared.txt";
        let initial = format!("hold-release seed {seed}\n");
        daemon_a
            .write_file(path, Bytes::from(initial.clone()))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &initial, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        // Hold messages from daemon-b to s2 (paused, not dropped)
        turmoil::hold("daemon-b", "s2-lite");
        tokio::time::sleep(Duration::from_millis(250)).await;

        // daemon-b makes several edits while held
        let b_markers: Vec<String> = (0..4)
            .map(|idx| format!("B-held-{idx}-seed-{seed}"))
            .collect();
        for marker in &b_markers {
            daemon_b
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_b.request_single_file_scan(path).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // daemon-a also makes edits concurrently
        let a_markers: Vec<String> = (0..3)
            .map(|idx| format!("A-concurrent-{idx}-seed-{seed}"))
            .collect();
        for marker in &a_markers {
            daemon_a
                .append_file(path, Bytes::from(format!("{marker}\n")))
                .await?;
            daemon_a.request_single_file_scan(path).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Release all paused messages at once
        turmoil::release("daemon-b", "s2-lite");

        let expected_markers: Vec<String> =
            a_markers.iter().chain(b_markers.iter()).cloned().collect();
        let final_text =
            wait_for_shared_text_with_markers(&daemon_a, &daemon_b, path, &expected_markers, steps)
                .await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        println!(
            "SIM_OK workload=message-hold-and-release seed={seed} markers={} final_bytes={}",
            expected_markers.len(),
            final_text.len()
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_transient_high_packet_loss_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("00000000000000000000000000000019".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    // Set packet loss on daemon-b <-> s2-lite links (applied for entire sim).
    // Rate kept moderate so initial TCP connections still succeed across seeds.
    sim.set_link_fail_rate("daemon-b", "s2-lite", 0.05);
    sim.set_link_fail_rate("s2-lite", "daemon-b", 0.05);

    sim.client("controller", async move {
        // Each daemon edits its own file to avoid conflict copies under packet loss
        let path_a = "lossy-a.txt";
        let path_b = "lossy-b.txt";
        let initial_a = format!("lossy-a seed {seed}\n");
        let initial_b = format!("lossy-b seed {seed}\n");
        daemon_a
            .write_file(path_a, Bytes::from(initial_a.clone()))
            .await?;
        daemon_a.request_single_file_scan(path_a).await?;
        daemon_b
            .write_file(path_b, Bytes::from(initial_b.clone()))
            .await?;
        daemon_b.request_single_file_scan(path_b).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path_a, &initial_a, steps).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path_b, &initial_b, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        // Both daemons make edits over several rounds under packet loss
        let mut a_markers: Vec<String> = Vec::new();
        let mut b_markers: Vec<String> = Vec::new();
        for round in 0..3u64 {
            let a_marker = format!("A-lossy-r{round}-seed-{seed}");
            daemon_a
                .append_file(path_a, Bytes::from(format!("{a_marker}\n")))
                .await?;
            daemon_a.request_single_file_scan(path_a).await?;
            a_markers.push(a_marker);

            let b_marker = format!("B-lossy-r{round}-seed-{seed}");
            daemon_b
                .append_file(path_b, Bytes::from(format!("{b_marker}\n")))
                .await?;
            daemon_b.request_single_file_scan(path_b).await?;
            b_markers.push(b_marker);

            let delay = deterministic_delay_ms(seed, 0x1055_0000_0000_0001, round);
            tokio::time::sleep(Duration::from_millis(delay + 500)).await;
        }

        // Wait for convergence (may take longer due to retries under packet loss)
        wait_for_shared_text_with_markers(&daemon_a, &daemon_b, path_a, &a_markers, steps)
            .await?;
        wait_for_shared_text_with_markers(&daemon_a, &daemon_b, path_b, &b_markers, steps)
            .await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        let total = a_markers.len() + b_markers.len();
        println!(
            "SIM_OK workload=transient-high-packet-loss seed={seed} markers={total} rounds=3",
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_sustained_editing_stress_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("0000000000000000000000000000001a".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let rounds = 10u64;
        let mut a_file_markers: Vec<String> = Vec::new();
        let mut b_file_markers: Vec<String> = Vec::new();

        for round in 0..rounds {
            // daemon-a creates/edits files with unique markers
            for file_idx in 0..4u64 {
                let path = format!("stress-a-{file_idx}.txt");
                let marker = format!("A-r{round}-f{file_idx}-seed-{seed}");
                daemon_a
                    .append_file(&path, Bytes::from(format!("{marker}\n")))
                    .await?;
                daemon_a.request_single_file_scan(&path).await?;
                a_file_markers.push(marker);
            }

            // daemon-b creates/edits files with unique markers
            for file_idx in 0..4u64 {
                let path = format!("stress-b-{file_idx}.txt");
                let marker = format!("B-r{round}-f{file_idx}-seed-{seed}");
                daemon_b
                    .append_file(&path, Bytes::from(format!("{marker}\n")))
                    .await?;
                daemon_b.request_single_file_scan(&path).await?;
                b_file_markers.push(marker);
            }

            // Brief pause between rounds for convergence
            let delay = deterministic_delay_ms(seed, 0x57AE_5500_0000_0001, round);
            tokio::time::sleep(Duration::from_millis(delay + 200)).await;
        }

        // Verify all per-daemon file markers converge
        for file_idx in 0..4u64 {
            let path_a = format!("stress-a-{file_idx}.txt");
            let markers_a: Vec<String> = a_file_markers
                .iter()
                .filter(|m| m.contains(&format!("-f{file_idx}-")))
                .cloned()
                .collect();
            wait_for_shared_text_with_markers(&daemon_a, &daemon_b, &path_a, &markers_a, steps)
                .await?;

            let path_b = format!("stress-b-{file_idx}.txt");
            let markers_b: Vec<String> = b_file_markers
                .iter()
                .filter(|m| m.contains(&format!("-f{file_idx}-")))
                .cloned()
                .collect();
            wait_for_shared_text_with_markers(&daemon_a, &daemon_b, &path_b, &markers_b, steps)
                .await?;
        }
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        let total = a_file_markers.len() + b_file_markers.len();
        println!(
            "SIM_OK workload=sustained-editing-stress seed={seed} rounds={rounds} total_markers={total}",
        );
        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Ok(())
    });

    Ok(())
}

fn run_delete_while_offline_workload(
    sim: &mut turmoil::Sim<'static>,
    seed: u64,
    steps: u64,
) -> eyre::Result<()> {
    let workspace_id = WorkspaceId("0000000000000000000000000000001b".to_string());
    let daemon_a = spawn_daemon(sim, "daemon-a", 0, workspace_id.clone())?;
    let daemon_b = spawn_daemon(sim, "daemon-b", 1, workspace_id)?;
    configure_daemon_s2_link_latencies(sim);

    sim.client("controller", async move {
        let path = "contested.txt";
        let initial = format!("contested seed {seed}\n");
        daemon_a
            .write_file(path, Bytes::from(initial.clone()))
            .await?;
        daemon_a.request_single_file_scan(path).await?;
        wait_for_file_text(&daemon_a, &daemon_b, path, &initial, steps).await?;
        wait_for_outbox_empty(&daemon_a, steps).await?;
        wait_for_outbox_empty(&daemon_b, steps).await?;

        // Partition daemon-b
        turmoil::partition("daemon-b", "s2-lite");
        tokio::time::sleep(Duration::from_millis(250)).await;

        // daemon-b appends to the file while offline
        let b_marker = format!("B-offline-edit-seed-{seed}");
        daemon_b
            .append_file(path, Bytes::from(format!("{b_marker}\n")))
            .await?;
        daemon_b.request_single_file_scan(path).await?;
        wait_for_outbox_at_least(&daemon_b, 1, steps).await?;

        // daemon-a deletes the file while daemon-b is partitioned
        daemon_a.delete_file(path).await?;
        daemon_a.request_single_file_scan(path).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Repair daemon-b
        turmoil::repair("daemon-b", "s2-lite");
        wait_for_connectivity_online(&daemon_b, steps).await?;

        // Wait for convergence — the CRDT should resolve the conflict
        // Either: file is deleted, edit preserved as conflict copy, edit wins, etc.
        let mut last_a = BTreeMap::new();
        let mut last_b = BTreeMap::new();
        for _ in 0..steps {
            last_a = daemon_a.snapshot_text_files().await?;
            last_b = daemon_b.snapshot_text_files().await?;
            if last_a == last_b {
                // Converged — determine resolution
                let resolution = if last_a.is_empty() {
                    "delete_won"
                } else if last_a.get(path).is_some() {
                    "edit_preserved"
                } else {
                    "conflict_copy"
                };
                wait_for_outbox_empty(&daemon_a, steps).await?;
                wait_for_outbox_empty(&daemon_b, steps).await?;
                println!(
                    "SIM_OK workload=delete-while-offline seed={seed} resolution={resolution} files={}",
                    last_a.len()
                );
                daemon_a.shutdown().await;
                daemon_b.shutdown().await;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        daemon_a.shutdown().await;
        daemon_b.shutdown().await;
        Err(io_err(format!(
            "delete-while-offline did not converge; a={last_a:?} b={last_b:?}"
        )))
    });

    Ok(())
}

fn init_sim(seed: u64, failure_rate: f64) -> turmoil::Sim<'static> {
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .simulation_duration(Duration::MAX)
        .min_message_latency(Duration::from_millis(2))
        .max_message_latency(Duration::from_millis(300))
        .tcp_capacity(10_000)
        .tick_duration(Duration::from_millis(1))
        .epoch(SystemTime::UNIX_EPOCH);

    if failure_rate > 0.0 {
        builder.fail_rate(failure_rate);
    }

    builder.build()
}

fn seed_rng(seed: u64) {
    mad_turmoil::rand::set_rng(rand::rngs::StdRng::seed_from_u64(seed));
    fastrand::seed(seed);
}

/// Stamps each log line with the simulation step (sim-elapsed ms; the sim
/// tick is explicitly 1ms) and a global event ordinal, instead of wall-clock
/// time. The ordinal disambiguates events within a single step and gives
/// `meta` determinism comparisons a stable line identity.
struct SimStepTimeFormat;

impl tracing_subscriber::fmt::time::FormatTime for SimStepTimeFormat {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        static EVENT_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let event = EVENT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let step = turmoil::sim_elapsed().unwrap_or_default().as_millis();
        write!(w, "[s{step} e{event}]")
    }
}

fn init_tracing(quiet: bool) {
    let filter = if quiet {
        tracing_subscriber::EnvFilter::new("error")
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_timer(SimStepTimeFormat)
        .with_target(true)
        .try_init();
}

fn random_seed() -> u64 {
    use std::io::Read;

    let mut bytes = [0u8; 8];
    if let Ok(mut urandom) = std::fs::File::open("/dev/urandom")
        && urandom.read_exact(&mut bytes).is_ok()
    {
        return u64::from_le_bytes(bytes);
    }

    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos() as u64
}

fn io_err(err: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    Box::new(std::io::Error::other(err.to_string()))
}
