//! Hardware-assisted low-power spin-wait primitives.
//!
//! Arms a cache-line monitor and enters a low-power C0 sub-state until a
//! store to the monitored line, a TSC-based timeout, or an interrupt wakes
//! the core. Backend (AMD MONITORX/MWAITX, Intel WAITPKG UMONITOR/UMWAIT,
//! or fallback spin) is picked once via CPUID.

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

/// SAFETY: `addr` must point to mapped, readable memory that remains valid until
/// the paired [`mwaitx`] returns. CPU must support MONITORX.
#[inline(always)]
pub unsafe fn monitorx(addr: *const u8) {
    core::arch::asm!(
        "monitorx",
        in("rax") addr,
        in("ecx") 0u32,
        in("edx") 0u32,
        options(nostack, preserves_flags),
    );
}

/// SAFETY: Must be preceded by [`monitorx`] on the current thread. CPU must support MWAITX.
#[inline(always)]
pub unsafe fn mwaitx(timeout_tsc: u32) {
    if timeout_tsc > 0 {
        core::arch::asm!(
            "push rbx",
            "mov ebx, {timeout:e}",
            "mwaitx",
            "pop rbx",
            timeout = in(reg) timeout_tsc,
            in("eax") 0u32,
            in("ecx") 2u32,
            options(nostack, preserves_flags),
        );
    } else {
        core::arch::asm!(
            "mwaitx",
            in("eax") 0u32,
            in("ecx") 0u32,
            options(nostack, preserves_flags),
        );
    }
}

/// SAFETY: `addr` must point to mapped, readable memory. CPU must support WAITPKG.
#[inline(always)]
pub unsafe fn umonitor(addr: *const u8) {
    core::arch::asm!(
        "umonitor {addr:r}",
        addr = in(reg) addr,
        options(nostack, preserves_flags),
    );
}

/// SAFETY: Must be preceded by [`umonitor`] on the current thread. CPU must support WAITPKG.
#[inline(always)]
pub unsafe fn umwait(deadline_tsc: u64) {
    let lo = deadline_tsc as u32;
    let hi = (deadline_tsc >> 32) as u32;
    core::arch::asm!(
        "umwait {ctrl:e}",
        ctrl = in(reg) 0u32,
        in("edx") hi,
        in("eax") lo,
        options(nostack, preserves_flags),
    );
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum IdleBackend {
    AmdMwaitx = 1,
    IntelUmwait = 2,
    SpinLoop = 3,
}

fn detect_backend() -> IdleBackend {
    use std::sync::atomic::{AtomicU8, Ordering};
    static CACHED: AtomicU8 = AtomicU8::new(0);
    let v = CACHED.load(Ordering::Relaxed);
    if v != 0 {
        return match v {
            1 => IdleBackend::AmdMwaitx,
            2 => IdleBackend::IntelUmwait,
            _ => IdleBackend::SpinLoop,
        };
    }
    let backend = detect_backend_cpuid();
    CACHED.store(backend as u8, Ordering::Relaxed);
    backend
}

fn detect_backend_cpuid() -> IdleBackend {
    unsafe {
        let ext = core::arch::x86_64::__cpuid(0x8000_0001);
        if ext.ecx & (1 << 29) != 0 {
            return IdleBackend::AmdMwaitx;
        }
        let feat = core::arch::x86_64::__cpuid_count(7, 0);
        if feat.ecx & (1 << 5) != 0 {
            return IdleBackend::IntelUmwait;
        }
    }
    IdleBackend::SpinLoop
}

/// Default timeout: ~25 µs at 4.05 GHz.
pub const DEFAULT_TIMEOUT_TSC: u32 = 100_000;

/// Idle-wait on a cache line with automatic backend selection.
///
/// # Safety
///
/// `addr` must point to mapped, readable memory that remains valid for
/// the duration of the call.
#[inline(always)]
pub unsafe fn idle_wait(addr: *const u8, condition_still_true: impl FnOnce() -> bool) {
    match detect_backend() {
        IdleBackend::AmdMwaitx => {
            monitorx(addr);
            if condition_still_true() {
                mwaitx(DEFAULT_TIMEOUT_TSC);
            }
        }
        IdleBackend::IntelUmwait => {
            umonitor(addr);
            if condition_still_true() {
                let deadline = rdtsc() + DEFAULT_TIMEOUT_TSC as u64;
                umwait(deadline);
            }
        }
        IdleBackend::SpinLoop => {
            let initial = core::ptr::read_volatile(addr as *const u64);
            for _ in 0..256 {
                core::hint::spin_loop();
                if core::ptr::read_volatile(addr as *const u64) != initial {
                    return;
                }
            }
        }
    }
}
