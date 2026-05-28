pub mod spsc;
#[cfg(target_arch = "x86_64")]
pub mod wait;

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

use std::io;

pub fn pin_current_thread(core_id: usize) -> bool {
    let ids = match core_affinity::get_core_ids() {
        Some(v) => v,
        None => return false,
    };
    match ids.into_iter().find(|c| c.id == core_id) {
        Some(id) => core_affinity::set_for_current(id),
        None => false,
    }
}

pub fn set_sched_fifo(priority: i32) -> io::Result<()> {
    let param = libc::sched_param {
        sched_priority: priority,
    };
    let ret = unsafe { libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) };
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_to_invalid_core_returns_false() {
        assert!(!pin_current_thread(usize::MAX));
    }

    #[test]
    fn set_sched_fifo_without_cap_returns_err_or_ok() {
        match set_sched_fifo(99) {
            Ok(()) => {}
            Err(e) => assert!(
                e.kind() == std::io::ErrorKind::PermissionDenied
                    || e.raw_os_error() == Some(libc::EPERM),
                "unexpected error: {e}"
            ),
        }
    }
}
