//! Windows console code page management and RAII restoration guard.

#[cfg(windows)]
use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(windows)]
static ORIGINAL_OUTPUT_CP: AtomicU32 = AtomicU32::new(0);
#[cfg(windows)]
static ORIGINAL_INPUT_CP: AtomicU32 = AtomicU32::new(0);

#[derive(Debug)]
pub struct ConsoleCodePageGuard {
    #[cfg(windows)]
    orig_output_cp: u32,
    #[cfg(windows)]
    orig_input_cp: u32,
}

impl ConsoleCodePageGuard {
    /// Queries current console code pages, sets them to UTF-8 (65001),
    /// and returns an RAII guard that restores the original code pages when dropped.
    pub fn init() -> Self {
        #[cfg(windows)]
        unsafe {
            let orig_output_cp = windows_sys::Win32::System::Console::GetConsoleOutputCP();
            let orig_input_cp = windows_sys::Win32::System::Console::GetConsoleCP();

            let _ = ORIGINAL_OUTPUT_CP.compare_exchange(
                0,
                orig_output_cp,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            let _ = ORIGINAL_INPUT_CP.compare_exchange(
                0,
                orig_input_cp,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            if orig_output_cp != 65001 {
                windows_sys::Win32::System::Console::SetConsoleOutputCP(65001);
            }
            if orig_input_cp != 65001 {
                windows_sys::Win32::System::Console::SetConsoleCP(65001);
            }

            Self {
                orig_output_cp,
                orig_input_cp,
            }
        }

        #[cfg(not(windows))]
        Self {}
    }

    /// Returns the currently active console output code page.
    pub fn output_codepage(&self) -> u32 {
        #[cfg(windows)]
        unsafe {
            windows_sys::Win32::System::Console::GetConsoleOutputCP()
        }
        #[cfg(not(windows))]
        65001
    }

    /// Returns the original console output code page recorded before initialization.
    pub fn orig_output_codepage(&self) -> u32 {
        #[cfg(windows)]
        {
            self.orig_output_cp
        }
        #[cfg(not(windows))]
        65001
    }

    /// Returns the original console input code page recorded before initialization.
    pub fn orig_input_codepage(&self) -> u32 {
        #[cfg(windows)]
        {
            self.orig_input_cp
        }
        #[cfg(not(windows))]
        65001
    }

    /// Returns true if the output code page is UTF-8 (65001).
    pub fn is_utf8(&self) -> bool {
        self.output_codepage() == 65001
    }

    /// Restores the console code pages to their values recorded prior to initialization.
    pub fn restore(&self) {
        Self::restore_original();
    }

    /// Static helper to restore original code pages from anywhere (e.g. panic hooks, signal handlers).
    pub fn restore_original() {
        #[cfg(windows)]
        unsafe {
            let orig_out = ORIGINAL_OUTPUT_CP.load(Ordering::SeqCst);
            let orig_in = ORIGINAL_INPUT_CP.load(Ordering::SeqCst);
            if orig_out != 0 {
                windows_sys::Win32::System::Console::SetConsoleOutputCP(orig_out);
            }
            if orig_in != 0 {
                windows_sys::Win32::System::Console::SetConsoleCP(orig_in);
            }
        }
    }
}

impl Drop for ConsoleCodePageGuard {
    fn drop(&mut self) {
        self.restore();
    }
}
