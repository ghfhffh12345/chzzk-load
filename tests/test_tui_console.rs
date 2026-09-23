use chzzk_load::tui::console::ConsoleCodePageGuard;

#[test]
fn test_console_codepage_guard_lifecycle() {
    let guard = ConsoleCodePageGuard::init();

    #[cfg(windows)]
    {
        // On Windows, output codepage should now be UTF-8 (65001)
        assert_eq!(guard.output_codepage(), 65001);
        assert!(guard.is_utf8());

        // The original code pages must be non-zero
        assert!(guard.orig_output_codepage() > 0);
        assert!(guard.orig_input_codepage() > 0);
    }

    #[cfg(not(windows))]
    {
        assert!(guard.is_utf8());
    }

    drop(guard);
}

#[test]
fn test_console_codepage_guard_restore() {
    let guard = ConsoleCodePageGuard::init();

    #[cfg(windows)]
    {
        let orig = guard.orig_output_codepage();
        guard.restore();
        unsafe {
            let current = windows_sys::Win32::System::Console::GetConsoleOutputCP();
            assert_eq!(current, orig);
        }
    }

    #[cfg(not(windows))]
    {
        guard.restore();
        assert!(guard.is_utf8());
    }
}

#[test]
fn test_console_codepage_guard_restore_original_static() {
    let guard = ConsoleCodePageGuard::init();

    #[cfg(windows)]
    {
        let orig = guard.orig_output_codepage();
        ConsoleCodePageGuard::restore_original();
        unsafe {
            let current = windows_sys::Win32::System::Console::GetConsoleOutputCP();
            assert_eq!(current, orig);
        }
    }

    #[cfg(not(windows))]
    {
        ConsoleCodePageGuard::restore_original();
        assert!(guard.is_utf8());
    }
}
