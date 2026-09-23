use ratatui::style::Color;

/// Exact subtle, muted, and balanced palette sampled directly from
/// `assets/tui-preview.png` (as rendered in Zed terminal with Catppuccin Frappé).
///
/// Uses explicit 24-bit TrueColor RGB values to guarantee identical, balanced,
/// and non-oversaturated colors across all terminal emulators and platforms.
pub const CYAN: Color = Color::Rgb(105, 156, 154); // #699c9a - ACTIVE badge, REC log badge
pub const GREEN: Color = Color::Rgb(131, 162, 117); // #83a275 - LIVE badge, UP badge, CLEAN log badge
pub const RED: Color = Color::Rgb(177, 109, 116); // #b16d74 - REC badge, ERROR log badge
pub const YELLOW: Color = Color::Rgb(175, 156, 122); // #af9c7a - WARN log badge, shutdown header
pub const BLUE: Color = Color::Rgb(112, 135, 188); // #7087bc - DRIVE log badge
pub const MAGENTA: Color = Color::Rgb(185, 144, 181); // #b990b5 - FFMPEG log badge
pub const MUTED_GRAY: Color = Color::Rgb(124, 128, 144); // #7c8090 - OFFLINE, IDLE, INFO, POLL, panel headers, keybind footers
pub const DIVIDER: Color = Color::Rgb(83, 88, 111); // #53586f - Horizontal rule dividers ─
pub const SOFT_RED: Color = Color::Rgb(192, 122, 126); // #c07a7e - Shutdown warning footer
