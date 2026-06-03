pub fn print_logo() {
    const LOGO_ANSI: &str = include_str!("../assets/geminis-labs-logo.txt");
    const LOGO_PLAIN: &str = include_str!("../assets/geminis-labs-logo_plain.txt");
    // Structured logs must remain plain text without ANSI control sequences.
    const GRAY: &str = "\x1b[38;2;180;180;180m";
    const WHITE: &str = "\x1b[97m";
    const RESET: &str = "\x1b[0m";

    let ansi_enabled = is_ansi_enabled();
    let logo = if ansi_enabled { LOGO_ANSI } else { LOGO_PLAIN };
    let (gray, white, reset) = if ansi_enabled {
        (GRAY, WHITE, RESET)
    } else {
        ("", "", "")
    };

    println!();
    println!("\t\t{white}GeminiLabs :: Geocontext{reset}");
    println!("{gray}────────────────────────────────────────────────────────────────{reset}");
    println!();

    use std::io::Write;
    if let Err(e) = std::io::stdout().write_all(logo.as_bytes()) {
        tracing::warn!(error = %e, "failed to print startup logo");
    }

    println!();
    println!("{gray}────────────────────────────────────────────────────────────────{reset}");
    println!("\t\t{gray}geocontext • @geminislabs{reset}");
    println!();
    println!();
}

fn is_ansi_enabled() -> bool {
    use std::io::IsTerminal;

    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }

    if matches!(std::env::var("TERM").ok().as_deref(), Some("dumb")) {
        return false;
    }

    std::io::stdout().is_terminal()
}
