//! Language-code normalization — ISO 639 alias folding, **not** filename
//! parsing.
//!
//! This lives apart from [`crate::filename_meta`] on purpose: it doesn't read a
//! release title, it canonicalizes a *language code* (an indexer's declared
//! language, a query's `languages:` filter, a title-derived stamp) onto the
//! single ISO 639-2/B alpha-3 vocabulary the rest of the stack compares against.
//! It is the one piece of the old `filename_meta` module that has no equivalent
//! in the `metamesh-plugin-filename-parser` plugin — because it isn't filename
//! parsing — so it stays in Rust while the title-parsing algorithm itself is
//! owned by the plugin. Callers in the **request hot path** (e.g. the torznab
//! indexer-language fan-out skip) need it synchronously, with no network hop.

/// Normalize a language code to the ISO 639-2/B alpha-3 vocabulary the stack
/// emits (`fre`/`ger`/`chi`, **not** the 639-2/T `fra`/`deu`/`zho` nor the
/// 639-1 two-letter forms), lowercased. This is the space meta-share's
/// "languages I read" preference uses and that the title-derived language stamps
/// write, so an operator who declares an indexer's language as `fra`/`de`/`zh`
/// still lines up with a `fre`/`ger`/`chi` query filter. Unknown /
/// already-canonical codes pass through lowercased+trimmed unchanged.
pub fn normalize_lang_code(code: &str) -> String {
    let c = code.trim().to_ascii_lowercase();
    let mapped = match c.as_str() {
        // 639-2/T and 639-1 → 639-2/B for the codes that actually differ.
        "fra" | "fr" => "fre",
        "deu" | "de" => "ger",
        "zho" | "zh" => "chi",
        "nld" | "nl" => "dut",
        "ell" | "el" => "gre",
        "ron" | "ro" => "rum",
        "slk" | "sk" => "slo",
        "ces" | "cs" => "cze",
        // 639-1 → 639-2 for the rest of the language-tag output space (these
        // share the same /B and /T alpha-3, so only the two-letter form maps).
        "en" => "eng",
        "ja" => "jpn",
        "it" => "ita",
        "es" => "spa",
        "pt" => "por",
        "ko" => "kor",
        "ru" => "rus",
        "pl" => "pol",
        "hu" => "hun",
        "uk" => "ukr",
        "bg" => "bul",
        "sv" => "swe",
        "no" => "nor",
        "da" => "dan",
        "fi" => "fin",
        "tr" => "tur",
        "ar" => "ara",
        "hi" => "hin",
        "ta" => "tam",
        "te" => "tel",
        "ml" => "mal",
        "vi" => "vie",
        "th" => "tha",
        _ => return c,
    };
    mapped.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_lang_maps_to_639_2b_vocabulary() {
        // 639-2/T and 639-1 spellings collapse onto the /B forms the stack uses.
        assert_eq!(normalize_lang_code("fra"), "fre");
        assert_eq!(normalize_lang_code("FR"), "fre");
        assert_eq!(normalize_lang_code("deu"), "ger");
        assert_eq!(normalize_lang_code("zh"), "chi");
        assert_eq!(normalize_lang_code("nld"), "dut");
        // Already-canonical / unknown codes pass through trimmed + lowercased.
        assert_eq!(normalize_lang_code("  ENG "), "eng");
        assert_eq!(normalize_lang_code("fre"), "fre");
        assert_eq!(normalize_lang_code("mul"), "mul");
        assert_eq!(normalize_lang_code("xyz"), "xyz");
    }
}
