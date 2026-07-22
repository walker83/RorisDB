//! InfluxDB line protocol parser

use std::collections::HashMap;

/// Parsed line protocol point
#[derive(Debug, Clone)]
pub struct Point {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, FieldValue>,
    pub timestamp: Option<i64>,
}

/// Field value types
#[derive(Debug, Clone)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    String(String),
    Boolean(bool),
}

/// Line protocol parser
pub struct LineProtocolParser;

impl LineProtocolParser {
    /// Parse line protocol string into points
    /// Format: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
    ///
    /// Handles backslash escapes per the InfluxDB line protocol spec: in the
    /// measurement and tag keys/values, the characters `,`, ` ` and `=` may be
    /// escaped with a leading backslash. The first *unescaped* space separates
    /// the measurement+tags segment from the fields+timestamp segment.
    pub fn parse(line: &str) -> Option<Point> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }

        // Split on the first unescaped space (separates measurement+tags from
        // fields+timestamp). A backslash escapes the following character.
        let split_at = next_unescaped_space(line)?;
        let measurement_tags = &line[..split_at];
        let fields_timestamp = &line[split_at + 1..];

        // Parse measurement and tags
        let (measurement, tags) = Self::parse_measurement_tags(measurement_tags)?;

        // Parse fields and timestamp
        let (fields, timestamp) = Self::parse_fields_timestamp(fields_timestamp)?;

        Some(Point {
            measurement,
            tags,
            fields,
            timestamp,
        })
    }

    fn parse_measurement_tags(s: &str) -> Option<(String, HashMap<String, String>)> {
        // Split on unescaped commas. Measurement is the first token; each
        // subsequent token is `tagkey=tagvalue`, all with escapes unescaped.
        let tokens = split_on_unescaped(s, ',');
        if tokens.is_empty() {
            return None;
        }

        let measurement = unescape_measurement(&tokens[0]);
        let mut tags = HashMap::new();

        for part in &tokens[1..] {
            // Split on the first unescaped '='.
            if let Some(eq) = next_unescaped_char(part, '=') {
                let key = unescape_tag(&part[..eq]);
                let value = unescape_tag(&part[eq + 1..]);
                tags.insert(key, value);
            }
        }

        Some((measurement, tags))
    }

    fn parse_fields_timestamp(s: &str) -> Option<(HashMap<String, FieldValue>, Option<i64>)> {
        // The timestamp is separated from the field set by the first space
        // that is neither backslash-escaped nor inside a quoted string field
        // value.
        let (fields_str, timestamp) = match find_field_timestamp_boundary(s) {
            Some(idx) => (&s[..idx], s[idx + 1..].trim().parse::<i64>().ok()),
            None => (s, None),
        };

        let mut fields = HashMap::new();
        for field in split_fields(fields_str) {
            if let Some(eq) = next_unescaped_char(&field, '=') {
                let key = unescape_field_key(&field[..eq]);
                let raw_value = &field[eq + 1..];
                let field_value = Self::parse_field_value(raw_value)?;
                fields.insert(key, field_value);
            }
        }

        Some((fields, timestamp))
    }

    fn parse_field_value(s: &str) -> Option<FieldValue> {
        if s.is_empty() {
            return None;
        }

        // Boolean — InfluxDB line protocol accepts t/T/true/True/TRUE and
        // f/F/false/False/FALSE.
        if s == "t" || s == "T" || s == "true" || s == "True" || s == "TRUE" {
            return Some(FieldValue::Boolean(true));
        }
        if s == "f" || s == "F" || s == "false" || s == "False" || s == "FALSE" {
            return Some(FieldValue::Boolean(false));
        }

        // String (quoted). A quoted field value preserves everything between
        // the opening and closing quote, with `\"` unescaped to `"`.
        if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
            let inner = &s[1..s.len() - 1];
            return Some(FieldValue::String(inner.replace("\\\"", "\"")));
        }

        // Integer (ends with 'i')
        if s.ends_with('i') {
            if let Ok(n) = s[..s.len() - 1].parse::<i64>() {
                return Some(FieldValue::Integer(n));
            }
        }

        // Float
        if let Ok(f) = s.parse::<f64>() {
            return Some(FieldValue::Float(f));
        }

        None
    }
}

/// Find the byte index of the first space that is not preceded by a backslash.
/// Returns `None` if there is no unescaped space.
fn next_unescaped_space(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2, // skip the escaped char
            b' ' => return Some(i),
            _ => i += 1,
        }
    }
    None
}

/// Like [`next_unescaped_space`], but also skips over spaces that appear inside
/// a double-quoted string field value (e.g. `msg="a b"`). Used to find the
/// boundary between the field set and the timestamp.
fn find_field_timestamp_boundary(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                i += 2; // skip the escaped char
            }
            b'"' => {
                in_string = !in_string;
                i += 1;
            }
            b' ' if !in_string => return Some(i),
            _ => i += 1,
        }
    }
    None
}

/// Find the byte index of the first occurrence of `ch` not preceded by a
/// backslash.
fn next_unescaped_char(s: &str, ch: char) -> Option<usize> {
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            // Skip the next char (escaped).
            chars.next();
            continue;
        }
        if c == ch {
            return Some(i);
        }
    }
    None
}

/// Split `s` on unescaped occurrences of `delim`, returning the raw segments
/// (escapes NOT yet removed).
fn split_on_unescaped(s: &str, delim: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            cur.push(c);
            if let Some(&next) = chars.peek() {
                cur.push(next);
                chars.next();
            }
            continue;
        }
        if c == delim {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    out.push(cur);
    out
}

/// Split the field set on unescaped commas, but do NOT split inside a
/// double-quoted string field value.
fn split_fields(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = s.chars().peekable();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if in_string {
            cur.push(c);
            if c == '\\' {
                if let Some(&next) = chars.peek() {
                    cur.push(next);
                    chars.next();
                }
                continue;
            }
            if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '\\' => {
                cur.push(c);
                if let Some(&next) = chars.peek() {
                    cur.push(next);
                    chars.next();
                }
            }
            '"' => {
                in_string = true;
                cur.push(c);
            }
            ',' => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

/// Unescape measurement/tag characters: `\,` `\ ` `\=` → the literal char,
/// and `\\` → `\`.
fn unescape_tag(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Measurement unescaping: like [`unescape_tag`] (InfluxDB escapes the same
/// set plus nothing else for measurements).
fn unescape_measurement(s: &str) -> String {
    unescape_tag(s)
}

/// Field keys escape `,`, `=`, ` ` — same unescape rule.
fn unescape_field_key(s: &str) -> String {
    unescape_tag(s)
}

/// Format points to line protocol
pub fn format_line_protocol(points: &[Point]) -> String {
    let mut lines = Vec::new();

    for point in points {
        let mut line = point.measurement.clone();

        // Add tags
        if !point.tags.is_empty() {
            let tags: Vec<String> = point.tags
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            line.push(',');
            line.push_str(&tags.join(","));
        }

        line.push(' ');

        // Add fields
        let fields: Vec<String> = point.fields
            .iter()
            .map(|(k, v)| {
                let value = match v {
                    FieldValue::Float(f) => f.to_string(),
                    FieldValue::Integer(i) => format!("{}i", i),
                    FieldValue::String(s) => format!("\"{}\"", s),
                    FieldValue::Boolean(b) => if *b { "t" } else { "f" }.to_string(),
                };
                format!("{}={}", k, value)
            })
            .collect();
        line.push_str(&fields.join(","));

        // Add timestamp
        if let Some(ts) = point.timestamp {
            line.push(' ');
            line.push_str(&ts.to_string());
        }

        lines.push(line);
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple() {
        let line = "cpu,host=server01 value=0.64 1434055562000000000";
        let point = LineProtocolParser::parse(line).unwrap();
        assert_eq!(point.measurement, "cpu");
        assert_eq!(point.tags.get("host").unwrap(), "server01");
        assert!(matches!(point.fields.get("value"), Some(FieldValue::Float(_))));
        assert_eq!(point.timestamp, Some(1434055562000000000));
    }

    #[test]
    fn test_parse_multiple_fields() {
        let line = "weather,location=us temperature=82,humidity=71";
        let point = LineProtocolParser::parse(line).unwrap();
        assert_eq!(point.measurement, "weather");
        assert_eq!(point.fields.len(), 2);
    }

    #[test]
    fn test_parse_integer() {
        let line = "disk free=123456i";
        let point = LineProtocolParser::parse(line).unwrap();
        assert!(matches!(point.fields.get("free"), Some(FieldValue::Integer(123456))));
    }

    #[test]
    fn test_parse_boolean_uppercase_true_false() {
        let line = "flag a=TRUE,b=FALSE";
        let point = LineProtocolParser::parse(line).unwrap();
        assert!(matches!(point.fields.get("a"), Some(FieldValue::Boolean(true))));
        assert!(matches!(point.fields.get("b"), Some(FieldValue::Boolean(false))));
    }

    #[test]
    fn test_parse_escaped_measurement() {
        // Measurement with an escaped comma and space.
        let line = r"wea\,\ ther,temp=1 v=2";
        let point = LineProtocolParser::parse(line).unwrap();
        assert_eq!(point.measurement, "wea, ther");
    }

    #[test]
    fn test_parse_escaped_tag_value() {
        // Tag value containing escaped comma and equals.
        let line = r"m,k=a\,b\=c v=1";
        let point = LineProtocolParser::parse(line).unwrap();
        assert_eq!(point.tags.get("k").unwrap(), "a,b=c");
    }

    #[test]
    fn test_parse_string_field_with_comma() {
        // A quoted string field value keeps its comma; the second field must
        // still be parsed. A bare number is a float in line protocol.
        let line = r#"log msg="hello, world",level=1"#;
        let point = LineProtocolParser::parse(line).unwrap();
        match point.fields.get("msg") {
            Some(FieldValue::String(s)) => assert_eq!(s, "hello, world"),
            other => panic!("expected string field, got {:?}", other),
        }
        assert!(matches!(point.fields.get("level"), Some(FieldValue::Float(_))));
    }

    #[test]
    fn test_parse_escaped_space_does_not_split_segments() {
        // The escaped space in the tag stays in measurement+tags; the real
        // separator is the later unescaped space.
        let line = r"m,k=a\ b v=1 100";
        let point = LineProtocolParser::parse(line).unwrap();
        assert_eq!(point.tags.get("k").unwrap(), "a b");
        assert_eq!(point.timestamp, Some(100));
    }
}
