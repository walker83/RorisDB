// Token type IDs
pub const COLMETADATA_TOKEN: u8 = 0x81;
pub const ROW_TOKEN: u8 = 0xD1;
pub const DONE_TOKEN: u8 = 0xFD;
pub const DONEPROC_TOKEN: u8 = 0xFE;
pub const ERROR_TOKEN: u8 = 0xAA;
pub const INFO_TOKEN: u8 = 0xAB;
pub const RETURNSTATUS_TOKEN: u8 = 0x79;
pub const ENVCHANGE_TOKEN: u8 = 0xE3;
pub const LOGINACK_TOKEN: u8 = 0xAD;

pub fn encode_done(row_count: u64, status: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(DONE_TOKEN);
    buf.extend_from_slice(&status.to_le_bytes()); // status
    buf.extend_from_slice(&0u16.to_le_bytes()); // curcmd
    // Row count as 4-byte (TDS 5.0), saturate at u32::MAX
    buf.extend_from_slice(&(row_count.min(u32::MAX as u64) as u32).to_le_bytes());
    buf
}

pub fn encode_error(number: u32, state: u8, severity: u8, message: &str, server: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(ERROR_TOKEN);
    let msg_bytes = message.encode_utf16().collect::<Vec<_>>();
    // 4(number) + 1(state) + 1(severity) + 2(msg_len) + msg_bytes*2 + 2(srv_len) + srv*2 + 2(proc_len) + 4(line)
    let srv: Vec<u16> = server.encode_utf16().collect();
    let total_len = 4 + 1 + 1 + 2 + msg_bytes.len() * 2 + 2 + srv.len() * 2 + 2 + 4;
    buf.extend_from_slice(&(total_len as u16).to_le_bytes());
    buf.extend_from_slice(&number.to_le_bytes());
    buf.push(state);
    buf.push(severity);
    // Message length + message (UTF-16LE)
    buf.extend_from_slice(&(msg_bytes.len() as u16).to_le_bytes());
    for ch in &msg_bytes { buf.extend_from_slice(&ch.to_le_bytes()); }
    // Server name length + name (UTF-16LE)
    let srv: Vec<u16> = server.encode_utf16().collect();
    buf.extend_from_slice(&(srv.len() as u16).to_le_bytes());
    for ch in &srv { buf.extend_from_slice(&ch.to_le_bytes()); }
    // Process name (empty) + line number
    buf.extend_from_slice(&0u16.to_le_bytes()); // process name length
    buf.extend_from_slice(&0u32.to_le_bytes()); // line number
    buf
}

pub fn encode_info(number: u32, state: u8, severity: u8, message: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(INFO_TOKEN);
    let msg_bytes: Vec<u16> = message.encode_utf16().collect();
    let total_len = 4 + 1 + 1 + 2 + msg_bytes.len() * 2 + 2 + 2 + 4;
    buf.extend_from_slice(&(total_len as u16).to_le_bytes());
    buf.extend_from_slice(&number.to_le_bytes());
    buf.push(state);
    buf.push(severity);
    buf.extend_from_slice(&(msg_bytes.len() as u16).to_le_bytes());
    for ch in &msg_bytes { buf.extend_from_slice(&ch.to_le_bytes()); }
    // server name (empty)
    buf.extend_from_slice(&0u16.to_le_bytes());
    // process name (empty)
    buf.extend_from_slice(&0u16.to_le_bytes());
    // line number
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf
}

pub fn encode_login_ack() -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(LOGINACK_TOKEN);
    let interface: u8 = 1; // SQL Server
    let tds_version = [0x05, 0x00, 0x00, 0x00]; // TDS 5.0
    let prog_name = "HarnessDB";
    let prog: Vec<u16> = prog_name.encode_utf16().collect();
    // 1(interface) + 4(tds_version) + 1(prog_name_len) + prog.len()*2 + 4(version)
    let total_len = 1 + 4 + 1 + prog.len() * 2 + 4;
    buf.extend_from_slice(&(total_len as u16).to_le_bytes());
    buf.push(interface);
    buf.extend_from_slice(&tds_version);
    buf.push(prog.len().min(255) as u8);
    for ch in &prog { buf.extend_from_slice(&ch.to_le_bytes()); }
    // Version: major.minor.build.patch (4 bytes total)
    buf.extend_from_slice(&16u32.to_le_bytes()); // 16.0.0.0
    buf
}

pub fn encode_env_change_db(database: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(ENVCHANGE_TOKEN);
    let db_new: Vec<u16> = database.encode_utf16().collect();
    let total_len = 1 + 1 + db_new.len() * 2 + 1;
    buf.extend_from_slice(&(total_len as u16).to_le_bytes());
    buf.push(1); // env type: DATABASE
    buf.push(db_new.len() as u8);
    for ch in &db_new { buf.extend_from_slice(&ch.to_le_bytes()); }
    buf.push(0); // old value length
    buf
}

pub fn encode_return_status(status: i32) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(RETURNSTATUS_TOKEN);
    buf.extend_from_slice(&status.to_le_bytes());
    buf
}

// Column metadata for TDS 5.0
pub struct TdsColumnDef {
    pub name: String,
    pub tds_type: u8,
    pub max_length: u16,
}

pub fn encode_colmetadata(columns: &[TdsColumnDef]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(COLMETADATA_TOKEN);
    buf.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for col in columns {
        // Column name length + name (UTF-16LE)
        let name_chars: Vec<u16> = col.name.encode_utf16().collect();
        buf.push(name_chars.len() as u8);
        for ch in &name_chars { buf.extend_from_slice(&ch.to_le_bytes()); }
        // Status (nullable=1)
        buf.push(1);
        // TDS type
        buf.push(col.tds_type);
        // Max length
        buf.extend_from_slice(&col.max_length.to_le_bytes());
    }
    buf
}

// Row data encoding (all as strings for simplicity)
pub fn encode_row(values: &[Option<String>]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(ROW_TOKEN);
    for val in values {
        match val {
            Some(s) => {
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            None => {
                buf.extend_from_slice(&0xFFFFu16.to_le_bytes()); // NULL marker
            }
        }
    }
    buf
}
